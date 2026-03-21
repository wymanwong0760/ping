"""数据校验实现。

该模块提供与数据源解耦的质量校验逻辑，输入任意 bars-like DataFrame，
输出结构化 `ValidationResult`，用于：
- 预加载阶段的数据健康检查；
- 严格模式下的失败阻断；
- 调试时定位时间戳、重复键、空值比例等问题。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from quant_system.core.schema import BAR_REQUIRED_COLUMNS


@dataclass(slots=True)
class ValidationResult:
    """校验结果容器。"""

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    stats: dict[str, object] = field(default_factory=dict)


class DataValidator:
    """bars 数据质量校验器。

    校验目标是保证数据满足回测链路的最小可用约束：
    - 主键层面：`(symbol, timestamp)` 可唯一定位；
    - 时间层面：时间戳可解析、且同标的时间序非逆序；
    - 数值层面：`high >= low`、`volume >= 0` 等基础一致性；
    - 质量层面：按列统计空值占比并输出预警。
    """

    def validate(
        self,
        df: pd.DataFrame,
        null_ratio_threshold: float = 0.3,
    ) -> ValidationResult:
        """执行完整校验并返回结构化结果。

        返回的 `ValidationResult` 中：
        - `errors` 表示会影响正确性的硬问题；
        - `warnings` 表示可运行但质量需关注的问题；
        - `stats` 提供计数与明细，便于日志与测试断言。
        """
        errors: list[str] = []
        warnings: list[str] = []

        missing_required = [col for col in BAR_REQUIRED_COLUMNS if col not in df.columns]
        if missing_required:
            errors.append(f"Missing required columns: {missing_required}")
            return ValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
                stats={
                    "row_count": int(len(df)),
                    "missing_required_columns": missing_required,
                },
            )

        tmp = df.copy()
        tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce", utc=False)

        invalid_timestamp_count = int(tmp["timestamp"].isna().sum())
        if invalid_timestamp_count > 0:
            errors.append(f"Invalid timestamps found: {invalid_timestamp_count}")

        symbol_missing_count = int(
            tmp["symbol"].isna().sum() + (tmp["symbol"].astype(str).str.strip() == "").sum()
        )
        if symbol_missing_count > 0:
            errors.append(f"Missing symbol values found: {symbol_missing_count}")

        duplicate_count = int(tmp.duplicated(subset=["symbol", "timestamp"]).sum())
        if duplicate_count > 0:
            errors.append(f"Duplicate (symbol, timestamp) rows found: {duplicate_count}")

        non_monotonic_symbols: list[str] = []
        for symbol, grp in tmp.groupby("symbol", dropna=False, sort=False):
            if grp["timestamp"].isna().all():
                continue
            if not grp["timestamp"].is_monotonic_increasing:
                non_monotonic_symbols.append(str(symbol))
        if non_monotonic_symbols:
            errors.append(
                "Non-monotonic timestamps found for symbols: "
                + ",".join(non_monotonic_symbols)
            )

        high_low_invalid = int((tmp["high"] < tmp["low"]).fillna(False).sum())
        if high_low_invalid > 0:
            errors.append(f"Rows with high < low found: {high_low_invalid}")

        volume_numeric = pd.to_numeric(tmp["volume"], errors="coerce")
        negative_volume_count = int((volume_numeric < 0).fillna(False).sum())
        if negative_volume_count > 0:
            errors.append(f"Rows with negative volume found: {negative_volume_count}")

        null_ratio_by_column = {
            col: float(tmp[col].isna().mean())
            for col in tmp.columns
            if len(tmp) > 0
        }
        exceeded_null_ratio = {
            col: ratio
            for col, ratio in null_ratio_by_column.items()
            if ratio > null_ratio_threshold
        }
        if exceeded_null_ratio:
            warnings.append(
                "Columns exceeded null ratio threshold "
                f"({null_ratio_threshold}): {sorted(exceeded_null_ratio.keys())}"
            )

        stats: dict[str, object] = {
            "row_count": int(len(tmp)),
            "missing_required_columns": missing_required,
            "invalid_timestamp_count": invalid_timestamp_count,
            "symbol_missing_count": symbol_missing_count,
            "duplicate_count": duplicate_count,
            "non_monotonic_symbols": non_monotonic_symbols,
            "high_low_invalid_count": high_low_invalid,
            "negative_volume_count": negative_volume_count,
            "null_ratio_by_column": null_ratio_by_column,
            "exceeded_null_ratio": exceeded_null_ratio,
        }

        return ValidationResult(
            is_valid=(len(errors) == 0),
            errors=errors,
            warnings=warnings,
            stats=stats,
        )
