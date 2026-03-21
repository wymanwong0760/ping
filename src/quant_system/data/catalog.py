"""数据目录实现。

该模块维护“数据集名称 -> 元信息”的映射，用于把具体文件路径与格式配置
从业务代码中解耦。上层 provider 只依赖目录查询结果，不直接硬编码数据文件位置。
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .exceptions import DataConfigError


@dataclass(slots=True)
class DatasetMetadata:
    """数据集元信息。"""

    name: str
    path: str
    file_format: str
    timeframe: str = "1d"
    timezone: str = "UTC"
    column_mapping: dict[str, str] = field(default_factory=dict)
    default_symbol: str | None = None

    @classmethod
    def from_dict(cls, name: str, payload: dict[str, Any]) -> "DatasetMetadata":
        """从字典构建元信息。"""
        path = payload.get("path")
        file_format = payload.get("format") or payload.get("file_format")
        if not path:
            raise DataConfigError(f"Dataset '{name}' missing required field: path")
        if not file_format:
            raise DataConfigError(f"Dataset '{name}' missing required field: format")
        file_format_normalized = str(file_format).lower().strip()
        if file_format_normalized not in {"csv", "parquet"}:
            raise DataConfigError(
                f"Dataset '{name}' has unsupported format '{file_format_normalized}'"
            )

        mapping = payload.get("column_mapping") or {}
        if not isinstance(mapping, dict):
            raise DataConfigError(f"Dataset '{name}' column_mapping must be an object")

        return cls(
            name=name,
            path=str(path),
            file_format=file_format_normalized,
            timeframe=str(payload.get("timeframe", "1d")),
            timezone=str(payload.get("timezone", "UTC")),
            column_mapping={str(k): str(v) for k, v in mapping.items()},
            default_symbol=(
                str(payload["default_symbol"]) if payload.get("default_symbol") else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """转换为可序列化字典。"""
        payload = asdict(self)
        payload["format"] = payload.pop("file_format")
        return payload


class DataCatalog:
    """数据目录管理对象。

    职责边界：
    - 维护内存态数据集注册表；
    - 提供按名称查询与默认查询策略；
    - 在配置了 `catalog_path` 时负责持久化与重载。

    该对象本身不读取 bars 数据，只负责“去哪读、按什么格式读”的元信息管理。
    """

    def __init__(self, catalog_path: str | Path | None = None) -> None:
        self.catalog_path = Path(catalog_path) if catalog_path else None
        self._datasets: dict[str, DatasetMetadata] = {}
        if self.catalog_path and self.catalog_path.exists():
            self._load_from_file()

    def register_dataset(self, name: str, metadata: dict[str, Any]) -> None:
        """注册一个数据集。"""
        self._datasets[name] = DatasetMetadata.from_dict(name, metadata)
        if self.catalog_path:
            self.save()

    def get_dataset(self, name: str | None = None) -> DatasetMetadata:
        """按名称获取数据集；未指定时在唯一数据集场景下返回它。

        行为约定：
        - 传入 `name`：严格按名称查找，不存在则报错；
        - 未传 `name` 且仅有一个数据集：返回该数据集，便于简化单数据源调用；
        - 未传 `name` 且存在多个数据集：要求调用方显式指定，避免误读。
        """
        if name:
            dataset = self._datasets.get(name)
            if not dataset:
                raise DataConfigError(f"Dataset '{name}' not found in catalog")
            return dataset

        if len(self._datasets) == 1:
            return next(iter(self._datasets.values()))
        if not self._datasets:
            raise DataConfigError("Catalog is empty; register at least one dataset")
        raise DataConfigError(
            "Multiple datasets found. Please pass dataset_name explicitly"
        )

    def list_datasets(self) -> list[str]:
        """列出已注册数据集名称。"""
        return sorted(self._datasets.keys())

    def save(self) -> None:
        """将目录写回磁盘。"""
        if not self.catalog_path:
            return
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            name: metadata.to_dict()
            for name, metadata in sorted(self._datasets.items(), key=lambda x: x[0])
        }
        self.catalog_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _load_from_file(self) -> None:
        """从磁盘加载目录。"""
        if not self.catalog_path:
            return
        try:
            payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise DataConfigError(
                f"Invalid catalog JSON file: {self.catalog_path}"
            ) from exc
        if not isinstance(payload, dict):
            raise DataConfigError(
                f"Catalog file must be an object map: {self.catalog_path}"
            )

        datasets: dict[str, DatasetMetadata] = {}
        for name, metadata in payload.items():
            if not isinstance(metadata, dict):
                raise DataConfigError(
                    f"Dataset metadata for '{name}' must be an object"
                )
            datasets[name] = DatasetMetadata.from_dict(str(name), metadata)
        self._datasets = datasets
