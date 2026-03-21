"""test_validator 测试用例。"""
from __future__ import annotations

import pandas as pd

from quant_system.data import DataValidator


def test_validator_detects_bad_data() -> None:
    df = pd.read_csv("tests/fixtures/data/bars_bad.csv")
    validator = DataValidator()

    result = validator.validate(df)

    assert result.is_valid is False
    assert any("Invalid timestamps" in err for err in result.errors)
    assert any("Duplicate (symbol, timestamp)" in err for err in result.errors)
    assert any("high < low" in err for err in result.errors)
    assert any("negative volume" in err for err in result.errors)
    assert any("Missing symbol" in err for err in result.errors)


def test_validator_null_ratio_warning() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2024-01-02 09:30:00", "2024-01-03 09:30:00"],
            "symbol": ["000001.SZ", "000001.SZ"],
            "open": [10.0, 10.1],
            "high": [10.2, 10.3],
            "low": [9.9, 10.0],
            "close": [10.1, 10.2],
            "volume": [100, 120],
            "amount": [None, None],
        }
    )
    validator = DataValidator()

    result = validator.validate(df, null_ratio_threshold=0.4)

    assert result.is_valid is True
    assert result.warnings
    assert "amount" in str(result.warnings[0])
