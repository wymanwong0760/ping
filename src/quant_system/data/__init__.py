"""data 模块导出。"""
from .ashare_downloader import AshareDownloader, DownloadResult, download_ashare_bars
from .ashare_provider import AshareDataProvider
from .base import BaseDataProvider
from .calendar import TradingCalendar
from .catalog import DataCatalog, DatasetMetadata
from .exceptions import DataConfigError, DataError, DataLoadError, DataValidationError
from .local_file_provider import LocalFileDataProvider
from .validator import DataValidator, ValidationResult

__all__ = [
    "BaseDataProvider",
    "LocalFileDataProvider",
    "AshareDataProvider",
    "AshareDownloader",
    "DownloadResult",
    "download_ashare_bars",
    "DataCatalog",
    "DatasetMetadata",
    "TradingCalendar",
    "DataValidator",
    "ValidationResult",
    "DataError",
    "DataConfigError",
    "DataLoadError",
    "DataValidationError",
]
