from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any


class BaseConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._validate_config()

    @abstractmethod
    def _validate_config(self):
        """Validate connector-specific configuration"""
        pass

    @abstractmethod
    def read_data(self) -> DataFrame:
        """Read data from source"""
        pass

    @abstractmethod
    def write_data(self, df: DataFrame):
        """Write data to sink (if supported)"""
        pass


class DataConnectorError(Exception):
    """Base exception for connector errors"""
    pass