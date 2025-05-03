import logging

from pyspark.sql import DataFrame, SparkSession

from utils.error_handling import SecretManagerError
from .base import BaseConnector, DataConnectorError
from security.secrets_manager import get_secret_manager

logger = logging.getLogger(__name__)

class DatabaseConnector(BaseConnector):
    def _validate_config(self):
        required = ['type', 'host', 'database']
        if not all(r in self.config for r in required):
            raise DataConnectorError("Missing required DB config")

    def _get_credentials(self):
        try:
            secret_manager = get_secret_manager(self.config['secret_provider'])
            return secret_manager.get_secret(self.config['secret_name'])
        except SecretManagerError as e:
            logger.error(f"Secret management failure: {str(e)}")
            raise

    def read_data(self) -> DataFrame:
        spark = SparkSession.getActiveSession()
        try:
            secret_manager = get_secret_manager(self.config['secret_provider'])
            credentials = secret_manager.get_secret(self.config['secret_name'])
            jdbc_url = f"jdbc:{self.config['type']}://{self.config['host']}/{self.config['database']}"

            return spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", self.config['table']) \
                .option("user", credentials['username']) \
                .option("password", credentials['password']) \
                .option("ssl", self.config.get('ssl', True)) \
                .load()

        except Exception as e:
            raise DataConnectorError(f"Database read error: {str(e)}")