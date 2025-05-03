import logging

from pyspark.sql import DataFrame, SparkSession
import pandas as pd

from connectors.base import BaseConnector, DataConnectorError
from security.encryption import Decryptor



class FileConnector(BaseConnector):

    def __init__(self):
        logging.info("Entered into File processing....")

    def _validate_config(self):
        required = ['path', 'format']
        if not all(r in self.config for r in required):
            raise DataConnectorError("Missing required file configuration")

    def read_data(self) -> DataFrame:
        spark = SparkSession.getActiveSession()
        try:
            if self.config['format'] == 'excel':
                return self._read_excel()

            if self.config.get('encrypted', False):
                return self._read_encrypted_file()

            return spark.read \
                .format(self.config['format']) \
                .options(**self.config.get('options', {})) \
                .load(self.config['path'])

        except Exception as e:
            raise DataConnectorError(f"File read error: {str(e)}")

    def _read_excel(self) -> DataFrame:
        try:
            pd_df = pd.read_excel(
                self.config['path'],
                **self.config.get('options', {})
            )
            return SparkSession.getActiveSession().createDataFrame(pd_df)
        except Exception as e:
            raise DataConnectorError(f"Excel read error: {str(e)}")

    def _read_encrypted_file(self) -> DataFrame:
        try:
            decryptor = Decryptor(self.config['encryption_key'])
            decrypted_data = decryptor.decrypt_file(self.config['path'])
            # Implement logic to convert decrypted data to DataFrame
            # Example for CSV:
            return SparkSession.getActiveSession().read \
                .csv(decrypted_data, **self.config.get('options', {}))
        except Exception as e:
            raise DataConnectorError(f"Decryption failed: {str(e)}")