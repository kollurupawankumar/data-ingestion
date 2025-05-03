from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from utils.error_handling import DeltaTableError


class DeltaLakeManager:
    def __init__(self, spark):
        self.spark = spark

    def write_table(self, df: DataFrame, table_config: dict):
        try:
            writer = df.write.format("delta")

            if table_config.get('partition_by'):
                writer = writer.partitionBy(table_config['partition_by'])

            if table_config['mode'] == 'merge':
                self._merge_data(df, table_config)
            else:
                writer.mode(table_config['mode']) \
                    .saveAsTable(table_config['name'])

        except Exception as e:
            raise DeltaTableError(f"Delta write failed: {str(e)}")

    def _merge_data(self, df: DataFrame, config: dict):
        delta_table = DeltaTable.forName(self.spark, config['name'])
        merge_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in config['merge_keys']]
        )

        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()