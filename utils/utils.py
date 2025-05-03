from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


def handle_schema_evolution(df: DataFrame, table_path: str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    try:
        existing_df = spark.read.format("delta").load(table_path)
        new_columns = [col for col in df.columns if col not in existing_df.columns]

        for col_name in new_columns:
            existing_df = existing_df.withColumn(col_name, lit(None))

        return existing_df.unionByName(df)
    except AnalysisException:
        return df


def manage_partitions(df: DataFrame, partition_columns: list) -> DataFrame:
    return df.repartition(*partition_columns)


def optimize_delta_table(table_path: str):
    spark = SparkSession.getActiveSession()
    spark.sql(f"OPTIMIZE delta.`{table_path}`")