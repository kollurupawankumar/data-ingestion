from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, DataFrame
from delta_lake.manager import DeltaLakeManager
from connectors import FileConnector, DatabaseConnector
from utils.error_handling import handle_errors, ValidationError
from pyspark.sql import functions as F
import yaml
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession

from utils.validators import validate_schema

logger = logging.getLogger(__name__)

class DataIngestionFramework:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()  # Fixed method call
        self.spark = self._create_spark_session()
        self.delta_mgr = DeltaLakeManager(self.spark)

    def _load_config(self) -> Dict[str, Any]:
        """Load and validate YAML configuration"""
        try:
            with open(self.config_path) as f:
                config = yaml.safe_load(f)
            self._validate_config(config)
            return config
        except FileNotFoundError:
            raise ValueError(f"Config file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format: {str(e)}")

    @staticmethod
    def _validate_config(config: Dict[str, Any]):
        """Validate configuration structure"""
        required_sections = ['source', 'raw', 'processed']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required section: {section}")

        source_type = config['source'].get('type')
        if source_type not in ['file', 'database', 'api', 'stream']:
            raise ValueError(f"Invalid source type: {source_type}")

    @staticmethod
    def _create_spark_session() -> SparkSession:
        """Create secured Spark session"""
        return SparkSession.builder \
            .appName("DataIngestionFramework") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .getOrCreate()

    @handle_errors
    def ingest(self):
        """Main ingestion workflow"""
        logger.info("Starting data ingestion process")

        # Stage 1: Raw data ingestion
        raw_df = self._ingest_raw_data()

        # Stage 2: Data transformation
        processed_df = self._transform_data(raw_df)
        #processed_df = self._apply_transformation(processed_df)

        # Write processed data
        self._write_processed_data(processed_df)

        logger.info("Data ingestion completed successfully")

    def _ingest_raw_data(self) -> DataFrame:
        """Handle raw data ingestion"""
        connector = self._get_connector()
        raw_df = connector.read_data()

        # Write raw data to Delta Lake
        self.delta_mgr.write_table(
            df=raw_df,
            table_config={
                "name": self.config['raw']['table_name'],
                "mode": self.config['raw']['mode'],
                "partition_by": self.config['raw'].get('partition_by')
            }
        )
        return raw_df

    def _transform_data(self, raw_df: DataFrame) -> DataFrame:
        """Perform data transformations and validations"""
        # Apply schema validation
        validate_schema(raw_df, self.config['processing']['expected_schema'])

        # Apply transformations
        transformed_df = raw_df
        for transform in self.config['processing']['transformations']:
            transformed_df = self._apply_transformation(transformed_df, transform)

        # Apply validations
        for validation in self.config['processing']['validations']:
            self._apply_validation(transformed_df, validation)

        return transformed_df

    def _write_processed_data(self, df: DataFrame):
        """Handle processed data writing"""
        self.delta_mgr.write_table(
            df=df,
            table_config={
                "name": self.config['processed']['table_name'],
                "mode": self.config['processed']['mode'],
                "merge_keys": self.config['processed'].get('merge_keys'),
                "partition_by": self.config['processed'].get('partition_by')
            }
        )

    def _get_connector(self):
        source_type = self.config['source']['type']
        connector_map = {
            'file': FileConnector,
            'database': DatabaseConnector,
            # Add other connectors
        }
        return connector_map[source_type](self.config['source'])


    @staticmethod
    def _apply_transformation(df: DataFrame, transform: dict) -> DataFrame:
        """Apply individual transformation rule"""
        try:
            if 'rename' in transform:
                old_name, new_name = list(transform['rename'].items())[0]
                return df.withColumnRenamed(old_name, new_name)

            if 'cast' in transform:
                column, new_type = list(transform['cast'].items())[0]
                return df.withColumn(column, F.col(column).cast(new_type))

            if 'expr' in transform:
                column, expression = list(transform['expr'].items())[0]
                return df.withColumn(column, F.expr(expression))

            raise ValueError(f"Unknown transformation type: {transform.keys()}")
        except Exception as e:
            raise ValidationError(f"Transformation failed: {str(e)}") from e

    @staticmethod
    def _apply_validation(df: DataFrame, validation: dict):
        """Apply data validation rule"""
        try:
            if 'non_null' in validation:
                null_cols = validation['non_null']
                null_counts = df.select([F.sum(F.col(c).isNull.cast("int")).alias(c) for c in null_cols]).first()

                for col_name in null_cols:
                    if null_counts[col_name] > 0:
                        raise ValidationError(f"Column {col_name} contains {null_counts[col_name]} null values")

            if 'unique' in validation:
                unique_cols = validation['unique']
                duplicate_count = df.groupBy(unique_cols).count().filter(F.col("count") > 1).count()
                if duplicate_count > 0:
                    raise ValidationError(f"Duplicate values found in unique columns {unique_cols}")

            if 'custom' in validation:
                condition = validation['custom']
                invalid_count = df.filter(F.expr(f"NOT ({condition})")).count()
                if invalid_count > 0:
                    raise ValidationError(f"Custom validation failed: {condition} - {invalid_count} invalid records")

        except AnalysisException as e:
            raise ValidationError(f"Validation failed due to analysis error: {str(e)}") from e
