from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class ValidationError(Exception):
    pass


def validate_schema(df: DataFrame, expected_schema: dict):
    """Validate DataFrame schema matches expected schema"""
    for field, dtype in expected_schema.items():
        if field not in df.columns:
            raise ValidationError(f"Missing column: {field}")
        if str(df.schema[field].dataType) != dtype:
            raise ValidationError(f"Datatype mismatch for {field}: "
                                  f"Expected {dtype}, Found {df.schema[field].dataType}")


def validate_null_columns(df: DataFrame, columns: list):
    """Validate specified columns have no null values"""
    null_counts = df.select([col(c).isNull.alias(c) for c in columns]) \
        .groupBy().sum() \
        .collect()[0].asDict()

    for col_name, null_count in null_counts.items():
        if null_count > 0:
            raise ValidationError(f"Column {col_name} contains {null_count} null values")