from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
from src.common.logger import get_logger
from src.common.exceptions import (
    ConfigurationException,
    DataIngestionException,
    DataWriteException
)

logger = get_logger(__name__)


# Map YAML data types to Spark data types
SPARK_TYPE_MAP = {
    "string": StringType,
    "double": DoubleType,
    "int": IntegerType
}


def build_spark_schema(schema_config: list) -> StructType:
    """
    Build a Spark schema using the schema defined in YAML.
    """

    fields = []

    for field in schema_config:
        field_name = field["name"]
        field_type = field["type"].lower()

        if field_type not in SPARK_TYPE_MAP:
            raise ValueError(
                f"Unsupported data type: {field_type}"
            )

        spark_data_type = SPARK_TYPE_MAP[field_type]()

        fields.append(
            StructField(field_name, spark_data_type, True)
        )

    return StructType(fields)



def run_bronze(spark, config: dict) -> None:
    """
    Read source data, add ingestion metadata,
    and write data into the Bronze Delta table.
    """

    try:
        source_path = config["source_path"]
        source_format = config["source_format"]
        bronze_table = config["bronze_table"]

        reader_options = config.get("reader_options", {})
        write_mode = config.get("bronze_write_mode", "overwrite")

        schema_config = config.get("schema")

        if not schema_config:
            raise ConfigurationException(
                "Missing schema configuration"
            )

        source_schema = build_spark_schema(schema_config)

    except ConfigurationException:
        logger.exception("Bronze configuration validation failed")
        raise

    except Exception as e:
        raise ConfigurationException(
            f"Invalid Bronze configuration: {e}"
        ) from e

    logger.info(
        f"Starting Bronze ingestion from: {source_path}"
    )

    # Read source data
    try:
        df_raw = (
            spark.read
            .format(source_format)
            .schema(source_schema)
            .options(**reader_options)
            .load(source_path)
        )

        df_bronze = df_raw.withColumn(
            "ingested_at",
            current_timestamp()
        )

    except Exception as e:
        logger.exception("Bronze data ingestion failed")

        raise DataIngestionException(
            f"Unable to ingest source data: {e}"
        ) from e

    # Write Bronze Delta table
    try:
        (
            df_bronze.write
            .format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table)
        )

    except Exception as e:
        logger.exception("Bronze table write failed")

        raise DataWriteException(
            f"Unable to write Bronze table: {e}"
        ) from e

    logger.info(
        f"Bronze ingestion completed successfully: {bronze_table}"
    )
