
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when
from src.common.logger import get_logger
from src.common.exceptions import (
    DataIngestionException,
    DataTransformationException,
    DataWriteException
)

logger = get_logger(__name__)


def clean_silver_data(df_bronze: DataFrame) -> DataFrame:
    """
    Apply data cleansing transformations to Bronze data.

    Args:
        df_bronze: DataFrame read from the Bronze Delta table.

    Returns:
        Cleaned Silver DataFrame.
    """

    # Standardize artist name
    df_cleaned = df_bronze.withColumn(
        "artist_name",
        trim(col("artist_name"))
    )

    # Standardize song title
    df_cleaned = df_cleaned.withColumn(
        "song_title",
        trim(col("title"))
    )

    # Convert invalid year value (0) to NULL
    df_cleaned = df_cleaned.withColumn(
        "year",
        when(col("year") == 0, None).otherwise(col("year"))
    )

    return df_cleaned
    


def run_silver(spark, config: dict) -> None:
    """
    Read Bronze data, apply Silver transformations,
    and write the result into the Silver Delta table.
    """

    # Validate configuration
    try:
        bronze_table = config["bronze_table"]
        silver_table = config["silver_table"]

    except KeyError as e:
        raise ConfigurationException(
            f"Missing Silver configuration: {e}"
        ) from e

    logger.info(
        f"Starting Silver transformation from: {bronze_table}"
    )

    # Read Bronze table
    try:
        df_bronze = spark.read.table(bronze_table)

    except Exception as e:
        logger.exception("Unable to read Bronze table")

        raise DataIngestionException(
            f"Failed to read Bronze table: {e}"
        ) from e

    # Apply Silver transformations
    try:
        df_silver = clean_silver_data(df_bronze)

    except Exception as e:
        logger.exception("Silver transformation failed")

        raise DataTransformationException(
            f"Failed to transform Silver data: {e}"
        ) from e

    # Write Silver table
    try:
        (
            df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table)
        )

    except Exception as e:
        logger.exception("Silver table write failed")

        raise DataWriteException(
            f"Unable to write Silver table: {e}"
        ) from e

    logger.info(
        f"Silver transformation completed successfully: {silver_table}"
    )
