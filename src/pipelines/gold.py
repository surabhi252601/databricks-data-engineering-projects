
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, dense_rank
from pyspark.sql.window import Window
from src.common.logger import get_logger
from src.common.exceptions import (
    ConfigurationException,
    DataIngestionException,
    DataTransformationException,
    DataWriteException
)

logger = get_logger(__name__)


def build_gold_data(df_silver: DataFrame) -> DataFrame:
    """
    Aggregate songs by year and artist,
    then rank the top artists for each year.

    Args:
        df_silver: Cleaned Silver DataFrame.

    Returns:
        Gold DataFrame containing top-ranked artists per year.
    """

    # Exclude records with unknown release years
    df_valid_years = df_silver.filter(
        col("year").isNotNull()
    )

    # Count source records for each artist and year
    df_artist_counts = (
        df_valid_years
        .groupBy("year", "artist_name")
        .agg(
            count("song_id").alias("total_songs")
        )
    )

    # Define ranking window for each year
    window_spec = (
        Window
        .partitionBy("year")
        .orderBy(col("total_songs").desc())
    )

    # Assign dense ranks and retain top 5 ranks
    df_gold = (
        df_artist_counts
        .withColumn(
            "rank",
            dense_rank().over(window_spec)
        )
        .filter(col("rank") <= 5)
        .select(
            "year",
            "artist_name",
            "total_songs",
            "rank"
        )
        .orderBy("year", "rank")
    )

    return df_gold



def run_gold(spark, config: dict) -> None:
    """
    Read Silver data, apply Gold transformations,
    and write the result into the Gold Delta table.
    """

    # Validate configuration
    try:
        silver_table = config["silver_table"]
        gold_table = config["gold_table"]

    except KeyError as e:
        logger.exception("Gold configuration validation failed")

        raise ConfigurationException(
            f"Missing Gold configuration: {e}"
        ) from e

    logger.info(
        f"Starting Gold aggregation from: {silver_table}"
    )

    # Read Silver table
    try:
        df_silver = spark.read.table(silver_table)

    except Exception as e:
        logger.exception("Unable to read Silver table")

        raise DataIngestionException(
            f"Failed to read Silver table: {e}"
        ) from e

    # Apply Gold transformations
    try:
        df_gold = build_gold_data(df_silver)

    except Exception as e:
        logger.exception("Gold transformation failed")

        raise DataTransformationException(
            f"Failed to transform Gold data: {e}"
        ) from e

    # Write Gold table
    try:
        (
            df_gold.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(gold_table)
        )

    except Exception as e:
        logger.exception("Gold table write failed")

        raise DataWriteException(
            f"Unable to write Gold table: {e}"
        ) from e

    logger.info(
        f"Gold aggregation completed successfully: {gold_table}"
    )
