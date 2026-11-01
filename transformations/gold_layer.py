import dlt
from pyspark.sql.functions import expr, desc

@dlt.table(
    name="top_artists_by_year_v3",
    comment="A table summarizing counts of songs released by top artists each year."
)
def top_artists_by_year_v3():
    # Use the LIVE prefix to reference the table from the second file
    return (
        spark.read.table("LIVE.songs_prepared_v3")
        .filter(expr("year > 0"))
        .groupBy("artist_name", "year")
        .count()
        .withColumnRenamed("count", "total_number_of_songs")
        .sort(desc("total_number_of_songs"), desc("year"))
    )
