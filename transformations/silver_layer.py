import dlt

@dlt.table(
    name="songs_prepared_v3",
    comment="Million Song Dataset with data cleaned and prepared for analysis."
)
@dlt.expect("valid_artist_name", "artist_name IS NOT NULL")
@dlt.expect("valid_title", "song_title IS NOT NULL")
@dlt.expect("valid_duration", "duration > 0")
def songs_prepared_v3():
    # Use the LIVE prefix to reference the streaming table from the first file
    return (
        spark.readStream.table("LIVE.songs_raw_v3")
        .withColumnRenamed("title", "song_title")
        .select("artist_id", "artist_name", "duration", "release", "tempo", "time_signature", "song_title", "year")
    )