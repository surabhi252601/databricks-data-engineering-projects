import yaml
from pyspark.sql.functions import count, desc, row_number
from pyspark.sql.window import Window

# 1. Accept the configuration parameter passed from the master orchestrator
dbutils.widgets.text("pipeline_config", "songs_pipeline")
pipeline_name = dbutils.widgets.get("pipeline_config")

# 2. Dynamically load the YAML configuration
config_path = f"/Workspace/Users/kateshipra5@gmail.com/Songs_pipeline/databricks-data-engineering-projects/configs/{pipeline_name}.yaml"


with open(config_path, "r") as f:
    cfg = yaml.safe_load(f)

print(f"🏆 Generating Gold Business Metrics from Silver Table: {cfg['silver_table']}")

# 3. Read clean data from the Silver Delta Table
df_silver = spark.read.table(cfg["silver_table"])

# 4. Apply Business Logic Transformation
if cfg["use_case"] == "songs_pipeline":
    # Group by Year and Artist to count total songs produced
    df_artist_counts = (df_silver
                        .filter(col("year") > 0) # Filter out invalid/empty years if any
                        .groupBy("year", "artist_name")
                        .agg(count(cfg["primary_key"]).alias("total_songs")))
    
    # Calculate the rank to easily identify the top artists per year
    window_spec = Window.partitionBy("year").orderBy(desc("total_songs"))
    df_gold = (df_artist_counts
               .withColumn("rank", row_number().over(window_spec))
               .filter(col("rank") <= 5) # Keep the top 5 artists per year
               .select("year", "artist_name", "total_songs", "rank")
               .orderBy("year", "rank"))
else:
    # Generic backup transformation fallback rule for future use cases
    df_gold = df_silver.groupBy(cfg["primary_key"]).count()

# 5. Save the final aggregated metrics to the Gold Delta Table
df_gold.write.format("delta").mode("overwrite").saveAsTable(cfg["gold_table"])
print(f"👑 Final Gold Aggregations published to: {cfg['gold_table']}")
