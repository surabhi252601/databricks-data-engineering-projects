import yaml
from pyspark.sql.functions import col, trim

# 1. Accept the configuration parameter passed from the master orchestrator
dbutils.widgets.text("pipeline_config", "songs_pipeline")
pipeline_name = dbutils.widgets.get("pipeline_config")

# 2. Dynamically load the YAML configuration
config_path = f"/Workspace/Users/kateshipra5@gmail.com/Songs_pipeline/databricks-data-engineering-projects/configs/{pipeline_name}.yaml"

with open(config_path, "r") as f:
    cfg = yaml.safe_load(f)

print(f"🧹 Starting Silver cleansing for table: {cfg['bronze_table']}")

# 3. Read raw data directly from the Bronze Delta Table
df_bronze = spark.read.table(cfg["bronze_table"])

# --- NEW STEP: Rename the raw indexed columns to real names ---
df_renamed = df_bronze \
    .withColumnRenamed("_c0", "artist_id") \
    .withColumnRenamed("_c4", "artist_name") \
    .withColumnRenamed("_c9", "duration") \
    .withColumnRenamed("_c11", "song_id") \
    .withColumnRenamed("_c16", "title") \
    .withColumnRenamed("_c13", "tempo") \
    .withColumnRenamed("_c14", "year")

# 4. Data Cleansing Transformations using the newly assigned names
pk = cfg["primary_key"]  # This will successfully find "song_id" now!
df_cleaned = df_renamed.dropDuplicates([pk])

# Standardize text columns using the corrected names
if "artist_name" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumn("artist_name", trim(col("artist_name")))
if "title" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumn("song_title", trim(col("title")))

# 5. Overwrite the clean data into the Silver Delta Table
df_cleaned.write.format("delta").mode("overwrite").saveAsTable(cfg["silver_table"])
print(f"🔸 Cleaned data successfully saved into Silver Table: {cfg['silver_table']}")
