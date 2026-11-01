import yaml
from pyspark.sql.functions import col, trim, lower

# 1. Accept the configuration parameter passed from the master orchestrator
dbutils.widgets.text("pipeline_config", "songs_pipeline")
pipeline_name = dbutils.widgets.get("pipeline_config")

# 2. Dynamically load the YAML configuration
# NOTE: Replace 'your_username_or_folder' with your actual Databricks folder/email path
config_path = f"Workspace/Users/kateshipra5@gmail.com/config/{pipeline_name}.yaml"
with open(config_path, "r") as f:
    cfg = yaml.safe_load(f)

print(f"🧹 Starting Silver cleansing for table: {cfg['bronze_table']}")

# 3. Read raw data directly from the Bronze Delta Table
df_bronze = spark.read.table(cfg["bronze_table"])

# 4. Data Cleansing Transformations
# Deduplicate records using the primary key defined in the YAML file
pk = cfg["primary_key"]
df_cleaned = df_bronze.dropDuplicates([pk])

# Standardize text columns (e.g., trim spaces and lowercase titles for uniform querying)
# Checking if specific expected columns exist in this use case
if "artist_name" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumn("artist_name", trim(col("artist_name")))
if "title" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumn("song_title", trim(col("title")))

# 5. Overwrite the clean data into the Silver Delta Table
df_cleaned.write.format("delta").mode("overwrite").saveAsTable(cfg["silver_table"])
print(f"🔸 Cleaned data successfully saved into Silver Table: {cfg['silver_table']}")
