import yaml
from pyspark.sql.functions import current_timestamp

# 1. Accept the config parameter passed down from the master driver
dbutils.widgets.text("pipeline_config", "songs_pipeline")
pipeline_name = dbutils.widgets.get("pipeline_config")

# 2. Read the matching YAML config file
# Double-check that your base path matches your exact Git folder path in your Databricks workspace
config_path = f"/Workspace/home/your_username_or_folder/config/{pipeline_name}.yaml"
with open(config_path, "r") as f:
    cfg = yaml.safe_load(f)

print(f"📥 Loading Raw Data from: {cfg['source_path']}")

# 3. Read raw data dynamically using config parameters
df_raw = spark.read.format(cfg["source_format"]).option("header", "true").option("inferSchema", "true").load(cfg["source_path"])

# 4. Add ingestion metadata (Audit column)
df_bronze = df_raw.withColumn("ingested_at", current_timestamp())

# 5. Write out directly to Delta format in the Bronze Table
df_bronze.write.format("delta").mode("overwrite").saveAsTable(cfg["bronze_table"])
print(f"🔹 Successfully written raw data into table: {cfg['bronze_table']}")
