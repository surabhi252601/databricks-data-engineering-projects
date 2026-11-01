# COMMAND ----------
# 1. Setup a dropdown/text widget to choose the use case configuration
dbutils.widgets.text("pipeline_config", "songs_pipeline")

# 2. Extract the parameter value
pipeline_name = dbutils.widgets.get("pipeline_config")
print(f"🚀 Starting framework execution for: {pipeline_name}")

# COMMAND ----------
# 3. Dynamically execute the Medallion layers sequentially
# We pass the pipeline name as an argument to the child notebooks

print("--- Running Bronze Layer ---")
dbutils.notebook.run("01_bronze", timeout_seconds=600, arguments={"pipeline_config": pipeline_name})

print("--- Running Silver Layer ---")
dbutils.notebook.run("02_silver", timeout_seconds=600, arguments={"pipeline_config": pipeline_name})

print("--- Running Gold Layer ---")
dbutils.notebook.run("03_gold", timeout_seconds=600, arguments={"pipeline_config": pipeline_name})

print("✅ Framework Pipeline Execution Completed Successfully!")
