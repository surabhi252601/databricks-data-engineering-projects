import os

# 1. Setup a dropdown/text widget to choose the use case configuration
dbutils.widgets.text("pipeline_config", "songs_pipeline")

# 2. Extract the parameter value
pipeline_name = dbutils.widgets.get("pipeline_config")
print(f"🚀 Starting framework execution for: {pipeline_name}")

# Get the directory where this current driver script resides
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
print("current_dir",current_dir)

# Helper function to run the .py files safely
def run_py_file(file_name):
    file_path = os.path.join(current_dir, f"{file_name}.py")
    print(f"--- Running {file_name.capitalize()} Layer ---")
    
    with open(file_path, "r") as f:
        code = f.read()
    
    # exec() executes the file content in the exact same session, 
    # making pipeline_name variables fully available to them.
    exec(code, globals())

# 3. Dynamically execute the Medallion layers sequentially
run_py_file("01_bronze")
run_py_file("02_silver")
run_py_file("03_gold")

print("✅ Framework Pipeline Execution Completed Successfully!")
