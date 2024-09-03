# caller_notebook
import json


# Run the callee_notebook and capture the return value
result = dbutils.notebook.run("./Read_json.py", 60)

result_dict = json.loads(result)
