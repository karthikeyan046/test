# Read_json.py
import json

# Assume we're reading a JSON file and storing it in a variable
def read_value():
    with open("./config.json", "r") as file:
        data = json.load(file)
        #return data
        return json.dumps(data)
        
#read_value()
print("Hi From Reading")
dbutils.notebook.exit(read_value())
