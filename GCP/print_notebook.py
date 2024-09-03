# Read_json.py
import json

# Assume we're reading a JSON file and storing it in a variable
with open("config.json", "r") as file:
    data = json.load(file)

# Define a global variable to store the result
global config_data
config_data = data



# Execute the script
#%run "C:\Users\karthi\Desktop\Testnotebook\Read_json.ipynb"

# Now you can access config_data directly
#print(config_data)

