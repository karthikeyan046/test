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

'''
import json

def read_jsonValues():
    with open("config.json", "r") as file:
        data = json.load(file)
        #print(data)
    return data


# Run the other notebook
%run ./read_j.ipynb

# Now you can use the variables or functions defined in the other notebook
res=read_jsonValues()
print(res['var1'])




''''




