import random
import string

# Function to generate a random employee name
def generate_random_name(length=6):
    return ''.join(random.choices(string.ascii_uppercase, k=length))

# Function to generate a random employee ID
def generate_random_id(length=5):
    return ''.join(random.choices(string.digits, k=length))

# Number of employees
num_employees = 10

# Generate random employee data
employees = [(generate_random_id(), generate_random_name()) for _ in range(num_employees)]

# Write to a text file
with open('/dbfs/FileStore/employees1.txt', 'w') as file:
    for emp_id, emp_name in employees:
        file.write(f"{emp_id}\n")

print("Employee data has been written to /dbfs/FileStore/employees1.txt")

'''
# Define the file path
file_path = "/dbfs/FileStore/employees1.txt"

# Read the file and print its contents
with open(file_path, 'r') as file:
    contents = file.read()
    print(contents)
'''
