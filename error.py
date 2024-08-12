import pyodbc

try:
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    # Your code here to interact with the database

except pyodbc.Error as e:
    # Handle specific database errors
    if '28000' in str(e):  # Invalid authorization specification
        print("Error: Invalid username or password.")
    elif '08001' in str(e):  # SQL Server does not exist or access denied
        print("Error: Database server not found or access denied.")
    else:
        print(f"An error occurred: {e}")

finally:
    # Close the connection if it was successfully opened
    try:
        connection.close()
    except NameError:
        pass
