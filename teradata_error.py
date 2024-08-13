import teradatasql

try:
    connection = teradatasql.connect(connection_string)
    cursor = connection.cursor()
    # Your code to interact with the database

except teradatasql.OperationalError as e:
    # This handles operational errors, such as incorrect database, username, or password
    if 'Invalid username or password' in str(e):
        print("Error: Invalid username or password.")
    elif 'Database does not exist' in str(e):
        print("Error: Database not found.")
    else:
        print(f"Operational error occurred: {e}")

except teradatasql.InterfaceError as e:
    # This handles errors related to the connection interface
    print(f"Interface error occurred: {e}")

except Exception as e:
    # General exception handler for any other errors
    print(f"An unexpected error occurred: {e}")

finally:
    # Ensure the connection is closed properly if it was opened
    try:
        connection.close()
    except NameError:
        pass
