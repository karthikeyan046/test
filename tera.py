import teradatasql

# Replace with your Teradata connection details
connection_string = 'dbcname=your_hostname, user=your_username, password=your_password'

try:
    with teradatasql.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            # Example query to fetch table names in a specific database
            query = """
                SELECT DatabaseName, TableName
                FROM dbc.tables
                WHERE TableKind = 'T'  -- 'T' indicates base table; use 'V' for views
                      AND DatabaseName = 'your_database'
                ORDER BY TableName
            """
            cursor.execute(query)

            # Fetch all results
            rows = cursor.fetchall()

            # Print table names
            for row in rows:
                print(f"Database: {row.DatabaseName}, Table: {row.TableName}")

except teradatasql.DatabaseError as e:
    print(f"Error connecting to Teradata: {e}")
