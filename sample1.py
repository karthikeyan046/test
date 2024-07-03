import json
import pyodbc
from google.cloud import bigquery

def get_sql_server_schema(connection_string, tables):
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    schema = {}
    
    sql_server_type_mapping = {
        "bigint": "INTEGER",
        "binary": "BYTES",
        "bit": "BOOLEAN",
        "char": "STRING",
        "date": "DATE",
        "datetime": "DATETIME",
        "datetime2": "DATETIME",
        "datetimeoffset": "TIMESTAMP",
        "decimal": "NUMERIC",
        "float": "FLOAT",
        "image": "BYTES",
        "int": "INTEGER",
        "money": "NUMERIC",
        "nchar": "STRING",
        "ntext": "STRING",
        "numeric": "NUMERIC",
        "nvarchar": "STRING",
        "real": "FLOAT",
        "smalldatetime": "DATETIME",
        "smallint": "INTEGER",
        "smallmoney": "NUMERIC",
        "text": "STRING",
        "time": "TIME",
        "timestamp": "TIMESTAMP",
        "tinyint": "INTEGER",
        "uniqueidentifier": "STRING",
        "varbinary": "BYTES",
        "varchar": "STRING",
        # Add more mappings as needed
    }
    
    for table in tables:
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table}'
        """)
        columns = cursor.fetchall()
        schema[table] = []
        for column in columns:
            column_name = column[0]
            column_type_raw = column[1].strip()  # Trim any whitespace
            column_type = sql_server_type_mapping.get(column_type_raw, column_type_raw.upper())
            is_nullable = column[2] == "YES"
            column_length = column[3] if column[3] else None
            column_schema = {
                "name": column_name,  # Column name
                "type": column_type,  # Data type
                "nullable": is_nullable,  # Nullable
                "length": column_length  # Length
            }
            schema[table].append(column_schema)
    
    return schema

def convert_schema_to_bigquery(sql_server_schema):
    bigquery_schema = {}
    
    for table, columns in sql_server_schema.items():
        bigquery_schema[table] = []
        for column in columns:
            field_type = column["type"]
            # Include length in description if available
            description = f"Max Length: {column['length']}" if column['length'] else None
            if column["type"] == "STRING" and column["length"]:
                #field_type = f"{field_type}({column['length']})"
                field_type = f"{field_type}"
                print(field_type)
            field = bigquery.SchemaField(
                name=column["name"],
                field_type=field_type,
                mode="NULLABLE" if column["nullable"] else "REQUIRED",
                description=description
            )
            bigquery_schema[table].append(field)
    
    return bigquery_schema


def create_bigquery_tables(project_id, dataset_id, bigquery_schema):
    client = bigquery.Client(project=project_id)
    
    for table, schema in bigquery_schema.items():
        table_id = f"{project_id}.{dataset_id}.{table}"
        table_ref = client.dataset(dataset_id).table(table_id)
        bq_schema = [bigquery.SchemaField(field.name, field.field_type, description=field.description, mode=field.mode) for field in schema]
        table = bigquery.Table(table_ref, schema=bq_schema)
        
        try:
            table = client.create_table(table)
            print(f'Table {table_id} created successfully.')
        except Exception as e:
            print(f'Error creating table {table_id}: {e}')

def main():
    # SQL Server connection details
    sql_server_connection_string = (
       
    )
    sql_server_tables = ['data_files']  # List of SQL Server tables to read schema for

    # Get SQL Server schema
    sql_server_schema = get_sql_server_schema(sql_server_connection_string, sql_server_tables)
    
    # Save SQL Server schema to file
    with open('sql_server_schema.json', 'w') as f:
        json.dump(sql_server_schema, f, indent=4)
    
    # Convert SQL Server schema to BigQuery schema
    bigquery_schema = convert_schema_to_bigquery(sql_server_schema)
    
    # GCP BigQuery details
    project_id = ''
    dataset_id = ''
    
    # Create tables in BigQuery
    create_bigquery_tables(project_id, dataset_id, bigquery_schema)

    # Save BigQuery schema to file
    bigquery_schema_json = {table: [field.to_api_repr() for field in schema] for table, schema in bigquery_schema.items()}
    with open('bigquery_schema.json', 'w') as f:
        json.dump(bigquery_schema_json, f, indent=4)

if __name__ == '__main__':
    main()
