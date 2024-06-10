import pyodbc
import json
from google.cloud import bigquery

def get_teradata_schema(connection_string, tables):
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    schema = {}
    
    for table in tables:
        cursor.execute(f"SELECT ColumnName, ColumnType, Nullable FROM DBC.ColumnsV WHERE TableName = '{table}'")
        columns = cursor.fetchall()
        schema[table] = []
        for column in columns:
            column_schema = {
                "name": column[0],  # Column name
                "type": column[1].upper(),  # Data type
                "nullable": column[2] == "Y"  # Nullable
            }
            schema[table].append(column_schema)
    
    return schema

def convert_schema_to_bigquery(teradata_schema):
    type_mapping = {
        "BYTEINT": "INTEGER",
        "SMALLINT": "INTEGER",
        "INTEGER": "INTEGER",
        "BIGINT": "INTEGER",
        "FLOAT": "FLOAT",
        "DECIMAL": "NUMERIC",
        "CHAR": "STRING",
        "VARCHAR": "STRING",
        "CLOB": "STRING",
        "DATE": "DATE",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        "INTERVAL": "STRING",
        # Add more mappings as needed
    }
    
    bigquery_schema = {}
    
    for table, columns in teradata_schema.items():
        bigquery_schema[table] = []
        for column in columns:
            field = bigquery.SchemaField(
                name=column["name"],
                field_type=type_mapping.get(column["type"], "STRING"),
                mode="NULLABLE" if column["nullable"] else "REQUIRED"
            )
            bigquery_schema[table].append(field)
    
    return bigquery_schema

def create_bigquery_tables(project_id, dataset_id, bigquery_schema):
    client = bigquery.Client(project=project_id)
    
    for table, schema in bigquery_schema.items():
        table_id = f"{project_id}.{dataset_id}.{table}"
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        print(f"Created table {table_id}")

def main():
    # Teradata connection details
    teradata_connection_string = (
        "DRIVER={Teradata};"
        "DBCName=your_server_name;"
        "DATABASE=your_database_name;"
        "UID=your_username;"
        "PWD=your_password"
    )
    teradata_tables = ['table1', 'table2']  # List of Teradata tables to read schema for

    # Get Teradata schema
    teradata_schema = get_teradata_schema(teradata_connection_string, teradata_tables)
    
    # Save Teradata schema to file
    with open('teradata_schema.json', 'w') as f:
        json.dump(teradata_schema, f, indent=4)
    
    # Convert Teradata schema to BigQuery schema
    bigquery_schema = convert_schema_to_bigquery(teradata_schema)
    
    # GCP BigQuery details
    project_id = 'your_gcp_project_id'
    dataset_id = 'your_bigquery_dataset_id'
    
    # Create tables in BigQuery
    create_bigquery_tables(project_id, dataset_id, bigquery_schema)

    # Save BigQuery schema to file
    bigquery_schema_json = {table: [field.to_api_repr() for field in schema] for table, schema in bigquery_schema.items()}
    with open('bigquery_schema.json', 'w') as f:
        json.dump(bigquery_schema_json, f, indent=4)

if __name__ == '__main__':
    main()
