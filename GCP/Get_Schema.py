import pyodbc
import json
from google.cloud import bigquery

def get_teradata_schema(connection_string, tables):
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    schema = {}
    
    teradata_type_mapping = {
        "AT": "ARRAY",
        "BF": "BYTE",
        "BO": "BOOLEAN",
        "BV": "VARBYTE",
        "CF": "CHARACTER",
        "CV": "VARCHAR",
        "CO": "COBOL",
        "DA": "DATE",
        "DH": "DATE",
        "DS": "DATE",
        "DT": "DATETIME",
        "DZ": "TIME WITH TIME ZONE",
        "ED": "NUMBER",
        "EF": "NUMBER",
        "I1": "TINYINT",
        "I2": "SMALLINT",
        "I4": "INTEGER",
        "I8": "BIGINT",
        "IA": "BIGINT",
        "IP": "INTEGER",
        "PD": "DECIMAL",
        "PN": "NUMBER",
        "RI": "ROWID",
        "SB": "BYTEINT",
        "SC": "CHARACTER",
        "SD": "DATE",
        "SF": "FLOAT",
        "SI": "SMALLINT",
        "SN": "NUMBER",
        "ST": "TIME",
        "SZ": "TIME WITH TIME ZONE",
        "TS": "TIMESTAMP",
        "TZ": "TIMESTAMP WITH TIME ZONE",
        "VC": "VARCHAR",
        "YM": "YEAR TO MONTH INTERVAL"
        # Add more mappings as needed
    }
    
    for table in tables:
        cursor.execute(f"SELECT ColumnName, ColumnType, Nullable, ColumnLength FROM DBC.ColumnsV WHERE TableName = '{table}'")
        columns = cursor.fetchall()
        schema[table] = []
        for column in columns:
            column_type = teradata_type_mapping.get(column[1], column[1].upper())
            column_length = column[3] if column[3] else None
            column_schema = {
                "name": column[0],  # Column name
                "type": column_type,  # Data type
                "nullable": column[2] == "Y",  # Nullable
                "length": column_length  # Length
            }
            schema[table].append(column_schema)
    
    return schema

def convert_schema_to_bigquery(teradata_schema):
    type_mapping = {
        "BYTE": "BYTES",
        "BOOLEAN": "BOOL",
        "VARBYTE": "BYTES",
        "CHARACTER": "STRING",
        "VARCHAR": "STRING",
        "DATE": "DATE",
        "DATETIME": "DATETIME",
        "NUMBER": "NUMERIC",
        "TINYINT": "INTEGER",
        "SMALLINT": "INTEGER",
        "INTEGER": "INTEGER",
        "BIGINT": "INTEGER",
        "DECIMAL": "NUMERIC",
        "ROWID": "STRING",
        "BYTEINT": "INTEGER",
        "FLOAT": "FLOAT",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        # Add more mappings as needed
    }
    
    bigquery_schema = {}
    
    for table, columns in teradata_schema.items():
        bigquery_schema[table] = []
        for column in columns:
            field_type = type_mapping.get(column["type"], "STRING")
            # Handle length if applicable
            if column["length"]:
                field_type = f"{field_type}({column['length']})"
            field = bigquery.SchemaField(
                name=column["name"],
                field_type=field_type,
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
