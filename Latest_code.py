import os
import json
import pyodbc
#import teradatasql
from google.cloud import bigquery
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)

# Set your Google Cloud project ID and dataset ID
project_id = ''
dataset_id = ''
# Ensure GOOGLE_APPLICATION_CREDENTIALS is set to your service account key file
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path_to_your_service_account_key.json"

# Initialize the BigQuery client
bq_client = bigquery.Client(project=project_id)

# Set up your Teradata connection
teradata_connection_string = 'DRIVER={Teradata};DBCNAME=your_teradata_server;UID=your_username;PWD=your_password'

def get_sql_server_schema(connection_string, table):
    print(f"Starting schema extraction for {table} from SQL Server...")
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    schema = []
    
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
        "numeric": "INTEGER",
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
    
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table}'
    """)
    columns = cursor.fetchall()
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
        schema.append(column_schema)
    
    print(f"Schema extraction for {table} from SQL Server completed.")
    return schema

def convert_schema_to_bigquery(sql_server_schema):
    print("Starting schema conversion to BigQuery format...")
    bigquery_schema = []
    
    for column in sql_server_schema:
        field_type = column["type"]
        description = f"Max Length: {column['length']}" if column['length'] else None
        if column["type"] == "STRING" and column["length"]:
            field_type = f"{field_type}"
        field = bigquery.SchemaField(
            name=column["name"],
            field_type=field_type,
            mode="NULLABLE" if column["nullable"] else "REQUIRED",
            description=description,
            max_length=column['length']
        )
        bigquery_schema.append(field)
    
    print("Schema conversion to BigQuery format completed.")
    return bigquery_schema

def create_bigquery_table(project_id, dataset_id, table, bigquery_schema):
    print(f"Starting table creation for {table} in BigQuery...")
    print(bigquery_schema)
    table_id = f"{project_id}.{dataset_id}.{table}"
    print(table_id)
    #table_ref = bq_client.dataset(dataset_id).table(table_id)
    #bq_schema = [bigquery.SchemaField(field.name, field.field_type, description=field.description, mode=field.mode) for field in bigquery_schema]
    #table = bigquery.Table(table_ref, schema=bigquery_schema)
    
    table = bigquery.Table(table_id, schema=bigquery_schema)
    #table = client.create_table(table_id, exists_ok=True)
    
    try:
        table = bq_client.create_table(table)
        print(f'Table {table_id} created successfully.')
    except Exception as e:
        print(f'Error creating table {table_id}: {e}')

def get_teradata_data(query):
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=miq.database.windows.net;DATABASE=MLPOC;UID=mpdadmin;PWD=wipro@123'
    with pyodbc.connect(connection_string) as con:
        return pd.read_sql(query, con)

def convert_dataframe_to_bigquery_schema(dataframe, table_id):
    table = bq_client.get_table(table_id)
    schema = {field.name: field.field_type for field in table.schema}

    type_mapping = {
        'STRING': 'object',
        'BYTES': 'object',
        'INTEGER': 'int64',
        'FLOAT': 'float64',
        'BOOLEAN': 'bool',
        'TIMESTAMP': 'datetime64[ns]',
        'DATE': 'datetime64[ns]',
        'TIME': 'datetime64[ns]',
        'DATETIME': 'datetime64[ns]',
        'GEOGRAPHY': 'object',
        'NUMERIC': 'float64',
        'BIGNUMERIC': 'float64'
    }
    '''
    for column in dataframe.columns:
        if column in schema:
            expected_type = schema[column]
            if expected_type in type_mapping:
                dataframe[column] = dataframe[column].astype(type_mapping[expected_type])

    return dataframe
    
    for column in dataframe.columns:
        if column in schema:
            expected_type = schema[column]
            if expected_type in type_mapping:
                try:
                    # Handle None values before type conversion
                    if type_mapping[expected_type] == 'int64':
                        dataframe[column] = dataframe[column].fillna(0).astype(type_mapping[expected_type])
                    elif type_mapping[expected_type] == 'float64':
                        dataframe[column] = dataframe[column].fillna(0.0).astype(type_mapping[expected_type])
                    else:
                        dataframe[column] = dataframe[column].astype(type_mapping[expected_type])
                except Exception as e:
                    print(f"Error converting column {column} to {expected_type}: {e}")

    return dataframe
    '''

    for column in dataframe.columns:
        if column in schema:
            expected_type = schema[column]
            if expected_type in type_mapping:
                try:
                    # Convert column to appropriate type while preserving null values
                    if type_mapping[expected_type] == 'int64':
                        dataframe[column] = dataframe[column].astype('Int64')  # Pandas nullable integer type
                    elif type_mapping[expected_type] == 'float64':
                        dataframe[column] = dataframe[column].astype('float64')
                    else:
                        dataframe[column] = dataframe[column].astype(type_mapping[expected_type])
                except Exception as e:
                    print(f"Error converting column {column} to {expected_type}: {e}")

    return dataframe 



def load_data_to_bigquery(dataframe, table_id):
    dataframe = convert_dataframe_to_bigquery_schema(dataframe, table_id)
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = bq_client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}.")

def validate_data_transfer(teradata_table, bigquery_table):
    teradata_count_query = f"SELECT COUNT(*) AS cnt FROM {teradata_table}"
    bigquery_count_query = f"SELECT COUNT(*) AS cnt FROM `{bigquery_table}`"

    teradata_count = get_teradata_data(teradata_count_query)['cnt'][0]

    query_job = bq_client.query(bigquery_count_query)
    bigquery_count = [row.cnt for row in query_job.result()][0]

    if teradata_count == bigquery_count:
        print(f"Data validation successful for table {teradata_table}. Row count matches: {teradata_count}")
    else:
        print(f"Data validation failed for table {teradata_table}. Teradata row count: {teradata_count}, BigQuery row count: {bigquery_count}")

def transfer_table(sql_server_table, bigquery_table):
    print(f"Processing table {sql_server_table}...")
    
    # Step 1: Get SQL Server schema
    sql_server_connection_string = ''
    sql_server_schema = get_sql_server_schema(sql_server_connection_string, sql_server_table)
    
    # Save SQL Server schema to file
    with open(f'{sql_server_table}_schema.json', 'w') as f:
        json.dump(sql_server_schema, f, indent=4)
    
    # Step 2: Convert SQL Server schema to BigQuery schema
    bigquery_schema = convert_schema_to_bigquery(sql_server_schema)
    print(bigquery_schema)
    # Step 3: Create table in BigQuery
    create_bigquery_table(project_id, dataset_id, sql_server_table, bigquery_schema)
    
    # Save BigQuery schema to file
    bigquery_schema_json = [field.to_api_repr() for field in bigquery_schema]
    with open(f'{sql_server_table}_bigquery_schema.json', 'w') as f:
        json.dump(bigquery_schema_json, f, indent=4)
    
    # Step 4: Transfer data from Teradata to BigQuery
    print(f"Starting data transfer for table {sql_server_table}...")
    query = f"SELECT * FROM {sql_server_table}"
    data = get_teradata_data(query)
    load_data_to_bigquery(data, bigquery_table)
    print(f"Data transfer for table {sql_server_table} completed.")

    # Step 5: Validate data transfer
    validate_data_transfer(sql_server_table, bigquery_table)
    print(f"Processing for table {sql_server_table} completed.")
   
def main():
    # List of tables to transfer with their corresponding Teradata and BigQuery table names
    tables_to_transfer = [
        {"sql_server_table": "data_files","bigquery_table": f"{project_id}.{dataset_id}.data_files"},
           # Add more tables as needed
    ]

    for table in tables_to_transfer:
        transfer_table(table["sql_server_table"], table["bigquery_table"])

if __name__ == '__main__':
    main()
