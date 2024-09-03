
import pyodbc
import json
from google.cloud import bigquery


def create_bigquery_tables(project_id, dataset_id, bigquery_schema):
    client = bigquery.Client(project=project_id)
    
    for table, schema in bigquery_schema.items():
        table_id = f"{project_id}.{dataset_id}.{table}"
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        #print(f"Created table {table_id}")

# Main function
def main():
    project_id = ''
    dataset_id = ''
    bigquery_schema="bigquery_schema.json"
    # Load BigQuery schema from file
    with open(bigquery_schema, "r") as schema_file:
        bigquery_schema = json.load(schema_file)
    # Create tables in BigQuery
    create_bigquery_tables(project_id, dataset_id, bigquery_schema)

    # Save BigQuery schema to file

if __name__ == '__main__':
    main()
