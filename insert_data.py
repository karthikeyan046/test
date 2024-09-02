
from google.cloud import bigquery

def insert_record(project_id, dataset_id, table_id, value1, value2, value3):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    rows_to_insert = [(value1, value2, value3)]
    errors = client.insert_rows(table_ref, rows_to_insert)
    if errors:
        print("Error inserting record:", errors)
    else:
        print("Record inserted successfully")

# Example usage:
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table_id = "your-table-id"
value1 = "example_value1"
value2 = "example_value2"
value3 = "example_value3"

insert_record(project_id, dataset_id, table_id, value1, value2, value3)

