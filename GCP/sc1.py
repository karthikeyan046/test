from google.cloud import bigquery


import google.oauth2.credentials
creds = google.oauth2.credentials.default()
username = creds.id_token['email'].split('@')[0]
print(username)




def list_datasets(project_id):
    client = bigquery.Client(project=project_id)

    datasets = list(client.list_datasets())
    if datasets:
        print(f"Datasets in project {project_id}:")
        for dataset in datasets:
            print(f"\t{dataset.dataset_id}")
    else:
        print(f"Project {project_id} does not contain any datasets.")

def main():
    project_id = ''
    list_datasets(project_id)

if __name__ == '__main__':
    main()
