from google.oauth2 import service_account
from google.cloud import bigquery

def execute():
    print("Load!")

    project_id = "buenos-aires-data"
    table_id = f"{project_id}.air_quality.measurements"

    # create authentication credentials
    gcp_credentials = service_account.Credentials.from_service_account_file('buenos-aires-data-c8b43156979d.json')
    
    # create the bigquery client
    bq_client = bigquery.Client(credentials=gcp_credentials)

    # create the table schema
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("measured_on", "DATETIME"),
            bigquery.SchemaField("station", "STRING"),
            bigquery.SchemaField("air_attribute", "STRING"),
            bigquery.SchemaField("value", "FLOAT"),
        ],
        source_format=bigquery.SourceFormat.CSV,
    )
    
    # create the Job
    with open("tmp/processed_air_quality.csv", "rb") as file:
        csv_load_job = bq_client.load_table_from_file(
            file, table_id, job_config=job_config
        )
    
    csv_load_job.result()

    print("SUCCESS")

