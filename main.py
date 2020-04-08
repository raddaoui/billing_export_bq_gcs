import base64
from google.cloud import bigquery
from google.cloud import storage
from datetime import date
from datetime import timedelta
from datetime import datetime
import logging
import json
import os

def export_billing(event, context):
    PREVIOUS_DAYS_TO_QUERY='1' # Default unless you pass a number to the pubsub topic
    BILLING_TEMP_TABLE="billing_export_temp"
    if 'data' in event:
        data = base64.b64decode(event['data']).decode('utf-8')
        if data.isdigit():
            if int(data) > 1:
                PREVIOUS_DAYS_TO_QUERY=data
                print("Querying data back {} days".format(PREVIOUS_DAYS_TO_QUERY))
    try:
        BQ_PROJECT_ID=os.environ['BQ_PROJECT_ID']
        BQ_DATASET=os.environ['BQ_DATASET']
        BQ_TABLE=os.environ['BQ_TABLE']
        BILLING_BUCKET_NAME=os.environ['BILLING_BUCKET_NAME']
    except KeyError as k:
        logging.error("environment variable: {} is missing".format(str(k)))
        return

    print("Importing from BQ dataset: {}.{}.{}. Exporting to bucket: {}".format(BQ_PROJECT_ID,BQ_DATASET,BQ_TABLE,BILLING_BUCKET_NAME))
    yesterday_date = date.today() - timedelta(days = 1)
    filename = "{}_{}".format(str(yesterday_date), str(PREVIOUS_DAYS_TO_QUERY))

    destination_uri = "gs://{}/{}".format(BILLING_BUCKET_NAME, filename + "-*.csv") # need wildcard as export shards on > 1 GB
    client = bigquery.Client()

    query = """
        CREATE OR REPLACE TABLE {BQ_DATASET}.billing_export_temp AS (
            SELECT billing_account_id,
                service.id          AS service_id,
                service.description AS service_description,
                sku.id              AS sku_id,
                sku.description     AS sku_description,
                usage_start_time,
                usage_end_time,
                project.id                     AS project_id,
                project.NAME                   AS project_name,
                project.ancestry_numbers       AS project_ancestry_numbers,
                to_json_string(project.labels) AS project_labels,
                to_json_string(labels)         AS labels,
                to_json_string(system_labels)  AS system_labels,
                location.location              AS location,
                location.country               AS location_country,
                location.region                AS location_region,
                location.zone                  AS location_zone,
                export_time,
                cost,
                currency,
                currency_conversion_rate,
                usage.amount                  AS usage_amount,
                usage.unit                    AS usage_unit,
                usage.amount_in_pricing_units AS usage_amount_in_pricing_units,
                usage.pricing_unit            AS usage_pricing_unit,
                to_json_string(credits)       AS credits,
                invoice.month                 AS invoice_month,
                cost_type
            FROM   {BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}
            WHERE  date(_partitiontime) BETWEEN date_sub(CURRENT_DATE(), interval {PREVIOUS_DAYS_TO_QUERY} day) AND date_sub(CURRENT_DATE(), interval 1 day))
    """.format(BQ_PROJECT_ID=BQ_PROJECT_ID,BQ_DATASET=BQ_DATASET,BQ_TABLE=BQ_TABLE,PREVIOUS_DAYS_TO_QUERY=PREVIOUS_DAYS_TO_QUERY)

    query_job = client.query(query)
    results = query_job.result()

    dataset_ref = client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(BILLING_TEMP_TABLE)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",
    )  # Extract into < 1 GB objects
    extract_job.result()  # Waits for job to complete.

    # delete table now that we're done
    client.delete_table(table_ref)

    # Connect to GCS and compose all the blobs into a single object then clean up temp objects
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BILLING_BUCKET_NAME)
    blobs = list(storage_client.list_blobs(
        BILLING_BUCKET_NAME, prefix=filename+"-", delimiter='/'
    ))

    final_name = filename + '.csv'

    if len(blobs) > 1: # Check for compose or rename required
        blob = bucket.blob(final_name)
        blob.compose(blobs)
        for blob in blobs:
            blob.delete()
    else:
        new_blob = bucket.rename_blob(blobs.pop(),final_name)
