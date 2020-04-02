import base64
from google.cloud import bigquery
from google.cloud import storage
from datetime import date
from datetime import timedelta
from datetime import datetime
import logging
import json

# This is to handle BQ returning a datatime function, not actual string we can put into Splunk
def dconvert(o):
    if isinstance(o, datetime):
        return o.__str__()

def export_billing(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    if 'data' in event:
        data = base64.b64decode(event['data']).decode('utf-8')
    else:
        data = ''

    if 'attributes' not in event or event['attributes'] == None:
        logging.error('no attributes passed to cloud function')
        return
    else:
        attributes = event['attributes']

    try:
        BQ_PROJECT_ID=attributes['bq_project_id']
        BQ_DATASET=attributes['bq_dataset']
        BQ_TABLE=attributes['bq_table']
        BILLING_BUCKET_NAME=attributes['billing_bucket_name']
    except KeyError as k:
        logging.error("attribute: {} is missing".format(str(k)))
        return

    try:
        PREVIOUS_DAYS_TO_QUERY=attributes['previous_days_to_query']
    except KeyError as k:
        # if not specified take data of one day by default
        PREVIOUS_DAYS_TO_QUERY='1'

    print("bq dataset: {}.{}.{}, days to query: {}, bucket: {}".format(BQ_PROJECT_ID,BQ_DATASET,BQ_TABLE,PREVIOUS_DAYS_TO_QUERY,BILLING_BUCKET_NAME))

    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT billing_account_id, service, sku, usage_start_time,
          usage_end_time, project.id AS project_id, project.name AS project_name,
          project.ancestry_numbers AS project_ancestry_numbers,
          TO_JSON_STRING(project.labels) AS project_labels, TO_JSON_STRING(labels) AS labels,
          TO_JSON_STRING(system_labels) AS system_labels, location, export_time,
          cost, currency, currency_conversion_rate, usage, invoice, cost_type, credits
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE DATE(_PARTITIONTIME) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {PREVIOUS_DAYS_TO_QUERY} DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """.format(BQ_PROJECT_ID=BQ_PROJECT_ID,BQ_DATASET=BQ_DATASET,BQ_TABLE=BQ_TABLE,PREVIOUS_DAYS_TO_QUERY=PREVIOUS_DAYS_TO_QUERY)

    # Make an API request.
    query_job = client.query(query)
    records = [json.dumps(dict(row), default = dconvert) for row in query_job]
    records_dump = '\n'.join(records)
    # Construct a Storage client object.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BILLING_BUCKET_NAME)
    yesterday_date = date.today() - timedelta(days = 1)
    filename = str(yesterday_date) + '_' + PREVIOUS_DAYS_TO_QUERY
    # Create a file in the bucket and store the data.
    blob = bucket.blob(filename)
    blob.upload_from_string(records_dump)
