# billing_export_bq_gcs
This is a POC for exporting Google Cloud billing data to GCS after processing it in BigQuery.

### Requirements
1. Existing BigQuery dataset that has your billing data
2. Existing bucket where you wish to send your data

### Steps

1. Create a pubsub topic

```shell
gcloud pubsub topics create billing_export
```

2. Create a Cloud function that subscribes to PUB/SUB topic(billing_export)

This Cloud function ships all the billing data of the day before the current date when the function was run from the billing BigQuery dataset to a file inside a bucket you specify. Also, you can specify a param to make it query more than one day. For example to query the last 30 days not including current day you can change the environment variable passed to the function as follows: `(previous_days_to_query='30')`.

If you trigger the cloud function multiple times, it will replace the billing export file for the day before with the latest billing data in the BigQuery dataset.

```shell
gcloud functions deploy export_billing --runtime python37 --trigger-topic billing_export --set-env-vars BQ_PROJECT_ID='{BQ project ID}',BQ_DATASET='{billing dataset name}',BQ_TABLE='{billing table name}',PREVIOUS_DAYS_TO_QUERY='1',BILLING_BUCKET_NAME='{billing bucket name}'
```

3. Publish a message to the billing topic to trigger the cloud function and test everything is working. Make sure to change the attributes to match your current environment:

`NOTE`: We are passing an empty message because either a message or an attribute should be specified when publishing to a PubSub topic

```shell
gcloud pubsub topics publish billing_export --message ' '
```

4. Create a cloud scheduler job to schedule a daily export(21:00 UTC) of the billing data by publishing a message to the billing_export pubsub topic

```shell
gcloud scheduler jobs create pubsub daily_billing_export --schedule "0 21 * * *" --topic billing_export --message-body " "
```
