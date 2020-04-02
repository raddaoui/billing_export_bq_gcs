# billing_export_bq_gcs
This is a POC for exporting Google Cloud billing data to GCS after processing it in BigQuery.

### requirements
1. Existing BigQuery dataset that has your billing data
2. Existing bucket where you wish to send your data

### setps

1. Create the pubsub topic(billing)

```shell
gcloud pubsub topics create billing
```

2. Create a Cloud function that subscribes to PUB/SUB topic(billing)

`NOTE`: the Cloud function ships all the billing data of the day before the current date when the function was run from the BigQuery dataset to a file inside a bucket you specify. Also, you can specify a param to make it query more than one day. For example to query the last 30 days not including current day you can pass in attribute `(previous_days_to_query='30')` as shown later in step 3.

```shell
gcloud functions deploy export_billing --runtime python37 --trigger-topic billing
```

3. publish a message to the billing topic to trigger the cloud function and test everything is working. Make sure to change the attributes to match your current environment:
`NOTE`: if you trigger the cloud function multiple times, it will replace the billing export file for the day before with the latest billing data in the BigQuery dataset.

```shell
gcloud pubsub topics publish billing --message '' --attribute=bq_project_id='{project_id}',bq_dataset='{billing dataset name}',bq_table='{billing table name}',previous_days_to_query='1',billing_bucket_name='{billing bucket name}'
```

4. Create a cloud scheduler job to schedule a daily export(21:00 UTC) of the billing data by publishing a message to the billing pubsub topic

```shell
gcloud beta scheduler jobs create pubsub daily_billing_export --schedule "0 21 * * *" --topic billing --message-body "test" --attributes bq_project_id='{project_id}',bq_dataset='{billing dataset name}',bq_table='{billing table name}',previous_days_to_query='1',billing_bucket_name='{billing bucket name}'
```
