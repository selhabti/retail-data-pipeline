output "data_bucket_name" {
  description = "The name of the GCS bucket where data is stored"
  value       = google_storage_bucket.data_bucket.name
}

output "function_url" {
  description = "The URL of the deployed Cloud Function"
  value       = google_cloudfunctions_function.generate_data.https_trigger_url
}

output "composer_airflow_uri" {
  description = "The URI of the Apache Airflow UI for the Cloud Composer environment"
  value       = google_composer_environment.retail_data_pipeline.config.0.airflow_uri
}

output "bigquery_dataset" {
  description = "The ID of the BigQuery dataset"
  value       = google_bigquery_dataset.retail_data.dataset_id
}
