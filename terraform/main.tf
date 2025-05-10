/**
 * Retail Data Pipeline Infrastructure
 *
 * This Terraform configuration sets up the infrastructure for the retail data pipeline:
 * - GCS bucket for data storage
 * - BigQuery dataset for analytics
 * - Cloud Function for data generation
 * - Cloud Composer (Airflow) environment for orchestration
 */

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS bucket for data storage
resource "google_storage_bucket" "data_bucket" {
  name     = var.bucket_name
  location = var.region
  force_destroy = true
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 90  # Keep data for 90 days
    }
    action {
      type = "Delete"
    }
  }
}

# Create folders in the bucket
resource "null_resource" "create_folders" {
  depends_on = [google_storage_bucket.data_bucket]
  
  provisioner "local-exec" {
    command = <<EOT
      gsutil mb -p ${var.project_id} gs://${var.bucket_name}/master
      gsutil mb -p ${var.project_id} gs://${var.bucket_name}/customers
      gsutil mb -p ${var.project_id} gs://${var.bucket_name}/products
      gsutil mb -p ${var.project_id} gs://${var.bucket_name}/orders
    EOT
  }
}

# GCS bucket for Cloud Function code
resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project_id}-functions"
  location = var.region
  force_destroy = true
  uniform_bucket_level_access = true
}

# Zip and upload Cloud Function code
data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "../cloud_functions/data_generator"
  output_path = "function.zip"
}

resource "google_storage_bucket_object" "function_zip" {
  name   = "function-${data.archive_file.function_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.function_zip.output_path
}

# Cloud Function for data generation
resource "google_cloudfunctions_function" "generate_data" {
  name        = "generate-data-function"
  description = "Generates synthetic retail data"
  runtime     = "python39"
  
  source_archive_bucket = google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.function_zip.name
  
  entry_point = "cloud_function_entry"
  trigger_http = true
  
  environment_variables = {
    BUCKET_NAME = google_storage_bucket.data_bucket.name
  }
  
  timeout = 540  # 9 minutes
  available_memory_mb = 2048
}

# BigQuery dataset
resource "google_bigquery_dataset" "retail_data" {
  dataset_id                  = "retail_data"
  friendly_name               = "Retail Data"
  description                 = "Dataset for retail data analytics"
  location                    = var.region
  default_table_expiration_ms = null
}

# Cloud Composer (Airflow) environment
resource "google_composer_environment" "retail_data_pipeline" {
  name   = "retail-data-pipeline"
  region = var.region
  
  config {
    node_count = 3
    
    software_config {
      image_version = "composer-2.0.12-airflow-2.2.3"
      
      airflow_config_overrides = {
        core-dags_are_paused_at_creation = "True"
      }
      
      env_variables = {
        BUCKET_NAME = google_storage_bucket.data_bucket.name
      }
    }
  }
}
