variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
  default     = "sound-machine-457008-i6"
}

variable "region" {
  description = "Google Cloud region"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for data storage"
  type        = string
  default     = "retail-data-landing-zone"
}