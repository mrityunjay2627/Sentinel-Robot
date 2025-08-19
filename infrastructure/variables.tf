variable "mwaa_dag_s3_bucket_name" {
  description = "A globally unique name for the S3 bucket to store Airflow DAGs"
  type        = string
  default     = "sentinel-mwaa-dags-s3" 
}

variable "my_ip_address" {
  description = "Your public IP address for accessing the Airflow UI"
  type        = string
  # Actual/Authorized IP address
  default     = "192.168.0.179/32" 
}