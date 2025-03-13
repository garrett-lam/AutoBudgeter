# ----------------------------------------------------------------------------------------------
# --- S3 Bucket Outputs ---
# ----------------------------------------------------------------------------------------------
output "s3_bucket_name" {
  description = "Name of the S3 bucket for storing transaction files."
  value       = aws_s3_bucket.transactions_bucket.id
}

# ----------------------------------------------------------------------------------------------
# --- IAM Outputs ---
# ----------------------------------------------------------------------------------------------
output "airflow_user_access_key" {
  value = aws_iam_access_key.airflow_user_key.id
}

output "airflow_user_secret_key" {
  value     = aws_iam_access_key.airflow_user_key.secret
  sensitive = true
}

# ----------------------------------------------------------------------------------------------
# --- RDS Database Outputs ---
# ----------------------------------------------------------------------------------------------
output "rds_instance_endpoint" {
  description = "The address of the RDS instance (RDS_HOST)"
  value       = aws_db_instance.transactions_db.address
}
output "rds_instance_db_name" {
  description = "The name of database in the RDS instance (RDS_DB)"
  value       = aws_db_instance.transactions_db.db_name
}

output "rds_instance_username" {
  description = "The username for the RDS instance (RDS_USERNAME)"
  value       = aws_db_instance.transactions_db.username
}
output "rds_instance_password" {
  description = "The password for the RDS instance (RDS_PASSWORD)"
  value       = aws_db_instance.transactions_db.password
  sensitive   = true
}
