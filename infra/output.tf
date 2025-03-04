# ----------------------------------------------------------------------------------------------
# --- S3 Bucket Outputs ---
# ----------------------------------------------------------------------------------------------
output "s3_bucket_name" {
  description = "Name of the S3 bucket for storing transaction files."
  value       = aws_s3_bucket.transactions_bucket.id
}

# ----------------------------------------------------------------------------------------------
# --- EC2 Instance Outputs ---
# ----------------------------------------------------------------------------------------------
output "ec2_instance_id" {
  description = "The ID of the EC2 instance running Airflow."
  value       = aws_instance.airflow_ec2.id
}

output "ec2_public_ip" {
  description = "Public IP address of the EC2 instance (for SSH & Airflow UI)."
  value       = aws_instance.airflow_ec2.public_ip
}

output "airflow_web_ui" {
  description = "URL to access the Airflow Web UI."
  value       = "http://${aws_instance.airflow_ec2.public_ip}:8080"
}

# ----------------------------------------------------------------------------------------------
# --- RDS Database Outputs ---
# ----------------------------------------------------------------------------------------------
output "rds_instance_endpoint" {
  description = "The endpoint of the RDS instance (RDS_HOST)"
  value       = aws_db_instance.transactions_db.endpoint
}
output "rds_db_name" {
  description = "The name of the initial database created in the RDS instance (RDS_DB)"
  value       = aws_db_instance.transactions_db.db_name
}

output "rds_user" {
  description = "The master username for the RDS instance (RDS_USER)"
  value       = aws_db_instance.transactions_db.username
}

# ----------------------------------------------------------------------------------------------
# --- IAM Role & Instance Profile Outputs ---
# ----------------------------------------------------------------------------------------------
output "iam_role_name" {
  description = "IAM Role assigned to EC2 for accessing AWS services."
  value       = aws_iam_role.airflow_role.name
}

output "iam_instance_profile_name" {
  description = "IAM Instance Profile assigned to EC2."
  value       = aws_iam_instance_profile.airflow_instance_profile.name
}

# ----------------------------------------------------------------------------------------------
# --- AWS Secrets Manager Outputs ---
# ----------------------------------------------------------------------------------------------
output "secrets_manager_arn" {
  description = "ARN of the AWS Secrets Manager storing OpenAI API Key & RDS credentials."
  value       = aws_secretsmanager_secret.airflow_secrets.arn
  sensitive   = true
}
