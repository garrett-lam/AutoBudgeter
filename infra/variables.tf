# NOTE: Create a separate file called terraform.tfvars to set these values, if privacy is not a concern, set them in main.tf
variable "aws_region" {
  description = "Your AWS region"
  type        = string
}

variable "rds_db_name" {
  description = "Name of the RDS database."
  type        = string
}

variable "rds_username" {
  description = "Username for RDS database"
  type        = string
}
variable "rds_password" {
  description = "Password for RDS database"
  type        = string
  sensitive   = true
}