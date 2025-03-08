# In terraform.tfvars, you can set the values for these variables:
variable "aws_region" {
  description = "Your AWS region"
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