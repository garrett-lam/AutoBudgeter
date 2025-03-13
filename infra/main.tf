# ----------------------------------------------------------------------------------------------
# --- S3 Bucket ---
# ----------------------------------------------------------------------------------------------
resource "aws_s3_bucket" "transactions_bucket" {
  bucket        = "personal-finance-transactions"
  force_destroy = true # Allow deletion of non-empty bucket
}

# ----------------------------------------------------------------------------------------------
# --- RDS Database ---
# ----------------------------------------------------------------------------------------------
resource "aws_db_instance" "transactions_db" {
  db_name                = var.rds_db_name
  instance_class         = "db.t3.micro"
  allocated_storage      = 20 # 20 GB
  engine                 = "postgres"
  engine_version         = "16.3"
  username               = var.rds_username
  password               = var.rds_password
  publicly_accessible    = true
  skip_final_snapshot    = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]

  # Comment out lifecycle block if you would like to allow deletion of the RDS instance
  lifecycle {
    prevent_destroy = true
  }
}


# Set up schema for table in RDS instance
resource "null_resource" "database_setup" {
  depends_on = [aws_db_instance.transactions_db]

  provisioner "local-exec" {
    command     = <<-EOF
      PGPASSWORD=${var.rds_password} psql \
        --host=${aws_db_instance.transactions_db.address} \
        --port=5432 \
        --username=${var.rds_username} \
        --dbname=${aws_db_instance.transactions_db.db_name} \
        --file=${path.module}/db_setup.sql
    EOF
    interpreter = ["bash", "-c"]
  }
}

# ----------------------------------------------------------------------------------------------
# --- IAM ---
# ----------------------------------------------------------------------------------------------

# Data source to get the current AWS account ID
data "aws_caller_identity" "current" {}

# Create the IAM user for Airflow
resource "aws_iam_user" "airflow_user" {
  name = "airflow-user"
  # Comment out lifecycle block if you would like to allow deletion of the IAM user
  lifecycle {
    prevent_destroy = true
  }

}

# Create an access key for the IAM user
resource "aws_iam_access_key" "airflow_user_key" {
  user = aws_iam_user.airflow_user.name
}

# Create an IAM policy that allows access to S3 and RDS (RDS write access is on application-level, cannot be configured via IAM)
resource "aws_iam_policy" "airflow_policy" {
  name        = "airflow-s3-rds-access-policy"
  description = "Policy for Airflow to access S3 and describe RDS instances"
  policy = jsonencode({
    "Version" = "2012-10-17",
    "Statement" = [
      {
        "Effect" = "Allow",
        "Action" = [
          "s3:ListBucket"
        ],
        "Resource" = [
          "${aws_s3_bucket.transactions_bucket.arn}",
        ]
      },
      {
        "Effect" = "Allow",
        "Action" = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        "Resource" = [
          "${aws_s3_bucket.transactions_bucket.arn}/*"
        ]
      },
      {
        "Effect" = "Allow",
        "Action" = [
          "rds:DescribeDBInstances"
        ],
        "Resource" = "*"
      }
    ]
  })
}

# Attach the above policy to the IAM user
resource "aws_iam_user_policy_attachment" "airflow_policy_attach" {
  user       = aws_iam_user.airflow_user.name
  policy_arn = aws_iam_policy.airflow_policy.arn
}

# ----------------------------------------------------------------------------------------------
# --- Security Groups ---
# ----------------------------------------------------------------------------------------------

# Security Group for RDS
resource "aws_security_group" "rds_sg" {
  name = "rds-security-group"

  # Allow incoming PostgreSQL connections (Port 5432)
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
