terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.88.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ----------------------------------------------------------------------------------------------
# --- S3 Bucket ---
# ----------------------------------------------------------------------------------------------
resource "aws_s3_bucket" "transactions_bucket" {
  bucket = "personal-finance-transactions"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "transactions_versioning" {
  bucket = aws_s3_bucket.transactions_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ----------------------------------------------------------------------------------------------
# --- IAM Roles ---
# ----------------------------------------------------------------------------------------------

# IAM Role for Airflow EC2
resource "aws_iam_role" "airflow_role" {
  name = "airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com" # This IAM Role can be assumed by EC2 instances
      }
    }]
  })
}

# IAM Instance Profile for EC2 (Wraps IAM Role)
resource "aws_iam_instance_profile" "airflow_instance_profile" {
  name = "airflow-instance-profile"
  role = aws_iam_role.airflow_role.name
}

# ----------------------------------------------------------------------------------------------
# --- IAM Policies ---
# ----------------------------------------------------------------------------------------------

# IAM Policy for Airflow EC2 to access S3
resource "aws_iam_policy" "airflow_s3_policy" {
  name        = "airflow-s3-policy"
  description = "Policy for Airflow EC2 to access S3"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = "arn:aws:s3:::personal-finance-transactions" # Grants access to the bucket itself
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "arn:aws:s3:::personal-finance-transactions/*" # Grants access to objects inside the bucket
      }
    ]
  })
}
# Attach the airflow_s3_policy to the IAM Role for Airflow EC2
resource "aws_iam_role_policy_attachment" "airflow_s3_attachment" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}

# IAM Policy for Airflow EC2 to Access RDS
resource "aws_iam_policy" "airflow_rds_policy" {
  name        = "airflow-rds-policy"
  description = "Allows EC2 to access RDS instance and use IAM authentication"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "rds:DescribeDBInstances"
        ]
        Resource = aws_db_instance.transactions_db.arn
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.airflow_secrets.arn
      }
    ]
  })
}
# Attach the airflow_rds_policy to the IAM Role for Airflow EC2
resource "aws_iam_role_policy_attachment" "airflow_rds_attachment" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.airflow_rds_policy.arn
}


# ----------------------------------------------------------------------------------------------
# --- Secrets Manager ---
# ----------------------------------------------------------------------------------------------
resource "aws_secretsmanager_secret" "airflow_secrets" {
  name = "airflow-credentials"

#   lifecycle {
#     prevent_destroy = true
#   }
}

# Store the secrets in the Secret Manager
resource "aws_secretsmanager_secret_version" "airflow_secrets_values" {
  secret_id = aws_secretsmanager_secret.airflow_secrets.id
  secret_string = jsonencode({
    "RDS_PASSWORD"   = var.rds_password
  })
}

# ----------------------------------------------------------------------------------------------
# --- Security Groups ---
# ----------------------------------------------------------------------------------------------

# Security Group for EC2
resource "aws_security_group" "ec2_sg" {
  name        = "ec2-security-group"
  description = "Allow outbound traffic from EC2"

  # Allow SSH Access (Port 22) from your IP
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Public access for demo purposes (restrict in production)
    # cidr_blocks = ["${chomp(data.http.myip.response_body)}/32"]
  }

  # Allow Airflow Web UI Access (Port 8080)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Public access for demo purposes (restrict in production)
  }

  #   # Allow EC2 to initiate PostgreSQL connections to RDS
  #   egress {
  #     from_port   = 5432
  #     to_port     = 5432
  #     protocol    = "tcp"
  #     security_groups = [aws_security_group.rds_sg.id]  # Allow EC2 to talk to RDS
  #   }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Security Group for RDS
resource "aws_security_group" "rds_sg" {
  name        = "rds-security-group"
  description = "Allow EC2 to access RDS"

  # Allow incoming PostgreSQL connections from EC2
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2_sg.id] # Only allow EC2 access
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ----------------------------------------------------------------------------------------------
# --- RDS Database ---
# ----------------------------------------------------------------------------------------------
resource "aws_db_instance" "transactions_db" {
  db_name                = "transactionDB"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20 # 20 GB 
  engine                 = "mysql"
  engine_version         = "8.0"                 # Specify the desired MySQL version
  username               = var.rds_username
  password               = var.rds_password
  publicly_accessible    = false
  skip_final_snapshot    = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id] # Attach Security Group

  # lifecycle {
  #   prevent_destroy = true
  # }
}

# ----------------------------------------------------------------------------------------------
# --- EC2 Instance with Airflow ---
# ----------------------------------------------------------------------------------------------
resource "aws_instance" "airflow_ec2" {
  ami                    = "ami-0884d2865dbe9de4b"                                # Ubuntu 22.04 LTS
  instance_type          = "t3.small"                                             # 2 vCPUs, 2 GiB RAM
  iam_instance_profile   = aws_iam_instance_profile.airflow_instance_profile.name # Attach IAM instance profile
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]                         # Attach Security Group

  tags = {
    Name = "airflow-ec2"
  }
}
