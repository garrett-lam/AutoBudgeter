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
# --- Security Groups ---
# ----------------------------------------------------------------------------------------------

# Security Group for EC2
resource "aws_security_group" "ec2_sg" {
  name        = "ec2-security-group"
  description = "Allow outbound traffic from EC2"

  # Allow SSH Access (Port 22)
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow Airflow Web UI Access (Port 8080)
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

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

  # Allow incoming PostgreSQL connections (Port 5432)
  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    #security_groups = [aws_security_group.ec2_sg.id] # Only allow EC2 access
  }

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
  db_name                = "transactionsDB"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20 # 20 GB 
  engine                 = "postgres"
  engine_version         = "16.3"
  username               = var.rds_username
  password               = var.rds_password
  publicly_accessible    = true
  skip_final_snapshot    = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id] # Attach Security Group

  lifecycle {
    prevent_destroy = true
  }
}

resource "null_resource" "database_setup" {
  depends_on = [aws_db_instance.transactions_db]

  provisioner "local-exec" {
    command     = <<-EOF
      PGPASSWORD=${var.rds_password} psql \
      -h ${aws_db_instance.transactions_db.address} \
      -p 5432 \
      -U ${var.rds_username} \
      -d ${aws_db_instance.transactions_db.db_name} \
      -f ${path.module}/db_setup.sql
    EOF
    interpreter = ["bash", "-c"]
  }
}

# ----------------------------------------------------------------------------------------------
# --- EC2 Instance with Airflow ---
# ----------------------------------------------------------------------------------------------
resource "aws_instance" "airflow_ec2" {
  ami                    = "ami-0884d2865dbe9de4b" # Ubuntu 22.04 LTS
  instance_type          = "t3.large"              # 2 vCPUs, 8 GiB RAM (4 GB minimum for Airflow)
  key_name               = "airflow-ec2-key"
  iam_instance_profile   = aws_iam_instance_profile.airflow_instance_profile.name # Attach IAM instance profile
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]                         # Attach Security Group

  root_block_device {
    volume_size = 30 # GB
    volume_type = "gp3"
  }
  tags = {
    Name = "airflow-ec2"
  }

  # user_data = <<-EOF
  #   #!/bin/bash
  #   # Update package list and install required packages
  #   apt-get update -y
  #   apt-get install -y ca-certificates curl gnupg lsb-release git

  #   # Install Docker
  #   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
  #   echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  #   apt-get update -y
  #   apt-get install -y docker-ce docker-ce-cli containerd.io

  #   # Add the 'ubuntu' user (or your instance’s default user) to the docker group so you can run docker without sudo
  #   usermod -aG docker ubuntu

  #   # Install Docker Compose (using the v2 plugin)
  #   apt-get install -y docker-compose-plugin

  #   # Switch to the ubuntu user, clone your GitHub repo, and run docker-compose up
  #   su - ubuntu -c "git clone https://github.com/yourusername/yourrepo.git ~/yourrepo"
  #   su - ubuntu -c "cd ~/yourrepo && docker compose up -d"
  # EOF
}
