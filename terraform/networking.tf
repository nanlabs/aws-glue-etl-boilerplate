# Security Groups for Glue Connections
# This file contains networking resources for AWS Glue connections

# Security group for Glue connections
resource "aws_security_group" "glue_connection" {
  name_prefix = "${local.name_prefix}-glue-connection-"
  description = "Security group for AWS Glue connections"
  vpc_id      = var.vpc_id

  # Outbound rules for database connections
  egress {
    description = "PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.database_cidr_blocks
  }

  egress {
    description = "MongoDB/DocumentDB"
    from_port   = 27017
    to_port     = 27017
    protocol    = "tcp"
    cidr_blocks = var.database_cidr_blocks
  }

  # HTTPS outbound for AWS services
  egress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP outbound for package downloads
  egress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Self-referencing rule for Glue job communication
  egress {
    description = "Self"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Self"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-glue-connection-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}
# AWS Glue Connection for database connectivity
resource "aws_glue_connection" "database_connection" {
  count = var.vpc_id != null && var.subnet_id != null ? 1 : 0
  
  name = "${local.name_prefix}-database-connection"
  
  connection_type = "JDBC"
  
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://localhost:5432/postgres"
    USERNAME           = "postgres"
    PASSWORD           = "password"
  }
  
  physical_connection_requirements {
    availability_zone      = data.aws_subnet.glue_subnet[0].availability_zone
    security_group_id_list = [aws_security_group.glue_connection.id]
    subnet_id             = var.subnet_id
  }
  
  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-database-connection"
  })
}

# Data source to get subnet information
data "aws_subnet" "glue_subnet" {
  count = var.subnet_id != null ? 1 : 0
  id    = var.subnet_id
}