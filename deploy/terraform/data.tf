# Postgres (RDS) and Valkey (ElastiCache), both Multi-AZ and private.

resource "random_password" "database" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_parameter_group" "main" {
  name   = local.name
  family = "postgres${var.postgres_version}"

  # Statement statistics, and a log line for anything slower than 250 ms:
  # the first two things to look at when a candle query gets slow.
  parameter {
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements"
    apply_method = "pending-reboot"
  }

  parameter {
    name  = "log_min_duration_statement"
    value = "250"
  }

  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
}

resource "aws_db_instance" "main" {
  identifier     = local.name
  engine         = "postgres"
  engine_version = var.postgres_version
  instance_class = var.db_instance_class

  db_name  = "pricefeed"
  username = "pricefeed"
  password = random_password.database.result

  allocated_storage     = var.db_allocated_storage_gb
  max_allocated_storage = var.db_max_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true

  multi_az               = true
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.database.id]
  parameter_group_name   = aws_db_parameter_group.main.name
  publicly_accessible    = false

  backup_retention_period      = 7
  copy_tags_to_snapshot        = true
  deletion_protection          = true
  skip_final_snapshot          = false
  final_snapshot_identifier    = "${local.name}-final"
  performance_insights_enabled = true
  auto_minor_version_upgrade   = true
}

resource "random_password" "cache" {
  length  = 64
  special = false
}

resource "aws_elasticache_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

# A primary and a replica in different zones with automatic failover. The
# price bus is pub/sub plus one hash, so a cluster-mode-disabled group is
# enough; candle caching is the only thing that would ever need sharding.
resource "aws_elasticache_replication_group" "main" {
  replication_group_id = local.name
  description          = "Pricefeed price bus and candle cache"

  engine               = "valkey"
  engine_version       = "8.1"
  parameter_group_name = "default.valkey8"
  node_type            = var.cache_node_type
  port                 = 6379

  num_cache_clusters         = 2
  automatic_failover_enabled = true
  multi_az_enabled           = true

  subnet_group_name  = aws_elasticache_subnet_group.main.name
  security_group_ids = [aws_security_group.cache.id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = random_password.cache.result

  snapshot_retention_limit = 0
}
