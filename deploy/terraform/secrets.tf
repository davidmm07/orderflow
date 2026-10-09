# Connection strings live in Secrets Manager and reach the tasks as
# environment variables injected by ECS at start; they never appear in a
# task definition, an image or a log line.

resource "aws_secretsmanager_secret" "database_url" {
  name                    = "${local.name}/database-url"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id     = aws_secretsmanager_secret.database_url.id
  secret_string = "postgres://pricefeed:${random_password.database.result}@${aws_db_instance.main.address}:5432/pricefeed?sslmode=require"
}

resource "aws_secretsmanager_secret" "redis_url" {
  name                    = "${local.name}/redis-url"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "redis_url" {
  secret_id     = aws_secretsmanager_secret.redis_url.id
  secret_string = "rediss://:${random_password.cache.result}@${aws_elasticache_replication_group.main.primary_endpoint_address}:6379"
}

# Optional provider credentials: created only when a value is given.
locals {
  optional_secrets = {
    PRICEFEED_EVM_RPC_URL       = var.evm_rpc_url
    PRICEFEED_SOLANA_RPC_URL    = var.solana_rpc_url
    PRICEFEED_COINGECKO_API_KEY = var.coingecko_api_key
  }
  # Keys are known at plan time; only the values are sensitive.
  provided_secrets = toset([
    for name in keys(local.optional_secrets) : name if local.optional_secrets[name] != null
  ])
}

resource "aws_secretsmanager_secret" "optional" {
  for_each = nonsensitive(local.provided_secrets)

  name                    = "${local.name}/${lower(replace(each.key, "_", "-"))}"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "optional" {
  for_each = nonsensitive(local.provided_secrets)

  secret_id     = aws_secretsmanager_secret.optional[each.key].id
  secret_string = local.optional_secrets[each.key]
}

# Shared secret between CloudFront and the load balancer, so clients
# cannot skip the CDN and its caching by calling the ALB directly.
resource "random_password" "origin_verify" {
  length  = 48
  special = false
}
