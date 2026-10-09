output "cdn_domain_name" {
  description = "Public entry point: https://<this>/v1/prices and wss://<this>/v1/ws."
  value       = aws_cloudfront_distribution.main.domain_name
}

output "load_balancer_dns_name" {
  description = "Point origin_domain_name's DNS record here."
  value       = aws_lb.main.dns_name
}

output "ecr_repository_url" {
  description = "Push the pricefeed image here (docker build --target pricefeed)."
  value       = aws_ecr_repository.pricefeed.repository_url
}

output "database_endpoint" {
  value = aws_db_instance.main.address
}

output "cache_endpoint" {
  value = aws_elasticache_replication_group.main.primary_endpoint_address
}
