variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used in resource names."
  type        = string
  default     = "staging"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.environment))
    error_message = "Use 2 to 21 lowercase letters, digits or dashes."
  }
}

variable "availability_zones" {
  description = "Two or more zones: RDS, ElastiCache and ECS spread across them."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]

  validation {
    condition     = length(var.availability_zones) >= 2
    error_message = "Multi-AZ failover needs at least two zones."
  }
}

variable "vpc_cidr" {
  description = "VPC address range; subnets are carved out of it."
  type        = string
  default     = "10.40.0.0/16"
}

variable "image_tag" {
  description = "Tag of the pricefeed image in ECR (immutable tags: use the git sha)."
  type        = string
}

variable "certificate_arn" {
  description = "ACM certificate for the load balancer's HTTPS listener."
  type        = string
}

variable "origin_domain_name" {
  description = "DNS name that points at the load balancer and is covered by certificate_arn. CloudFront connects to it."
  type        = string
}

variable "db_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.medium"
}

variable "db_allocated_storage_gb" {
  description = "Initial gp3 storage; autoscaling grows it up to db_max_storage_gb."
  type        = number
  default     = 50
}

variable "db_max_storage_gb" {
  type    = number
  default = 500
}

variable "postgres_version" {
  description = "Major version, matching the postgres image used locally."
  type        = string
  default     = "18"
}

variable "cache_node_type" {
  description = "ElastiCache node type for Valkey."
  type        = string
  default     = "cache.t4g.small"
}

variable "edge_min_tasks" {
  description = "Edge tasks never scale below this; two keeps one per zone."
  type        = number
  default     = 2
}

variable "edge_max_tasks" {
  type    = number
  default = 20
}

variable "edge_cpu_target" {
  description = "Average CPU percentage the edge service scales to hold."
  type        = number
  default     = 50
}

variable "evm_rpc_url" {
  description = "Ethereum RPC provider URL. Usually holds an API key; leave null to use the public endpoint."
  type        = string
  default     = null
  sensitive   = true
}

variable "solana_rpc_url" {
  description = "Solana RPC provider URL. Leave null to use the public endpoint."
  type        = string
  default     = null
  sensitive   = true
}

variable "coingecko_api_key" {
  description = "Optional CoinGecko API key."
  type        = string
  default     = null
  sensitive   = true
}

variable "log_retention_days" {
  type    = number
  default = 30
}
