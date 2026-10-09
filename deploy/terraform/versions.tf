terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }

  # State holds generated passwords. Keep it in an encrypted, versioned S3
  # bucket with locking, never in git:
  #
  # backend "s3" {
  #   bucket       = "<state-bucket>"
  #   key          = "orderflow/pricefeed.tfstate"
  #   region       = "us-east-1"
  #   encrypt      = true
  #   use_lockfile = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "orderflow"
      Service     = "pricefeed"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
