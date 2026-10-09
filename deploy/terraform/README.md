# Price feed on AWS

Terraform for the production shape of the price feed (ADR 0008). It is
validated in CI (`make tf-validate`, tflint) and has never been applied:
treat it as a reviewed design that has not been deployed yet.

```
CloudFront --> ALB (HTTPS, only with CloudFront's secret header) --> ECS edge tasks (autoscaled)
                                                                       |
ECS ingest tasks (leader + standby) --> RDS PostgreSQL (Multi-AZ) <----+
            +-----------------------> ElastiCache Valkey (TLS, auth) <-+
```

| File | Contents |
|---|---|
| `network.tf` | VPC, public and private subnets in two zones, NAT gateway |
| `security.tf` | One security group per tier, each open only to the tier in front of it |
| `data.tf` | RDS PostgreSQL 18 Multi-AZ (gp3, encrypted, `pg_stat_statements`, forced TLS) and a Valkey replication group with failover |
| `secrets.tf` | Database and Redis URLs, optional RPC and CoinGecko keys, in Secrets Manager |
| `ecs.tf` | Fargate cluster, ingest and edge services, IAM roles, CPU autoscaling, 1M file descriptors per task |
| `alb.tf` | HTTPS listener that refuses traffic without CloudFront's header; target group on `/health/ready` |
| `cdn.tf` | CloudFront: the REST API cached per `Cache-Control`, `/v1/ws` passed through, `/metrics` hidden |
| `ecr.tf` | Image repository with immutable tags and scan on push |

## Validate

```bash
make tf-validate
```

## Before a real apply

1. Configure the S3 backend in `versions.tf`. State holds the generated
   database and cache passwords, so it must be encrypted and private.
2. Copy `terraform.tfvars.example` to `terraform.tfvars` (git-ignored) and
   pass provider keys through the environment, for example
   `TF_VAR_evm_rpc_url`.
3. Push the image: `docker build --target pricefeed .`, tagged with the
   git sha, to the `ecr_repository_url` output.
4. Point `origin_domain_name` at the load balancer and issue its
   certificate in ACM.
5. Harden the database connection to `sslmode=verify-full` with the RDS
   CA bundle in the image.
6. Scrape `/metrics` from inside the VPC, for example with Amazon Managed
   Service for Prometheus and an ADOT collector, and load the alert rules
   from `deploy/prometheus/alerts.yml`.

Cost drivers in a small setup: the NAT gateway, RDS Multi-AZ and the
ElastiCache replica. Staging can drop Multi-AZ and the replica; production
should also run one NAT gateway per zone.
