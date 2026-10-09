# Two services from one image. Ingest runs two tasks: one holds the
# Postgres lease and ingests, the other waits to take over. Edge tasks are
# stateless and scale on CPU behind the load balancer.

resource "aws_ecs_cluster" "main" {
  name = local.name

  setting {
    name  = "containerInsights"
    value = "enhanced"
  }
}

resource "aws_cloudwatch_log_group" "pricefeed" {
  name              = "/ecs/${local.name}"
  retention_in_days = var.log_retention_days
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Used by ECS itself to pull the image, write logs and read the secrets.
resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "read_secrets" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = concat(
      [aws_secretsmanager_secret.database_url.arn, aws_secretsmanager_secret.redis_url.arn],
      [for secret in aws_secretsmanager_secret.optional : secret.arn],
    )
  }
}

resource "aws_iam_role_policy" "execution_secrets" {
  name   = "read-pricefeed-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.read_secrets.json
}

# Assumed by the application. It calls no AWS API, so it has no policy.
resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

locals {
  image = "${aws_ecr_repository.pricefeed.repository_url}:${var.image_tag}"

  secrets = concat(
    [
      { name = "PRICEFEED_DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn },
      { name = "PRICEFEED_REDIS_URL", valueFrom = aws_secretsmanager_secret.redis_url.arn },
    ],
    [for name, secret in aws_secretsmanager_secret.optional : { name = name, valueFrom = secret.arn }],
  )

  container = {
    for role in ["ingest", "edge"] : role => {
      name                   = "pricefeed"
      image                  = local.image
      essential              = true
      readonlyRootFilesystem = true
      user                   = "nonroot"
      # Time for the shutdown sequence: close WebSockets with 1001, finish
      # requests, flush price history.
      stopTimeout  = 30
      portMappings = [{ containerPort = 8090, protocol = "tcp" }]
      environment = [
        { name = "PRICEFEED_ROLE", value = role },
        { name = "PRICEFEED_LOG_FORMAT", value = "json" },
        { name = "RUST_LOG", value = "info" },
      ]
      secrets = local.secrets
      # Every WebSocket is a file descriptor.
      ulimits = [{ name = "nofile", softLimit = 1048576, hardLimit = 1048576 }]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.pricefeed.name
          awslogs-region        = var.region
          awslogs-stream-prefix = role
        }
      }
    }
  }
}

resource "aws_ecs_task_definition" "ingest" {
  family                   = "${local.name}-ingest"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn
  container_definitions    = jsonencode([local.container["ingest"]])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}

resource "aws_ecs_task_definition" "edge" {
  family                   = "${local.name}-edge"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn
  container_definitions    = jsonencode([local.container["edge"]])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}

resource "aws_ecs_service" "ingest" {
  name            = "ingest"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.ingest.arn
  launch_type     = "FARGATE"
  desired_count   = 2

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.tasks.id]
  }

  # New tasks start before old ones stop. Only one of them holds the lease
  # at a time, so a deploy never runs two ingesting leaders.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }
}

resource "aws_ecs_service" "edge" {
  name                              = "edge"
  cluster                           = aws_ecs_cluster.main.id
  task_definition                   = aws_ecs_task_definition.edge.arn
  launch_type                       = "FARGATE"
  desired_count                     = var.edge_min_tasks
  health_check_grace_period_seconds = 30

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.tasks.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.edge.arn
    container_name   = "pricefeed"
    container_port   = 8090
  }

  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  # Autoscaling owns the task count after creation.
  lifecycle {
    ignore_changes = [desired_count]
  }
}

resource "aws_appautoscaling_target" "edge" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.edge.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.edge_min_tasks
  max_capacity       = var.edge_max_tasks
}

# WebSocket fanout is CPU bound (one write per subscriber per update), so
# CPU is the scaling signal. Scale-in is slow on purpose: every removed
# task disconnects its clients, who then reconnect elsewhere.
resource "aws_appautoscaling_policy" "edge_cpu" {
  name               = "${local.name}-edge-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.edge.service_namespace
  resource_id        = aws_appautoscaling_target.edge.resource_id
  scalable_dimension = aws_appautoscaling_target.edge.scalable_dimension

  target_tracking_scaling_policy_configuration {
    target_value       = var.edge_cpu_target
    scale_out_cooldown = 60
    scale_in_cooldown  = 600

    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}
