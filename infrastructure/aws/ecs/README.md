# AWS ECS Setup Guide

This guide covers the ECS testing deployment for the Kafka/MSK architecture.

## Prerequisites

- AWS CLI configured (`aws configure`)
- Docker installed and running
- Git installed
- Python 3 installed (task definition rendering)
- IAM permissions to create ECS, ECR, S3, CloudWatch Logs, IAM roles, and basic EC2 describe calls

## Quick Start (One Command)

```bash
# Run complete setup from repo root
./infrastructure/aws/scripts/setup-ecs-from-scratch.sh testing
```

This script will:
1. Create or reuse VPC networking and security group
2. Create S3 bucket for environment files and upload `.env`
3. Create IAM roles (execution + task roles)
4. Create CloudWatch log group
5. Create ECS cluster
6. Build and push Docker image to ECR
7. Register 3 task definitions (worker, ingest, background)
8. Create 3 ECS services
9. Wait for services to become stable

## What Gets Created

### ECS Services

- **Worker service** (`frank-career-counselor-<env>-ecs`)
  - API + Kafka consumer
  - Runs `uvicorn` and the Kafka consumer loop
- **Ingest service** (`frank-career-counselor-<env>-ecs-ingest`)
  - Photon listener only (ingest -> Kafka)
- **Background service** (`frank-career-counselor-<env>-ecs-background`)
  - Groupchat summary/followup + proactive workers

### Task Definitions

- `taskdef-frank-worker.json`
- `taskdef-photon-ingest.json`
- `taskdef-background-workers.json`

### Resource Allocation (per task)

- CPU: 512 (0.5 vCPU)
- Memory: 1024 MB (1 GB)
- Launch type: Fargate
- Networking: awsvpc

## Deployment Workflow

### Initial setup

```bash
./infrastructure/aws/scripts/setup-ecs-from-scratch.sh testing
```

### Subsequent deployments

Use GitHub Actions (recommended):

```bash
gh workflow run deploy-testing-ecs.yml \
  -f create_service_if_missing=false \
  -f worker_poll_seconds=5
```

Or use the emergency script:

```bash
./infrastructure/aws/scripts/deploy-ecs.sh testing
```

## .env handling

ECS reads environment variables from S3 via `environmentFiles`:

- S3 bucket: `franklink-<env>-config-<account-id>`
- Key: `.env`

The GitHub Actions workflow uploads `.env` on each deploy.

## Kafka/MSK Notes

- Ensure ECS subnets/security groups can reach MSK brokers.
- If MSK uses TLS/SASL, set these in `.env`:
  - `KAFKA_SECURITY_PROTOCOL` (e.g. `SSL` or `SASL_SSL`)
  - `KAFKA_SASL_MECHANISM` (e.g. `SCRAM-SHA-512`)
  - `KAFKA_USERNAME` / `KAFKA_PASSWORD`
  - `KAFKA_BOOTSTRAP_SERVERS`

## Monitoring & Debugging

### Service status

```bash
aws ecs describe-services \
  --cluster franklink-testing-cluster \
  --services frank-career-counselor-testing-ecs \
  frank-career-counselor-testing-ecs-ingest \
  frank-career-counselor-testing-ecs-background \
  --region us-east-2
```

### Logs

```bash
aws logs tail /ecs/frank-career-counselor-testing-ecs --follow --region us-east-2
```

### Get task IP (worker service)

```bash
./infrastructure/aws/scripts/get-task-ip.sh testing
```

## Cleanup (Delete Resources)

Delete services first, then cluster/logs/S3/roles.

```bash
aws ecs update-service --cluster franklink-testing-cluster --service frank-career-counselor-testing-ecs --desired-count 0 --region us-east-2
aws ecs update-service --cluster franklink-testing-cluster --service frank-career-counselor-testing-ecs-ingest --desired-count 0 --region us-east-2
aws ecs update-service --cluster franklink-testing-cluster --service frank-career-counselor-testing-ecs-background --desired-count 0 --region us-east-2
```

## Support

- ECS docs: https://docs.aws.amazon.com/ecs/
- Task definitions: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definitions.html

**Last Updated**: February 1, 2026
