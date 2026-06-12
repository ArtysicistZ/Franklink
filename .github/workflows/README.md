# GitHub Actions CI/CD for Franklink

This directory contains the ECS testing deployment workflow.

## Workflow: deploy-testing-ecs.yml

Manual workflow that builds/pushes the Docker image and deploys 3 ECS services:

- Worker service: `${TESTING_ECS_SERVICE}` (API + Kafka consumer)
- Ingest service: `${TESTING_ECS_SERVICE}-ingest` (Photon listener)
- Background service: `${TESTING_ECS_SERVICE}-background` (summary/followup/proactive workers)

### Required GitHub Secrets (ECS)

In addition to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, configure:

- `TESTING_ECS_CLUSTER`
- `TESTING_ECS_SERVICE`
- `TESTING_ECS_TASK_FAMILY`
- `TESTING_ECS_TASK_EXECUTION_ROLE_ARN`
- `TESTING_ECS_TASK_ROLE_ARN`
- `TESTING_ECS_TASK_CPU` (e.g. `512`)
- `TESTING_ECS_TASK_MEMORY` (e.g. `1024`)
- `TESTING_ECS_SUBNETS_CSV` (comma-separated subnet IDs)
- `TESTING_ECS_SECURITY_GROUPS_CSV` (comma-separated security group IDs)
- `TESTING_ECS_LOG_GROUP` (CloudWatch Logs group name)

Optional:
- `TESTING_ECS_ENV_FILE_CONTENT` (full `.env` contents) if you do not keep `.env` in the repo checkout

### .env handling

The workflow uploads `.env` to S3 for ECS `environmentFiles` usage:

- S3 bucket: `franklink-testing-config-${account_id}` (created ahead of time)
- Key: `.env`

If `.env` is missing in the repo checkout, set `TESTING_ECS_ENV_FILE_CONTENT` and it will be written and uploaded on each deploy.

### IAM permissions for the GitHub Actions user

Your IAM user should have permissions for:

- ECR: `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:GetDownloadUrlForLayer`, `ecr:BatchGetImage`, `ecr:PutImage`, `ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`
- ECS: `ecs:RegisterTaskDefinition`, `ecs:DescribeServices`, `ecs:UpdateService`, `ecs:CreateService`, `ecs:ListTasks`, `ecs:DescribeTasks`, `ecs:WaitForServicesStable`
- IAM: `iam:PassRole` for the task execution role and task role
- Logs: `logs:CreateLogGroup`, `logs:PutRetentionPolicy`
- S3: `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`
- EC2: `ec2:DescribeNetworkInterfaces`

### Running the workflow

```
gh workflow run deploy-testing-ecs.yml \
  -f create_service_if_missing=true \
  -f worker_poll_seconds=5
```

### Kafka/MSK notes

- Make sure `TESTING_ECS_SUBNETS_CSV` and `TESTING_ECS_SECURITY_GROUPS_CSV` point to subnets/SGs that can reach MSK.
- If MSK uses TLS/SASL, set these in `.env`:
  - `KAFKA_SECURITY_PROTOCOL` (e.g. `SSL` or `SASL_SSL`)
  - `KAFKA_SASL_MECHANISM` (e.g. `SCRAM-SHA-512`)
  - `KAFKA_USERNAME` / `KAFKA_PASSWORD`
  - `KAFKA_BOOTSTRAP_SERVERS`

**Last Updated**: February 1, 2026
