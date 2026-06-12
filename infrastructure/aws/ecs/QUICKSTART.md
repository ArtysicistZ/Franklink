# ECS Quick Start (Git-Based Deployment)

## One-Time Setup

### 1) Set GitHub Secrets

Go to GitHub: Settings -> Secrets and variables -> Actions.

```
TESTING_ECS_CLUSTER=franklink-testing-cluster
TESTING_ECS_SERVICE=frank-career-counselor-testing-ecs
TESTING_ECS_TASK_FAMILY=frank-career-counselor-testing-ecs
TESTING_ECS_TASK_EXECUTION_ROLE_ARN=arn:aws:iam::<account>:role/franklink-ecs-execution-role-testing
TESTING_ECS_TASK_ROLE_ARN=arn:aws:iam::<account>:role/franklink-ecs-task-role-testing
TESTING_ECS_TASK_CPU=512
TESTING_ECS_TASK_MEMORY=1024
TESTING_ECS_SUBNETS_CSV=subnet-aaa,subnet-bbb,subnet-ccc
TESTING_ECS_SECURITY_GROUPS_CSV=sg-xxx
TESTING_ECS_LOG_GROUP=/ecs/frank-career-counselor-testing-ecs
```

Optional:
- `TESTING_ECS_ENV_FILE_CONTENT` (full `.env` contents), if `.env` is not in the repo checkout.

### 2) Run the setup script (terminal)

```bash
cd "$(git rev-parse --show-toplevel)"
./infrastructure/aws/scripts/setup-ecs-from-scratch.sh testing
```

### 3) Test initial deployment

```bash
./infrastructure/aws/scripts/get-task-ip.sh testing
curl http://PUBLIC_IP:8000/health
```

## Daily Workflow

```bash
# Commit and push
git add .
git commit -m "Update Kafka ECS deployment"
git push

# Trigger deployment
gh workflow run deploy-testing-ecs.yml -f create_service_if_missing=false -f worker_poll_seconds=5
```

## Common Commands

### View logs

```bash
aws logs tail /ecs/frank-career-counselor-testing-ecs --follow --region us-east-2
```

### Scale worker service

```bash
aws ecs update-service \
  --cluster franklink-testing-cluster \
  --service frank-career-counselor-testing-ecs \
  --desired-count 5 \
  --region us-east-2
```

### Describe services

```bash
aws ecs describe-services \
  --cluster franklink-testing-cluster \
  --services frank-career-counselor-testing-ecs \
  frank-career-counselor-testing-ecs-ingest \
  frank-career-counselor-testing-ecs-background \
  --region us-east-2
```

## Architecture (High Level)

- Worker service: API + Kafka consumer
- Ingest service: Photon listener
- Background service: summary/followup/proactive workers

## Notes

- Ensure ECS subnets/SGs can reach MSK brokers.
- Set Kafka settings in `.env` (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*` if needed).

**Last Updated**: February 1, 2026
