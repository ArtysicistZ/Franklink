#!/bin/bash

################################################################################
# ECS Deployment Script - Git-Based Deployment Helper
#
# IMPORTANT: This script is DEPRECATED in favor of GitHub Actions.
# For git-based deployment, use:
#   git commit -m "message" && git push
#   gh workflow run deploy-testing-ecs.yml -f create_service_if_missing=false
#
# This script is kept for emergency manual deployments only.
# Use this after initial setup with setup-ecs-from-scratch.sh
#
# Usage: ./deploy-ecs.sh [environment]
# Example: ./deploy-ecs.sh testing
################################################################################

set -euo pipefail

# Resolve AWS CLI reliably across macOS/Linux/Windows (Git Bash).
# IMPORTANT: some Windows setups (e.g. Conda) put a broken `aws` shim earlier on PATH.
AWS_BIN=""
AWS_CANDIDATES=(
  "/c/Program Files/Amazon/AWSCLIV2/aws.exe"
  "/c/Program Files/Amazon/AWSCLIV2/aws"
  "$(command -v aws.exe 2>/dev/null || true)"
  "$(command -v aws 2>/dev/null || true)"
)
for CAND in "${AWS_CANDIDATES[@]}"; do
  if [ -z "${CAND}" ]; then
    continue
  fi
  if "${CAND}" --version >/dev/null 2>&1; then
    AWS_BIN="${CAND}"
    break
  fi
done
if [ -z "${AWS_BIN}" ]; then
  echo "ERROR: AWS CLI not found (or not runnable). Ensure AWS CLI v2 is installed and 'aws --version' works in this shell."
  exit 1
fi

aws() { "${AWS_BIN}" "$@"; }

# Avoid paging and normalize CRLF output when invoking Windows-native `aws.exe` from bash.
export AWS_PAGER=""
strip_cr() { tr -d '\r'; }

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ ${NC}$1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }

# Normalize log output (ASCII) to avoid Windows encoding issues.
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

################################################################################
# CONFIGURATION
################################################################################

ENVIRONMENT="${1:-testing}"
AWS_REGION="us-east-2"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text | strip_cr)

ECS_CLUSTER="franklink-${ENVIRONMENT}-cluster"
ECS_SERVICE="frank-career-counselor-${ENVIRONMENT}-ecs"
ECS_TASK_FAMILY="frank-career-counselor-${ENVIRONMENT}-ecs"
ECS_SERVICE_INGEST="${ECS_SERVICE}-ingest"
ECS_SERVICE_BACKGROUND="${ECS_SERVICE}-background"
ECS_TASK_FAMILY_INGEST="${ECS_TASK_FAMILY}-ingest"
ECS_TASK_FAMILY_BACKGROUND="${ECS_TASK_FAMILY}-background"
ECR_REPOSITORY="frank-ai"
LOG_GROUP="/ecs/frank-career-counselor-${ENVIRONMENT}-ecs"
S3_BUCKET="franklink-${ENVIRONMENT}-config-${AWS_ACCOUNT_ID}"
ENV_FILE_KEY=".env"

TASK_CPU="512"
TASK_MEMORY="1024"
WORKER_DESIRED_COUNT="5"
INGEST_DESIRED_COUNT="1"
BACKGROUND_DESIRED_COUNT="1"

EXECUTION_ROLE_NAME="franklink-ecs-execution-role-${ENVIRONMENT}"
TASK_ROLE_NAME="franklink-ecs-task-role-${ENVIRONMENT}"

log_info "Deploying to ECS environment: ${ENVIRONMENT}"
echo ""

################################################################################
# STEP 1: Upload .env file to S3 (if changed)
################################################################################

if [ -f ".env" ]; then
  log_info "[1/5] Uploading .env file to S3..."
  aws s3 cp .env "s3://${S3_BUCKET}/${ENV_FILE_KEY}" --region ${AWS_REGION}
  log_success "Uploaded .env to S3"
else
  log_warning "[1/5] .env file not found, skipping upload"
fi

ENV_FILE_ARN="arn:aws:s3:::${S3_BUCKET}/${ENV_FILE_KEY}"

echo ""

################################################################################
# STEP 2: Build and Push Docker Image
################################################################################

log_info "[2/5] Building and pushing Docker image..."

# Login to ECR
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Build image
IMAGE_TAG="${ENVIRONMENT}-$(date +%Y%m%d-%H%M%S)-$(git rev-parse --short HEAD 2>/dev/null || echo 'local')"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"
IMAGE_URI_LATEST="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${ENVIRONMENT}"

log_info "Building image: ${IMAGE_TAG}"
docker build --platform linux/amd64 -t "${IMAGE_URI}" -t "${IMAGE_URI_LATEST}" .

log_info "Pushing to ECR..."
docker push "${IMAGE_URI}"
docker push "${IMAGE_URI_LATEST}"

log_success "Pushed: ${IMAGE_URI}"

echo ""

################################################################################
# STEP 3: Get IAM Role ARNs
################################################################################

log_info "[3/5] Retrieving IAM role ARNs..."

EXECUTION_ROLE_ARN=$(aws iam get-role --role-name "${EXECUTION_ROLE_NAME}" --query 'Role.Arn' --output text | strip_cr)
TASK_ROLE_ARN=$(aws iam get-role --role-name "${TASK_ROLE_NAME}" --query 'Role.Arn' --output text | strip_cr)

log_success "Execution Role: ${EXECUTION_ROLE_ARN}"
log_success "Task Role: ${TASK_ROLE_ARN}"

echo ""

################################################################################
# STEP 4: Register New Task Definitions
################################################################################

log_info "[4/5] Registering new task definitions..."

# Render task definitions
python3 << PY
import os
from pathlib import Path

env_data = {}
env_path = Path(".env")
if env_path.exists():
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env_data[k.strip()] = v.strip()

def required_env(key: str) -> str:
    val = str(env_data.get(key) or "").strip()
    if not val:
        raise SystemExit(f"Missing required key in .env for taskdef render: {key}")
    return val

base = {
    "__AWS_REGION__": "${AWS_REGION}",
    "__ECS_TASK_CPU__": "${TASK_CPU}",
    "__ECS_TASK_MEMORY__": "${TASK_MEMORY}",
    "__ECS_TASK_EXECUTION_ROLE_ARN__": "${EXECUTION_ROLE_ARN}",
    "__ECS_TASK_ROLE_ARN__": "${TASK_ROLE_ARN}",
    "__ECS_LOG_GROUP__": "${LOG_GROUP}",
    "__ECS_ENV_FILE_ARN__": "${ENV_FILE_ARN}",
    "__IMAGE_URI__": "${IMAGE_URI}",
}
worker_repl = {
    **base,
    "__ECS_TASK_FAMILY__": "${ECS_TASK_FAMILY}",
    "__COMPOSIO_API_KEY__": required_env("COMPOSIO_API_KEY"),
    "__COMPOSIO_AUTH_CONFIG_ID__": required_env("COMPOSIO_AUTH_CONFIG_ID"),
    "__LOGIN_PAGE_URL__": env_data.get("LOGIN_PAGE_URL", ""),
}
ingest_repl = {
    **base,
    "__ECS_TASK_FAMILY__": "${ECS_TASK_FAMILY_INGEST}",
}
background_repl = {
    **base,
    "__ECS_TASK_FAMILY__": "${ECS_TASK_FAMILY_BACKGROUND}",
    "__GROUPCHAT_WORKER_POLL_SECONDS__": "5",
}
optional_keys = {"__LOGIN_PAGE_URL__"}

def render(src_path: str, out_path: str, repl: dict) -> None:
    src = Path(src_path).read_text(encoding="utf-8")
    for k, v in repl.items():
        if not v and k not in optional_keys:
            raise SystemExit(f"Missing required env for template: {k}")
        src = src.replace(k, v)
    Path(out_path).write_text(src, encoding="utf-8")

render("infrastructure/aws/ecs/taskdef-frank-worker.json", "taskdef.worker.json", worker_repl)
render("infrastructure/aws/ecs/taskdef-photon-ingest.json", "taskdef.ingest.json", ingest_repl)
render("infrastructure/aws/ecs/taskdef-background-workers.json", "taskdef.background.json", background_repl)
PY

WORKER_TASK_DEF_ARN=$(aws ecs register-task-definition \
  --cli-input-json file://taskdef.worker.json \
  --region ${AWS_REGION} \
  --query 'taskDefinition.taskDefinitionArn' \
  --output text | strip_cr)
INGEST_TASK_DEF_ARN=$(aws ecs register-task-definition \
  --cli-input-json file://taskdef.ingest.json \
  --region ${AWS_REGION} \
  --query 'taskDefinition.taskDefinitionArn' \
  --output text | strip_cr)
BACKGROUND_TASK_DEF_ARN=$(aws ecs register-task-definition \
  --cli-input-json file://taskdef.background.json \
  --region ${AWS_REGION} \
  --query 'taskDefinition.taskDefinitionArn' \
  --output text | strip_cr)

log_success "Registered worker: ${WORKER_TASK_DEF_ARN}"
log_success "Registered ingest: ${INGEST_TASK_DEF_ARN}"
log_success "Registered background: ${BACKGROUND_TASK_DEF_ARN}"

echo ""

################################################################################
# STEP 5: Update ECS Services
################################################################################

log_info "[5/5] Updating ECS services..."

update_service() {
  local service_name="$1"
  local task_def_arn="$2"
  local desired_count="$3"
  aws ecs update-service \
    --cluster "${ECS_CLUSTER}" \
    --service "${service_name}" \
    --task-definition "${task_def_arn}" \
    --desired-count "${desired_count}" \
    --force-new-deployment \
    --region ${AWS_REGION} >/dev/null
  log_success "Service update initiated: ${service_name}"
}

update_service "${ECS_SERVICE_INGEST}" "${INGEST_TASK_DEF_ARN}" "${INGEST_DESIRED_COUNT}"
update_service "${ECS_SERVICE}" "${WORKER_TASK_DEF_ARN}" "${WORKER_DESIRED_COUNT}"
update_service "${ECS_SERVICE_BACKGROUND}" "${BACKGROUND_TASK_DEF_ARN}" "${BACKGROUND_DESIRED_COUNT}"

log_info "Waiting for services to stabilize (2-3 minutes)..."

aws ecs wait services-stable \
  --cluster "${ECS_CLUSTER}" \
  --services "${ECS_SERVICE_INGEST}" \
  --region ${AWS_REGION}
aws ecs wait services-stable \
  --cluster "${ECS_CLUSTER}" \
  --services "${ECS_SERVICE}" \
  --region ${AWS_REGION}
aws ecs wait services-stable \
  --cluster "${ECS_CLUSTER}" \
  --services "${ECS_SERVICE_BACKGROUND}" \
  --region ${AWS_REGION}

log_success "Services are stable!"

echo ""

################################################################################
# Get Task Info
################################################################################

log_info "Fetching running task information for worker service..."

TASK_ARNS=$(aws ecs list-tasks \
  --cluster "${ECS_CLUSTER}" \
  --service-name "${ECS_SERVICE}" \
  --desired-status RUNNING \
  --region ${AWS_REGION} \
  --query 'taskArns' \
  --output text | strip_cr)

if [ -n "$TASK_ARNS" ]; then
  for TASK_ARN in $TASK_ARNS; do
    ENI_ID=$(aws ecs describe-tasks \
      --cluster "${ECS_CLUSTER}" \
      --tasks "${TASK_ARN}" \
      --region ${AWS_REGION} \
      --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' \
      --output text | strip_cr)

    if [ -n "$ENI_ID" ]; then
      PUBLIC_IP=$(aws ec2 describe-network-interfaces \
        --network-interface-ids "${ENI_ID}" \
        --region ${AWS_REGION} \
        --query 'NetworkInterfaces[0].Association.PublicIp' \
        --output text 2>/dev/null | strip_cr || echo "N/A")

      log_success "Task: ${TASK_ARN##*/}"
      log_success "  Public IP: ${PUBLIC_IP}"
      log_success "  API: http://${PUBLIC_IP}:8000"
      log_success "  Health: http://${PUBLIC_IP}:8000/health"
    fi
  done
fi

echo ""

################################################################################
# Summary
################################################################################

log_success "============================================================"
log_success "Deployment Complete!"
log_success "============================================================"
echo ""
echo "Version:            ${IMAGE_TAG}"
echo "Worker Task Def:    ${WORKER_TASK_DEF_ARN}"
echo "Ingest Task Def:    ${INGEST_TASK_DEF_ARN}"
echo "Background Task Def:${BACKGROUND_TASK_DEF_ARN}"
echo "Services:           ${ECS_SERVICE}, ${ECS_SERVICE_INGEST}, ${ECS_SERVICE_BACKGROUND}"
echo ""
echo "View logs:"
echo "  aws logs tail ${LOG_GROUP} --follow --region ${AWS_REGION}"
echo ""
echo "View service:"
echo "  aws ecs describe-services --cluster ${ECS_CLUSTER} --services ${ECS_SERVICE} ${ECS_SERVICE_INGEST} ${ECS_SERVICE_BACKGROUND} --region ${AWS_REGION}"
echo ""
log_success "============================================================"
