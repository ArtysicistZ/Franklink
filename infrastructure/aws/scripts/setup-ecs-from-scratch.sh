#!/bin/bash

################################################################################
# ECS Complete Setup Script - Creates ECS Infrastructure from Scratch
#
# This script creates:
# 1. ECS Cluster
# 2. CloudWatch Log Group
# 3. IAM Roles (Task Execution + Task Role)
# 4. S3 Bucket for environment files
# 5. VPC, Subnets, Security Groups (if needed)
# 6. Task Definitions
# 7. ECS Services (Fargate)
#
# Prerequisites:
# - AWS CLI installed and configured
# - Docker installed
# - jq installed (for JSON processing)
#
# Usage: ./setup-ecs-from-scratch.sh [environment]
# Example: ./setup-ecs-from-scratch.sh testing
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

# Make all `aws ...` calls use the resolved binary.
aws() { "${AWS_BIN}" "$@"; }

# Avoid paging and normalize CRLF output when invoking Windows-native `aws.exe` from bash.
export AWS_PAGER=""
strip_cr() { tr -d '\r'; }

# Resolve Python (used for taskdef rendering).
PY_BIN="$(command -v python3 2>/dev/null || true)"
if [ -z "${PY_BIN}" ]; then
  PY_BIN="$(command -v python 2>/dev/null || true)"
fi
if [ -z "${PY_BIN}" ]; then
  echo "ERROR: Python not found. Install Python 3 and ensure 'python3' or 'python' is on PATH."
  exit 1
fi

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Resource names
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

# Task configuration
TASK_CPU="512"      # 0.5 vCPU
TASK_MEMORY="1024"  # 1 GB
WORKER_DESIRED_COUNT="5"
INGEST_DESIRED_COUNT="1"
BACKGROUND_DESIRED_COUNT="1"

# IAM role names
EXECUTION_ROLE_NAME="franklink-ecs-execution-role-${ENVIRONMENT}"
TASK_ROLE_NAME="franklink-ecs-task-role-${ENVIRONMENT}"

log_info "Setting up ECS infrastructure for environment: ${ENVIRONMENT}"
log_info "AWS Account: ${AWS_ACCOUNT_ID}"
log_info "AWS Region: ${AWS_REGION}"
echo ""

################################################################################
# STEP 1: Create VPC and Networking (if using default VPC, skip this)
################################################################################

log_info "[1/9] Checking VPC and networking..."

# Use default VPC for simplicity (or create custom VPC here)
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=isDefault,Values=true" \
  --query 'Vpcs[0].VpcId' \
  --output text \
  --region ${AWS_REGION} | strip_cr)

if [ "$VPC_ID" == "None" ] || [ -z "$VPC_ID" ]; then
  log_error "No default VPC found. Please create a VPC first."
  exit 1
fi

log_success "Using VPC: ${VPC_ID}"

# Get subnets
SUBNETS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${VPC_ID}" \
  --query 'Subnets[*].SubnetId' \
  --output text \
  --region ${AWS_REGION} | strip_cr)

SUBNET_ARRAY=($SUBNETS)
SUBNET1=${SUBNET_ARRAY[0]}
SUBNET2=${SUBNET_ARRAY[1]:-$SUBNET1}

log_success "Using subnets: ${SUBNET1}, ${SUBNET2}"

# Create Security Group
SG_NAME="franklink-${ENVIRONMENT}-sg"
EXISTING_SG=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
  --query 'SecurityGroups[0].GroupId' \
  --output text \
  --region ${AWS_REGION} 2>/dev/null | strip_cr || true)
if [ -z "$EXISTING_SG" ] || [ "${EXISTING_SG}" == "None" ]; then
  log_info "Creating security group: ${SG_NAME}"

  SECURITY_GROUP_ID=$(aws ec2 create-security-group \
    --group-name "${SG_NAME}" \
    --description "Security group for Franklink ECS ${ENVIRONMENT}" \
    --vpc-id "${VPC_ID}" \
    --query 'GroupId' \
    --output text \
    --region ${AWS_REGION} | strip_cr)

  # Allow outbound traffic (for API calls, webhooks, etc.)
  aws ec2 authorize-security-group-egress \
    --group-id "${SECURITY_GROUP_ID}" \
    --ip-permissions IpProtocol=-1,IpRanges='[{CidrIp=0.0.0.0/0}]' \
    --region ${AWS_REGION} 2>/dev/null || true

  # Allow inbound traffic on port 8000 from anywhere (for ALB or direct access)
  aws ec2 authorize-security-group-ingress \
    --group-id "${SECURITY_GROUP_ID}" \
    --protocol tcp \
    --port 8000 \
    --cidr 0.0.0.0/0 \
    --region ${AWS_REGION}

  log_success "Created security group: ${SECURITY_GROUP_ID}"
else
  SECURITY_GROUP_ID=$EXISTING_SG
  log_success "Using existing security group: ${SECURITY_GROUP_ID}"
fi

echo ""

################################################################################
# STEP 2: Create S3 Bucket for Environment Files
################################################################################

log_info "[2/9] Setting up S3 bucket for environment files..."

if aws s3 ls "s3://${S3_BUCKET}" 2>/dev/null; then
  log_success "S3 bucket already exists: ${S3_BUCKET}"
else
  log_info "Creating S3 bucket: ${S3_BUCKET}"

  # NOTE: `aws s3 mb` does not support `--create-bucket-configuration`; use s3api.
  if [ "${AWS_REGION}" == "us-east-1" ]; then
    aws s3api create-bucket --bucket "${S3_BUCKET}" --region ${AWS_REGION} >/dev/null
  else
    aws s3api create-bucket \
      --bucket "${S3_BUCKET}" \
      --region ${AWS_REGION} \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
  fi

  # Enable encryption
  aws s3api put-bucket-encryption \
    --bucket "${S3_BUCKET}" \
    --region ${AWS_REGION} \
    --server-side-encryption-configuration '{
      "Rules": [{
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }]
    }'

  # Block public access
  aws s3api put-public-access-block \
    --bucket "${S3_BUCKET}" \
    --region ${AWS_REGION} \
    --public-access-block-configuration \
      "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

  log_success "Created S3 bucket: ${S3_BUCKET}"
fi

# Upload .env file
if [ -f ".env" ]; then
  log_info "Uploading .env file to S3..."
  aws s3 cp .env "s3://${S3_BUCKET}/${ENV_FILE_KEY}" --region ${AWS_REGION}
  log_success "Uploaded .env to s3://${S3_BUCKET}/${ENV_FILE_KEY}"
else
  log_warning ".env file not found in current directory. You'll need to upload it manually."
fi

ENV_FILE_ARN="arn:aws:s3:::${S3_BUCKET}/${ENV_FILE_KEY}"
log_success "Environment file ARN: ${ENV_FILE_ARN}"

echo ""

################################################################################
# STEP 3: Create IAM Roles
################################################################################

log_info "[3/9] Creating IAM roles..."

# Task Execution Role (used by ECS to pull images, push logs)
EXECUTION_ROLE_ARN=$(aws iam get-role \
  --role-name "${EXECUTION_ROLE_NAME}" \
  --query 'Role.Arn' \
  --output text 2>/dev/null | strip_cr || true)
if [ "${EXECUTION_ROLE_ARN}" == "None" ]; then
  EXECUTION_ROLE_ARN=""
fi

if [ -z "$EXECUTION_ROLE_ARN" ]; then
  log_info "Creating task execution role: ${EXECUTION_ROLE_NAME}"

  aws iam create-role \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --assume-role-policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }]
    }' >/dev/null

  # Attach AWS managed policy
  aws iam attach-role-policy \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"

  # Add S3 access for environment files
  aws iam put-role-policy \
    --role-name "${EXECUTION_ROLE_NAME}" \
    --policy-name "S3EnvFileAccess" \
    --policy-document "{
      \"Version\": \"2012-10-17\",
      \"Statement\": [{
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\"],
        \"Resource\": \"arn:aws:s3:::${S3_BUCKET}/*\"
      }, {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetBucketLocation\"],
        \"Resource\": \"arn:aws:s3:::${S3_BUCKET}\"
      }]
    }"

  sleep 10  # Wait for IAM role propagation

  EXECUTION_ROLE_ARN=$(aws iam get-role --role-name "${EXECUTION_ROLE_NAME}" --query 'Role.Arn' --output text | strip_cr)
  log_success "Created execution role: ${EXECUTION_ROLE_ARN}"
else
  log_success "Using existing execution role: ${EXECUTION_ROLE_ARN}"
fi

# Task Role (used by application for AWS service access)
TASK_ROLE_ARN=$(aws iam get-role \
  --role-name "${TASK_ROLE_NAME}" \
  --query 'Role.Arn' \
  --output text 2>/dev/null | strip_cr || true)
if [ "${TASK_ROLE_ARN}" == "None" ]; then
  TASK_ROLE_ARN=""
fi

if [ -z "$TASK_ROLE_ARN" ]; then
  log_info "Creating task role: ${TASK_ROLE_NAME}"

  aws iam create-role \
    --role-name "${TASK_ROLE_NAME}" \
    --assume-role-policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }]
    }' >/dev/null

  # Add permissions your app needs (add more as needed)
  aws iam put-role-policy \
    --role-name "${TASK_ROLE_NAME}" \
    --policy-name "AppPermissions" \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath"
        ],
        "Resource": "*"
      }]
    }'

  sleep 10  # Wait for IAM role propagation

  TASK_ROLE_ARN=$(aws iam get-role --role-name "${TASK_ROLE_NAME}" --query 'Role.Arn' --output text | strip_cr)
  log_success "Created task role: ${TASK_ROLE_ARN}"
else
  log_success "Using existing task role: ${TASK_ROLE_ARN}"
fi

if [ -n "${MSK_CLUSTER_ARN:-}" ]; then
  log_info "Attaching MSK IAM policy to task role..."
  MSK_CLUSTER_ARN="${MSK_CLUSTER_ARN}" \
    MSK_TOPIC_ARN="${MSK_TOPIC_ARN:-}" \
    MSK_GROUP_ARN="${MSK_GROUP_ARN:-}" \
    MSK_TOPIC_NAME="${MSK_TOPIC_NAME:-}" \
    MSK_GROUP_NAME="${MSK_GROUP_NAME:-}" \
    ./infrastructure/aws/scripts/attach-msk-policy.sh "${ENVIRONMENT}"
fi

echo ""

################################################################################
# STEP 4: Create CloudWatch Log Group
################################################################################

log_info "[4/9] Creating CloudWatch log group..."

if aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP}" --region ${AWS_REGION} | grep -q "${LOG_GROUP}"; then
  log_success "Log group already exists: ${LOG_GROUP}"
else
  aws logs create-log-group --log-group-name "${LOG_GROUP}" --region ${AWS_REGION}
  aws logs put-retention-policy --log-group-name "${LOG_GROUP}" --retention-in-days 7 --region ${AWS_REGION}
  log_success "Created log group: ${LOG_GROUP}"
fi

echo ""

################################################################################
# STEP 5: Create ECS Cluster
################################################################################

log_info "[5/9] Creating ECS cluster..."

EXISTING_CLUSTER=$(aws ecs describe-clusters \
  --clusters "${ECS_CLUSTER}" \
  --region ${AWS_REGION} \
  --query 'clusters[0].status' \
  --output text 2>/dev/null | strip_cr || echo "MISSING")

if [ "$EXISTING_CLUSTER" == "ACTIVE" ]; then
  log_success "ECS cluster already exists: ${ECS_CLUSTER}"
else
  aws ecs create-cluster \
    --cluster-name "${ECS_CLUSTER}" \
    --region ${AWS_REGION} >/dev/null

  log_success "Created ECS cluster: ${ECS_CLUSTER}"
fi

echo ""

################################################################################
# STEP 6: Build and Push Docker Image
################################################################################

log_info "[6/9] Building and pushing Docker image..."

# Login to ECR
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# Check if ECR repository exists
if ! aws ecr describe-repositories --repository-names "${ECR_REPOSITORY}" --region ${AWS_REGION} >/dev/null 2>&1; then
  log_info "Creating ECR repository: ${ECR_REPOSITORY}"
  aws ecr create-repository \
    --repository-name "${ECR_REPOSITORY}" \
    --region ${AWS_REGION} \
    --image-scanning-configuration scanOnPush=true \
    --encryption-configuration encryptionType=AES256 >/dev/null
  log_success "Created ECR repository: ${ECR_REPOSITORY}"
fi

# Build image
IMAGE_TAG="${ENVIRONMENT}-$(date +%Y%m%d-%H%M%S)-$(git rev-parse --short HEAD 2>/dev/null || echo 'local')"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"
IMAGE_URI_LATEST="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${ENVIRONMENT}"

log_info "Building Docker image: ${IMAGE_TAG}"
docker build \
  --platform linux/amd64 \
  -t "${IMAGE_URI}" \
  -t "${IMAGE_URI_LATEST}" \
  .

log_info "Pushing Docker image to ECR..."
docker push "${IMAGE_URI}"
docker push "${IMAGE_URI_LATEST}"

log_success "Pushed image: ${IMAGE_URI}"

echo ""

################################################################################
# STEP 7: Register Task Definitions
################################################################################

log_info "[7/9] Registering ECS task definitions..."

for template in taskdef-frank-worker.json taskdef-photon-ingest.json taskdef-background-workers.json; do
  if [ ! -f "infrastructure/aws/ecs/${template}" ]; then
    log_error "Task definition template not found at: infrastructure/aws/ecs/${template}"
    exit 1
  fi
done

"${PY_BIN}" << PY
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
# STEP 8: Create ECS Services
################################################################################

log_info "[8/9] Creating ECS services..."

update_or_create_service() {
  local service_name="$1"
  local task_def_arn="$2"
  local desired_count="$3"
  local status

  status=$(aws ecs describe-services \
    --cluster "${ECS_CLUSTER}" \
    --services "${service_name}" \
    --region ${AWS_REGION} \
    --query 'services[0].status' \
    --output text 2>/dev/null | strip_cr || echo "MISSING")

  if [ "$status" == "ACTIVE" ]; then
    log_warning "ECS service already exists. Updating: ${service_name}"
    aws ecs update-service \
      --cluster "${ECS_CLUSTER}" \
      --service "${service_name}" \
      --task-definition "${task_def_arn}" \
      --desired-count "${desired_count}" \
      --force-new-deployment \
      --enable-execute-command \
      --region ${AWS_REGION} >/dev/null
    log_success "Updated ECS service: ${service_name}"
  else
    log_info "Creating new ECS service: ${service_name}"
    aws ecs create-service \
      --cluster "${ECS_CLUSTER}" \
      --service-name "${service_name}" \
      --task-definition "${task_def_arn}" \
      --desired-count "${desired_count}" \
      --launch-type FARGATE \
      --enable-execute-command \
      --network-configuration "awsvpcConfiguration={subnets=[${SUBNET1},${SUBNET2}],securityGroups=[${SECURITY_GROUP_ID}],assignPublicIp=ENABLED}" \
      --region ${AWS_REGION} >/dev/null
    log_success "Created ECS service: ${service_name}"
  fi
}

update_or_create_service "${ECS_SERVICE_INGEST}" "${INGEST_TASK_DEF_ARN}" "${INGEST_DESIRED_COUNT}"
update_or_create_service "${ECS_SERVICE}" "${WORKER_TASK_DEF_ARN}" "${WORKER_DESIRED_COUNT}"
update_or_create_service "${ECS_SERVICE_BACKGROUND}" "${BACKGROUND_TASK_DEF_ARN}" "${BACKGROUND_DESIRED_COUNT}"

echo ""

################################################################################
# STEP 9: Wait for Service Stability
################################################################################

log_info "[9/9] Waiting for services to become stable (this may take 2-3 minutes)..."

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
# Get Task Public IP
################################################################################

log_info "Fetching worker task public IP addresses..."

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
      log_success "  Health check: http://${PUBLIC_IP}:8000/health"
    fi
  done
fi

echo ""

################################################################################
# Summary
################################################################################

log_success "============================================================"
log_success "ECS Infrastructure Setup Complete!"
log_success "============================================================"
echo ""
echo "Cluster:            ${ECS_CLUSTER}"
echo "Services:           ${ECS_SERVICE}, ${ECS_SERVICE_INGEST}, ${ECS_SERVICE_BACKGROUND}"
echo "Worker Task Def:    ${WORKER_TASK_DEF_ARN}"
echo "Ingest Task Def:    ${INGEST_TASK_DEF_ARN}"
echo "Background Task Def:${BACKGROUND_TASK_DEF_ARN}"
echo "Image:              ${IMAGE_URI}"
echo "Region:             ${AWS_REGION}"
echo "Log Group:          ${LOG_GROUP}"
echo ""
echo "Next steps:"
echo "  1. Test health endpoint: curl http://PUBLIC_IP:8000/health"
echo "  2. View logs: aws logs tail ${LOG_GROUP} --follow --region ${AWS_REGION}"
echo "  3. Update service: aws ecs update-service --cluster ${ECS_CLUSTER} --services ${ECS_SERVICE} ${ECS_SERVICE_INGEST} ${ECS_SERVICE_BACKGROUND} --force-new-deployment --region ${AWS_REGION}"
echo ""
log_success "============================================================"
