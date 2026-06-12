#!/bin/bash

################################################################################
# Get ECS Task Public IP
#
# Usage: ./get-task-ip.sh [environment] [service]
# Example: ./get-task-ip.sh testing
# Example: ./get-task-ip.sh testing ingest
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

ENVIRONMENT="${1:-testing}"
SERVICE_HINT="${2:-}"
AWS_REGION="us-east-2"

ECS_CLUSTER="franklink-${ENVIRONMENT}-cluster"
ECS_SERVICE_BASE="frank-career-counselor-${ENVIRONMENT}-ecs"
if [ -n "$SERVICE_HINT" ]; then
  if [[ "$SERVICE_HINT" == *"-"* ]]; then
    ECS_SERVICE="${SERVICE_HINT}"
  else
    ECS_SERVICE="${ECS_SERVICE_BASE}-${SERVICE_HINT}"
  fi
else
  ECS_SERVICE="${ECS_SERVICE_BASE}"
fi

echo "Fetching task information for: ${ECS_SERVICE}"
echo ""

# Get running tasks
TASK_ARNS=$(aws ecs list-tasks \
  --cluster "${ECS_CLUSTER}" \
  --service-name "${ECS_SERVICE}" \
  --desired-status RUNNING \
  --region ${AWS_REGION} \
  --query 'taskArns' \
  --output text | strip_cr)

if [ -z "$TASK_ARNS" ]; then
  echo "No running tasks found for service: ${ECS_SERVICE}"
  exit 1
fi

for TASK_ARN in $TASK_ARNS; do
  echo ""
  echo "Task: ${TASK_ARN##*/}"

  # Get ENI
  ENI_ID=$(aws ecs describe-tasks \
    --cluster "${ECS_CLUSTER}" \
    --tasks "${TASK_ARN}" \
    --region ${AWS_REGION} \
    --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' \
    --output text | strip_cr)

  if [ -n "$ENI_ID" ]; then
    # Get public IP
    PUBLIC_IP=$(aws ec2 describe-network-interfaces \
      --network-interface-ids "${ENI_ID}" \
      --region ${AWS_REGION} \
      --query 'NetworkInterfaces[0].Association.PublicIp' \
      --output text 2>/dev/null | strip_cr || echo "N/A")

    # Get task status
    TASK_STATUS=$(aws ecs describe-tasks \
      --cluster "${ECS_CLUSTER}" \
      --tasks "${TASK_ARN}" \
      --region ${AWS_REGION} \
      --query 'tasks[0].lastStatus' \
      --output text | strip_cr)

    echo "Status:         ${TASK_STATUS}"
    echo "Public IP:      ${PUBLIC_IP}"
    echo ""
    echo "Endpoints:"
    echo "  Health:     http://${PUBLIC_IP}:8000/health"
    echo "  API Root:   http://${PUBLIC_IP}:8000/"
    echo "  Webhook:    http://${PUBLIC_IP}:8000/webhook/photon"
    echo ""

    # Test health endpoint
    if [ "$PUBLIC_IP" != "N/A" ]; then
      echo "Testing health endpoint..."
      HEALTH_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "http://${PUBLIC_IP}:8000/health" 2>/dev/null || echo "000")

      if [ "$HEALTH_RESPONSE" == "200" ]; then
        echo "Health check: OK (HTTP 200)"
      else
        echo "Health check: Failed (HTTP ${HEALTH_RESPONSE})"
      fi
    fi
  fi
  echo ""
  echo ""
done
