#!/bin/bash

################################################################################
# Attach MSK IAM Policy to ECS Task Role
#
# Usage:
#   MSK_CLUSTER_ARN=... MSK_TOPIC_NAMES=photon.inbound.v1,photon.inbound.retry.30s,photon.inbound.retry.2m,photon.inbound.retry.10m,photon.inbound.dlq.v1 MSK_GROUP_NAME=frank-worker \
#     ./infrastructure/aws/scripts/attach-msk-policy.sh testing
#
# Or provide explicit ARNs:
#   MSK_CLUSTER_ARN=... MSK_TOPIC_ARN=... MSK_GROUP_ARN=... \
#     ./infrastructure/aws/scripts/attach-msk-policy.sh testing
################################################################################

set -euo pipefail

# Resolve AWS CLI reliably across macOS/Linux/Windows (Git Bash).
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

export AWS_PAGER=""

ENVIRONMENT="${1:-testing}"
ROLE_NAME="franklink-ecs-task-role-${ENVIRONMENT}"

MSK_CLUSTER_ARN="${MSK_CLUSTER_ARN:-}"
MSK_TOPIC_ARN="${MSK_TOPIC_ARN:-}"
MSK_TOPIC_NAMES="${MSK_TOPIC_NAMES:-}"
MSK_GROUP_ARN="${MSK_GROUP_ARN:-}"
MSK_TOPIC_NAME="${MSK_TOPIC_NAME:-}"
MSK_GROUP_NAME="${MSK_GROUP_NAME:-}"

if [ -z "${MSK_CLUSTER_ARN}" ]; then
  echo "ERROR: MSK_CLUSTER_ARN is required."
  exit 1
fi

cluster_base="$(echo "${MSK_CLUSTER_ARN}" | awk -F: '{print $1 ":" $2 ":" $3 ":" $4 ":" $5}')"
cluster_path="$(echo "${MSK_CLUSTER_ARN}" | awk -F: '{print $6}')"
cluster_name_uuid="${cluster_path#cluster/}"

TOPIC_RESOURCES_JSON=""
if [ -n "${MSK_TOPIC_ARN}" ]; then
  TOPIC_RESOURCES_JSON="[\"${MSK_TOPIC_ARN}\"]"
else
  topic_names_csv="${MSK_TOPIC_NAMES}"
  if [ -z "${topic_names_csv}" ] && [ -n "${MSK_TOPIC_NAME}" ]; then
    topic_names_csv="${MSK_TOPIC_NAME}"
  fi
  if [ -z "${topic_names_csv}" ]; then
    echo "ERROR: MSK_TOPIC_ARN or MSK_TOPIC_NAMES/MSK_TOPIC_NAME is required."
    exit 1
  fi
  TOPIC_RESOURCES_JSON="["
  IFS=',' read -r -a _topic_names <<< "${topic_names_csv}"
  for _topic_name in "${_topic_names[@]}"; do
    topic_trimmed="$(echo "${_topic_name}" | xargs)"
    if [ -z "${topic_trimmed}" ]; then
      continue
    fi
    TOPIC_RESOURCES_JSON="${TOPIC_RESOURCES_JSON}\"${cluster_base}:topic/${cluster_name_uuid}/${topic_trimmed}\","
  done
  TOPIC_RESOURCES_JSON="${TOPIC_RESOURCES_JSON%,}]"
fi

if [ -z "${MSK_GROUP_ARN}" ] && [ -n "${MSK_GROUP_NAME}" ]; then
  MSK_GROUP_ARN="${cluster_base}:group/${cluster_name_uuid}/${MSK_GROUP_NAME}"
fi

if [ -z "${TOPIC_RESOURCES_JSON}" ] || [ "${TOPIC_RESOURCES_JSON}" = "[]" ]; then
  echo "ERROR: Topic resource list is empty."
  exit 1
fi

if [ -z "${MSK_GROUP_ARN}" ]; then
  echo "ERROR: MSK_GROUP_ARN or MSK_GROUP_NAME is required."
  exit 1
fi

policy_doc="$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:WriteDataIdempotently"
      ],
      "Resource": "${MSK_CLUSTER_ARN}"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData"
      ],
      "Resource": ${TOPIC_RESOURCES_JSON}
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": "${MSK_GROUP_ARN}"
    }
  ]
}
JSON
)"

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "MSKAccess" \
  --policy-document "${policy_doc}"

echo "Attached MSKAccess policy to role: ${ROLE_NAME}"
