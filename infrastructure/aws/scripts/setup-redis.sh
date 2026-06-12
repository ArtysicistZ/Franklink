#!/bin/bash

# Set up AWS ElastiCache Redis for Frank
# Usage: ./setup-redis.sh [environment]

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ENVIRONMENT=${1:-production}
AWS_REGION=${AWS_REGION:-us-east-1}
CACHE_CLUSTER_ID="frank-redis-${ENVIRONMENT}"
CACHE_NODE_TYPE="cache.t4g.micro"
ENGINE_VERSION="7.0"

echo -e "${GREEN}🔧 Setting up ElastiCache Redis for Frank${NC}"
echo -e "${YELLOW}Environment: ${ENVIRONMENT}${NC}"
echo -e "${YELLOW}Region: ${AWS_REGION}${NC}\n"

# Check if cluster already exists
echo "Checking if Redis cluster exists..."
if aws elasticache describe-cache-clusters \
    --cache-cluster-id ${CACHE_CLUSTER_ID} \
    --region ${AWS_REGION} &> /dev/null; then

    echo -e "${YELLOW}✅ Redis cluster already exists${NC}"

    # Get endpoint
    REDIS_ENDPOINT=$(aws elasticache describe-cache-clusters \
        --cache-cluster-id ${CACHE_CLUSTER_ID} \
        --show-cache-node-info \
        --region ${AWS_REGION} \
        --query 'CacheClusters[0].CacheNodes[0].Endpoint.Address' \
        --output text)

    REDIS_PORT=$(aws elasticache describe-cache-clusters \
        --cache-cluster-id ${CACHE_CLUSTER_ID} \
        --show-cache-node-info \
        --region ${AWS_REGION} \
        --query 'CacheClusters[0].CacheNodes[0].Endpoint.Port' \
        --output text)

    echo -e "${GREEN}Redis Endpoint: ${REDIS_ENDPOINT}:${REDIS_PORT}${NC}"
    echo -e "${GREEN}REDIS_URL=redis://${REDIS_ENDPOINT}:${REDIS_PORT}/0${NC}"

    exit 0
fi

# Create subnet group (required for ElastiCache)
echo -e "\nStep 1: Creating subnet group..."
SUBNET_GROUP_NAME="frank-redis-subnet-group-${ENVIRONMENT}"

# Get default VPC
DEFAULT_VPC=$(aws ec2 describe-vpcs \
    --filters "Name=is-default,Values=true" \
    --region ${AWS_REGION} \
    --query 'Vpcs[0].VpcId' \
    --output text)

echo -e "${YELLOW}Using default VPC: ${DEFAULT_VPC}${NC}"

# Get subnets from default VPC
SUBNETS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC}" \
    --region ${AWS_REGION} \
    --query 'Subnets[*].SubnetId' \
    --output text)

echo -e "${YELLOW}Found subnets: ${SUBNETS}${NC}"

# Create subnet group if it doesn't exist
if ! aws elasticache describe-cache-subnet-groups \
    --cache-subnet-group-name ${SUBNET_GROUP_NAME} \
    --region ${AWS_REGION} &> /dev/null; then

    aws elasticache create-cache-subnet-group \
        --cache-subnet-group-name ${SUBNET_GROUP_NAME} \
        --cache-subnet-group-description "Subnet group for Frank Redis" \
        --subnet-ids ${SUBNETS} \
        --region ${AWS_REGION}

    echo -e "${GREEN}✅ Subnet group created${NC}"
else
    echo -e "${YELLOW}Subnet group already exists${NC}"
fi

# Step 2: Create security group
echo -e "\nStep 2: Creating security group..."
SECURITY_GROUP_NAME="frank-redis-sg-${ENVIRONMENT}"

# Check if security group exists
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SECURITY_GROUP_NAME}" "Name=vpc-id,Values=${DEFAULT_VPC}" \
    --region ${AWS_REGION} \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "")

if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
    echo "Creating new security group..."
    SG_ID=$(aws ec2 create-security-group \
        --group-name ${SECURITY_GROUP_NAME} \
        --description "Security group for Frank Redis" \
        --vpc-id ${DEFAULT_VPC} \
        --region ${AWS_REGION} \
        --query 'GroupId' \
        --output text)

    # Allow Redis port from anywhere in VPC (you may want to restrict this)
    aws ec2 authorize-security-group-ingress \
        --group-id ${SG_ID} \
        --protocol tcp \
        --port 6379 \
        --cidr 0.0.0.0/0 \
        --region ${AWS_REGION}

    echo -e "${GREEN}✅ Security group created: ${SG_ID}${NC}"
else
    echo -e "${YELLOW}Security group already exists: ${SG_ID}${NC}"
fi

# Step 3: Create Redis cluster
echo -e "\nStep 3: Creating Redis cluster..."
echo -e "${YELLOW}This will take 5-10 minutes...${NC}"

aws elasticache create-cache-cluster \
    --cache-cluster-id ${CACHE_CLUSTER_ID} \
    --cache-node-type ${CACHE_NODE_TYPE} \
    --engine redis \
    --engine-version ${ENGINE_VERSION} \
    --num-cache-nodes 1 \
    --cache-subnet-group-name ${SUBNET_GROUP_NAME} \
    --security-group-ids ${SG_ID} \
    --region ${AWS_REGION} \
    --tags Key=Application,Value=Frank Key=Environment,Value=${ENVIRONMENT}

echo -e "${GREEN}✅ Redis cluster creation initiated${NC}"

# Wait for cluster to be available
echo -e "\n${YELLOW}Waiting for cluster to become available...${NC}"
aws elasticache wait cache-cluster-available \
    --cache-cluster-id ${CACHE_CLUSTER_ID} \
    --region ${AWS_REGION}

# Get endpoint
REDIS_ENDPOINT=$(aws elasticache describe-cache-clusters \
    --cache-cluster-id ${CACHE_CLUSTER_ID} \
    --show-cache-node-info \
    --region ${AWS_REGION} \
    --query 'CacheClusters[0].CacheNodes[0].Endpoint.Address' \
    --output text)

REDIS_PORT=$(aws elasticache describe-cache-clusters \
    --cache-cluster-id ${CACHE_CLUSTER_ID} \
    --show-cache-node-info \
    --region ${AWS_REGION} \
    --query 'CacheClusters[0].CacheNodes[0].Endpoint.Port' \
    --output text)

echo -e "\n${GREEN}🎉 Redis cluster is ready!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Endpoint: ${REDIS_ENDPOINT}${NC}"
echo -e "${GREEN}Port: ${REDIS_PORT}${NC}"
echo -e "\n${YELLOW}Add this to your App Runner environment variables:${NC}"
echo -e "${GREEN}REDIS_URL=redis://${REDIS_ENDPOINT}:${REDIS_PORT}/0${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Estimated cost
echo -e "\n${YELLOW}💰 Estimated monthly cost: ~$13 (cache.t4g.micro)${NC}"
