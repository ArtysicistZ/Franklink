# AWS App Runner Version Management Guide

Complete guide for deploying and rolling back Franklink on AWS App Runner with proper version control.

---

## Table of Contents

1. [Overview](#overview)
2. [Version Naming Convention](#version-naming-convention)
3. [Deployment Workflows](#deployment-workflows)
4. [Rollback Procedures](#rollback-procedures)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

---

## Overview

### Why Version Management?

Proper version management gives you:
- **Traceability**: Know exactly what code is running in production
- **Quick rollbacks**: Revert to any previous version in minutes
- **Audit trail**: Track who deployed what and when
- **Confidence**: Deploy without fear of breaking production

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   VERSION MANAGEMENT FLOW                    │
└─────────────────────────────────────────────────────────────┘

Git Commit (local)
    │
    ├─> generate version: v20250108-150530-a1b2c3d
    │                     └─ timestamp + git SHA
    │
    ├─> Docker Build
    │   └─> Tagged with version
    │
    ├─> Push to ECR with multiple tags:
    │   ├─> v20250108-150530-a1b2c3d  (specific version)
    │   ├─> production                (environment)
    │   ├─> production-latest         (environment + latest)
    │   └─> latest                    (triggers auto-deploy)
    │
    └─> App Runner Auto-Deploy (monitors :latest tag)
        └─> Deployment triggered
            └─> Rollback history saved to ~/.frank-deployments/
```

### Key Components

1. **deploy-apprunner-versioned.sh**: Enhanced deployment with versioning
2. **rollback-apprunner.sh**: Automated rollback tool
3. **Deployment history**: Stored in `~/.frank-deployments/`
4. **ECR image tags**: Multiple tags per image for flexibility

---

## Version Naming Convention

### Automatic Versioning (Recommended)

Format: `v{TIMESTAMP}-{GIT_SHA}`

Example: `v20250108-150530-a1b2c3d`
- `20250108`: Date (YYYYMMDD)
- `150530`: Time (HHMMSS) in UTC
- `a1b2c3d`: Short git commit SHA

**Generated automatically by**: `deploy-apprunner-versioned.sh`

### Manual Versioning

You can also specify custom versions:

```bash
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production v1.2.3
```

Recommended format:
- Semantic versioning: `v1.2.3`
- Feature releases: `v1.2.3-feature-name`
- Hotfixes: `v1.2.3-hotfix`

---

## Deployment Workflows

### Standard Deployment

**Scenario**: Deploy latest code to production

```bash
# 1. Ensure you're on the right branch
git checkout main
git pull origin main

# 2. Commit your changes
git add .
git commit -m "Add new feature"

# 3. Deploy with versioned script
cd /Users/eric/Documents/Franklink-iMessage
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production

# Version is auto-generated: v20250108-150530-a1b2c3d
```

**What happens**:
1. Script checks for uncommitted changes
2. Generates version from timestamp + git SHA
3. Builds Docker image with version tag
4. Pushes to ECR with multiple tags
5. Triggers App Runner deployment
6. Saves rollback metadata to `~/.frank-deployments/production-rollback.txt`

### Hotfix Deployment

**Scenario**: Critical bug fix needed immediately

```bash
# 1. Create hotfix branch
git checkout -b hotfix/critical-bug
git commit -m "Fix critical authentication bug"

# 2. Deploy with custom version
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production v1.2.3-hotfix

# 3. Verify fix in production
curl https://<service-url>/health

# 4. Merge back to main
git checkout main
git merge hotfix/critical-bug
git push origin main
```

### Staging Deployment

**Scenario**: Test changes in staging environment first

```bash
# 1. Deploy to staging
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh staging

# 2. Test thoroughly in staging

# 3. If tests pass, deploy same version to production
STAGING_VERSION=$(tail -1 ~/.frank-deployments/staging-rollback.txt | awk '{print $1}')
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production $STAGING_VERSION
```

### Feature Branch Deployment

**Scenario**: Deploy a feature branch for testing

```bash
# 1. Checkout feature branch
git checkout feature/new-onboarding

# 2. Deploy to staging with feature tag
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh staging v1.3.0-beta-onboarding

# 3. Test in staging

# 4. If approved, merge to main and deploy to production
git checkout main
git merge feature/new-onboarding
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production
```

---

## Rollback Procedures

### Quick Rollback to Previous Version

**Scenario**: Latest deployment has a bug, need to rollback immediately

```bash
# Rollback to the last known good version
./infrastructure/aws/scripts/rollback-apprunner.sh production previous
```

**What happens**:
1. Retrieves previous version from `~/.frank-deployments/production-rollback.txt`
2. Confirms with you before proceeding
3. Re-tags that version as `:latest` in ECR
4. Triggers App Runner deployment
5. Monitors deployment until complete
6. Records rollback in history

**Time to complete**: 3-5 minutes

### Rollback to Specific Version

**Scenario**: Need to rollback to a specific known-good version

```bash
# 1. List available versions
./infrastructure/aws/scripts/rollback-apprunner.sh production list

# Output shows:
# v20250108-150530-a1b2c3d | 2025-01-08 15:05:30 UTC
# v20250107-143000-b2c3d4e | 2025-01-07 14:30:00 UTC
# v20250106-120000-c3d4e5f | 2025-01-06 12:00:00 UTC

# 2. Rollback to specific version
./infrastructure/aws/scripts/rollback-apprunner.sh production v20250107-143000-b2c3d4e
```

### View Deployment History

```bash
# Show recent deployments
cat ~/.frank-deployments/production-rollback.txt

# Output format: version | previous_image | deployed_at
# v20250108-150530-a1b2c3d | ...ecr.../frank-ai:v20250107-143000 | 2025-01-08T15:05:30Z
```

### Emergency Rollback (Manual)

**Scenario**: Rollback script fails, need manual intervention

```bash
# 1. Find target version
aws ecr describe-images --repository-name frank-ai --region us-east-1 \
  --query 'sort_by(imageDetails,& imagePushedAt)[-10:].[imageTags[0], imagePushedAt]' \
  --output table

# 2. Get image manifest
aws ecr batch-get-image --repository-name frank-ai \
  --image-ids imageTag=v20250107-143000-b2c3d4e \
  --region us-east-1 --query 'images[0].imageManifest' \
  --output text > manifest.json

# 3. Re-tag as latest
aws ecr put-image --repository-name frank-ai \
  --image-tag latest --image-manifest file://manifest.json \
  --region us-east-1

# 4. Trigger deployment
SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
  --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
  --output text)

aws apprunner start-deployment --service-arn $SERVICE_ARN --region us-east-1

# 5. Monitor status
aws apprunner describe-service --service-arn $SERVICE_ARN \
  --region us-east-1 --query 'Service.Status'
```

---

## Best Practices

### Pre-Deployment Checklist

- [ ] All tests pass locally
- [ ] Code reviewed and approved
- [ ] Database migrations tested
- [ ] Environment variables up to date
- [ ] No uncommitted changes (or intentionally deploying uncommitted)
- [ ] Current production version noted (for quick rollback)

### Deployment Best Practices

1. **Deploy during low-traffic periods**
   - Reduces impact if issues occur
   - Easier to monitor for anomalies

2. **Monitor after deployment**
   ```bash
   # Watch logs in real-time
   SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
     --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
     --output text)

   aws logs tail /aws/apprunner/frank-career-counselor/${SERVICE_ARN##*/}/application --follow
   ```

3. **Test critical paths immediately**
   ```bash
   SERVICE_URL=$(aws apprunner describe-service --service-arn $SERVICE_ARN \
     --region us-east-1 --query 'Service.ServiceUrl' --output text)

   # Health check
   curl https://$SERVICE_URL/health

   # Test webhook (send test message)
   # Test auth flow
   # Test payment flow
   ```

4. **Keep rollback history**
   - Backup `~/.frank-deployments/` regularly
   - Consider storing in S3 or git

### Rollback Best Practices

1. **Test rollback in staging first**
   ```bash
   ./infrastructure/aws/scripts/rollback-apprunner.sh staging previous
   ```

2. **Communicate with team**
   - Announce rollback in Slack/Discord
   - Document reason for rollback
   - Create incident report

3. **Investigate root cause**
   - Check logs before and after deployment
   - Compare code between versions
   - Fix issue before next deployment

4. **Don't rollback too quickly**
   - Give deployment 5-10 minutes to stabilize
   - Some issues are transient
   - Check if it's actually a deployment issue

### Version Cleanup

Periodically clean up old ECR images to reduce costs:

```bash
# List images by age
aws ecr describe-images --repository-name frank-ai --region us-east-1 \
  --query 'sort_by(imageDetails,& imagePushedAt)[*].[imageTags[0], imagePushedAt]' \
  --output table

# Delete specific old version (BE CAREFUL!)
aws ecr batch-delete-image --repository-name frank-ai \
  --image-ids imageTag=v20240101-120000-old1234 \
  --region us-east-1

# Recommended: Keep last 20 versions, delete rest
# Create a cleanup script for this
```

---

## Troubleshooting

### Deployment Issues

#### Problem: "Service won't start after deployment"

**Symptoms**: Status stuck at `OPERATION_IN_PROGRESS` or becomes `FAILED`

**Solution**:
```bash
# 1. Check logs
SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
  --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
  --output text)

aws logs tail /aws/apprunner/frank-career-counselor/${SERVICE_ARN##*/}/application

# 2. Common issues:
# - Missing environment variables → Check App Runner console
# - Database connection fails → Verify Supabase credentials
# - Redis connection fails → Check REDIS_URL
# - Port mismatch → Should be 8000

# 3. If critical, rollback immediately
./infrastructure/aws/scripts/rollback-apprunner.sh production previous
```

#### Problem: "Docker build fails"

**Symptoms**: Build error during deployment

**Solution**:
```bash
# 1. Test build locally first
docker build -t frank-ai:test --platform linux/amd64 -f Dockerfile .

# 2. Check Dockerfile syntax
# 3. Verify all COPY paths exist
# 4. Check requirements.txt for bad packages

# 5. If specific to AWS, check:
docker run --rm -it frank-ai:test /bin/bash
# Test inside container
```

#### Problem: "Image push to ECR fails"

**Symptoms**: Authentication or permission error

**Solution**:
```bash
# 1. Re-authenticate to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com

# 2. Check IAM permissions
aws ecr describe-repositories --region us-east-1

# 3. Verify repository exists
aws ecr describe-repositories --repository-names frank-ai --region us-east-1
```

### Rollback Issues

#### Problem: "Version not found in ECR"

**Solution**:
```bash
# List all available versions
aws ecr describe-images --repository-name frank-ai --region us-east-1

# If version was deleted, you need to redeploy from git
git checkout <commit-sha>
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production v-recovered
```

#### Problem: "Rollback script fails"

**Solution**: Use manual rollback procedure (see above)

#### Problem: "Service still broken after rollback"

**Possible causes**:
1. **Database migration issue**
   - Rollback doesn't revert database changes
   - Check if migration needs manual rollback
   - Verify database schema compatibility

2. **External service issue**
   - Not actually a code problem
   - Check SendBlue, Azure OpenAI, Supabase status

3. **Environment variable change**
   - Check if env vars were changed in App Runner console
   - Compare with previous version's expected vars

### Monitoring Commands

```bash
# Get current deployed version
SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
  --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
  --output text)

aws apprunner describe-service --service-arn $SERVICE_ARN \
  --region us-east-1 --query 'Service.SourceConfiguration.ImageRepository.ImageIdentifier'

# Check service status
aws apprunner describe-service --service-arn $SERVICE_ARN \
  --region us-east-1 --query 'Service.Status'

# List recent deployments
aws apprunner list-operations --service-arn $SERVICE_ARN \
  --region us-east-1

# View real-time logs
aws logs tail /aws/apprunner/frank-career-counselor/${SERVICE_ARN##*/}/application --follow

# Get service metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/AppRunner \
  --metric-name RequestCount \
  --dimensions Name=ServiceName,Value=frank-career-counselor \
  --start-time $(date -u -v-1H +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

---

## Quick Reference

### Common Commands

```bash
# Deploy to production (auto version)
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production

# Deploy to staging
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh staging

# Deploy with custom version
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production v1.2.3

# List available versions
./infrastructure/aws/scripts/rollback-apprunner.sh production list

# Rollback to previous
./infrastructure/aws/scripts/rollback-apprunner.sh production previous

# Rollback to specific version
./infrastructure/aws/scripts/rollback-apprunner.sh production v20250107-143000-b2c3d4e

# View deployment history
cat ~/.frank-deployments/production-rollback.txt

# Make scripts executable (one-time setup)
chmod +x infrastructure/aws/deploy-apprunner-versioned.sh
chmod +x infrastructure/aws/rollback-apprunner.sh
```

### File Locations

- **Deployment scripts**: `infrastructure/aws/`
- **Deployment history**: `~/.frank-deployments/`
- **ECR repository**: `<aws-account-id>.dkr.ecr.us-east-1.amazonaws.com/frank-ai`
- **App Runner service**: `frank-career-counselor`

---

## Migration from Old Deployment Script

If you've been using the old `deploy-apprunner.sh`:

### One-time migration

```bash
# 1. Note current production version (if any)
SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
  --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
  --output text)

CURRENT_IMAGE=$(aws apprunner describe-service --service-arn $SERVICE_ARN \
  --region us-east-1 --query 'Service.SourceConfiguration.ImageRepository.ImageIdentifier' \
  --output text)

echo "Current production image: $CURRENT_IMAGE"

# 2. Make new scripts executable
chmod +x infrastructure/aws/deploy-apprunner-versioned.sh
chmod +x infrastructure/aws/rollback-apprunner.sh

# 3. Test new script in staging first
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh staging

# 4. Once confident, use for production
./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production
```

### What changes

- **Old**: Images tagged only as `latest` and `{environment}`
- **New**: Images tagged with version + latest + environment + environment-latest

Old images are still available, but won't have version tags. You can still use them via `:latest` or `:production` tags.

---

## Support

For issues:
- Check logs first: `aws logs tail /aws/apprunner/...`
- Review this guide
- Check AWS App Runner console
- Refer to [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)

---

**Version**: 1.0.0
**Last Updated**: January 8, 2025
**Maintained by**: Franklink Engineering Team
