# Franklink iMessage Application - Production Deployment Guide

**Version:** 1.0
**Last Updated:** 2025-11-21
**AWS Region:** us-east-2 (Ohio)
**Service:** AWS App Runner

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Pre-Deployment Checklist](#pre-deployment-checklist)
3. [Deployment Process](#deployment-process)
4. [Post-Deployment Verification](#post-deployment-verification)
5. [Rollback Procedures](#rollback-procedures)
6. [Troubleshooting](#troubleshooting)
7. [Environment Configuration](#environment-configuration)
8. [Monitoring and Logs](#monitoring-and-logs)

---

## Prerequisites

### Required Tools

1. **Docker Desktop** (for building images)
   - Version: 24.0 or higher
   - Platform: linux/amd64 (critical for AWS compatibility)
   - Ensure Docker daemon is running before deployment

2. **AWS CLI v2**
   - Installation path: `C:\Program Files\Amazon\AWSCLIV2\aws.exe`
   - Version: 2.32.2 or higher
   - Must be configured with credentials for account: `065875799800`

3. **Git**
   - For version control and tagging releases
   - Ensure working directory is clean before deployment

4. **Python 3.11+** (for utility scripts)
   - Required for database management scripts
   - Virtual environment with dependencies installed

### AWS Credentials Setup

Ensure your AWS credentials are configured:

```bash
aws configure
```

Required credentials:
- AWS Access Key ID
- AWS Secret Access Key
- Default region: `us-east-2`
- Output format: `json`

### AWS Resources

**ECR Repository:**
- Repository URI: `065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai`
- Region: `us-east-2`
- Repository Name: `frank-ai`

**App Runner Service:**
- Service Name: `frank-career-counselor`
- Service ARN: `arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346`
- Service URL: `https://dzmueh4wzy.us-east-2.awsapprunner.com`
- Auto-Deploy: Enabled (triggers on `:latest` tag push)

---

## Pre-Deployment Checklist

### 1. Code Quality Checks

- [ ] All tests passing locally (`pytest` or test suite)
- [ ] No linting errors (`flake8`, `black`, or your linter)
- [ ] Code reviewed and approved (if using PR workflow)
- [ ] No sensitive data hardcoded (API keys, passwords, etc.)
- [ ] All environment variables documented in `.env.example`

### 2. Git Repository Status

```bash
# Ensure you're on the correct branch
git branch --show-current

# Ensure branch is clean
git status

# Ensure branch is up to date with remote
git pull origin <branch-name>

# Check recent commits
git log -3 --oneline
```

Expected output for `git status`:
```
On branch <your-branch>
Your branch is up to date with 'origin/<your-branch>'.

nothing to commit, working tree clean
```

### 3. Environment Variables Verification

Review and verify all required environment variables are set in App Runner:

**Critical Variables:**
- `PHOTON_SERVER_URL` - iMessage integration endpoint
- `PHOTON_DEFAULT_NUMBER` - Frank's phone number
- `PHOTON_ENABLE_LISTENER` - Must be `"true"`
- `REDIS_URL` - Redis connection string
- `SUPABASE_URL` and `SUPABASE_KEY` - Database connection
- `AZURE_OPENAI_API_KEY` and `AZURE_OPENAI_ENDPOINT` - AI service

See [Environment Configuration](#environment-configuration) section for complete list.

### 4. Dependency Check

```bash
# Verify requirements.txt is up to date
pip freeze > requirements-check.txt
diff requirements.txt requirements-check.txt

# Clean up
rm requirements-check.txt
```

If there are differences, update `requirements.txt`:
```bash
pip freeze > requirements.txt
git add requirements.txt
git commit -m "chore: Update dependencies"
```

---

## Deployment Process

### Step 1: Create Release Tag

```bash
# Get current commit hash (short form)
git rev-parse --short HEAD

# Create version tag with timestamp and commit hash
# Format: vYYYYMMDD-HHMMSS-<commit-hash>
# Example: v20251121-143025-abaa771
VERSION_TAG="v$(date +%Y%m%d-%H%M%S)-$(git rev-parse --short HEAD)"
echo "Creating release: $VERSION_TAG"

# Optional: Create git tag for tracking
git tag -a $VERSION_TAG -m "Production deployment $VERSION_TAG"
git push origin $VERSION_TAG
```

### Step 2: Authenticate with ECR

```bash
# Login to ECR (token expires after 12 hours)
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 065875799800.dkr.ecr.us-east-2.amazonaws.com

# Verify login success
# Expected output: "Login Succeeded"
```

### Step 3: Build Docker Image

**CRITICAL:** Always build for linux/amd64 platform (AWS App Runner requirement)

```bash
# Set version tag variable (from Step 1)
VERSION_TAG="v$(date +%Y%m%d-%H%M%S)-$(git rev-parse --short HEAD)"

# Build image with platform flag
docker build --platform linux/amd64 -t frank-ai:$VERSION_TAG .

# Verify build completed successfully
docker images frank-ai:$VERSION_TAG
```

**Expected output:**
```
REPOSITORY   TAG                          IMAGE ID       CREATED         SIZE
frank-ai     v20251121-143025-abaa771     <image-id>     X seconds ago   ~789MB
```

**Common Build Issues:**

1. **Out of disk space:**
   ```bash
   docker system prune -a
   ```

2. **Build fails on dependency installation:**
   - Check `requirements.txt` for invalid packages
   - Verify Python version in Dockerfile matches local

3. **Build extremely slow:**
   - Check Docker Desktop settings > Resources
   - Ensure adequate CPU and memory allocated

### Step 4: Tag Image for ECR

```bash
# Tag with three tags: latest, versioned, and production
docker tag frank-ai:$VERSION_TAG 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:latest
docker tag frank-ai:$VERSION_TAG 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:$VERSION_TAG
docker tag frank-ai:$VERSION_TAG 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:production

# Verify tags
docker images 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai
```

**Expected output:**
```
REPOSITORY                                                  TAG                          IMAGE ID       CREATED         SIZE
065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai      latest                       <image-id>     X seconds ago   789MB
065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai      v20251121-143025-abaa771     <image-id>     X seconds ago   789MB
065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai      production                   <image-id>     X seconds ago   789MB
```

### Step 5: Push Images to ECR

```bash
# Push all three tags
docker push 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:latest
docker push 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:$VERSION_TAG
docker push 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:production
```

**Expected output for each push:**
```
The push refers to repository [065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai]
<layer-hash>: Pushed
<layer-hash>: Pushed
...
latest: digest: sha256:<digest> size: 856
```

**Push typically takes 3-5 minutes depending on network speed.**

### Step 6: Trigger App Runner Deployment

```bash
# Trigger deployment manually (or wait for auto-deploy)
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner start-deployment \
  --service-arn "arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346" \
  --region us-east-2
```

**Expected output:**
```json
{
    "OperationId": "<unique-operation-id>"
}
```

**Note:** Auto-deployment is enabled, so pushing `:latest` tag will automatically trigger deployment within 1-2 minutes. Manual trigger is optional.

### Step 7: Monitor Deployment Progress

```bash
# Check deployment status (repeat every 30-60 seconds)
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner describe-service \
  --service-arn "arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346" \
  --region us-east-2 \
  --query "Service.Status" \
  --output text
```

**Deployment States:**

1. `OPERATION_IN_PROGRESS` - Deployment is running (5-10 minutes)
2. `RUNNING` - Deployment successful, service is healthy
3. `CREATE_FAILED` / `UPDATE_FAILED` - Deployment failed (see logs)

**Typical deployment timeline:**
- Image pull: 1-2 minutes
- Container start: 2-3 minutes
- Health checks: 2-5 minutes
- Total: 5-10 minutes

### Step 8: Verify Health Check

Once status is `RUNNING`:

```bash
# Test health endpoint
curl https://dzmueh4wzy.us-east-2.awsapprunner.com/health
```

**Expected output:**
```json
{
  "status": "healthy",
  "version": "<version-tag>",
  "timestamp": "<iso-timestamp>"
}
```

### Step 9: Document Deployment

Update deployment log with:
- Date and time
- Version tag deployed
- Commit hash
- Deployer name
- Any issues encountered

Example:
```
2025-11-21 14:30:25 UTC - v20251121-143025-abaa771 (commit: abaa771)
Deployed by: <your-name>
Notes: Fixed onboarding greeting_sent flag preservation bug
Status: Success
```

---

## Post-Deployment Verification

### 1. Service Health Checks

```bash
# Health endpoint
curl https://dzmueh4wzy.us-east-2.awsapprunner.com/health

# API documentation (Swagger)
# Visit in browser: https://dzmueh4wzy.us-east-2.awsapprunner.com/docs
```

### 2. Check Application Logs

```bash
# View recent logs (last 5 minutes)
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs tail \
  /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
  --region us-east-2 \
  --since 5m \
  --follow
```

**Look for:**
- ✅ `[PHOTON] ✅ Photon listener initialized and connected successfully`
- ✅ No `ERROR` or `CRITICAL` level logs
- ✅ Successful database connections
- ✅ Redis connection established

**Red flags:**
- ❌ `ConnectionError` or `TimeoutError`
- ❌ `AuthenticationError` for external services
- ❌ Repeated health check failures
- ❌ Import errors or missing dependencies

### 3. Test Photon (iMessage) Connection

Check logs for Photon listener status:

```bash
# Search for Photon-related logs
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs filter-log-events \
  --log-group-name /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
  --region us-east-2 \
  --filter-pattern "PHOTON" \
  --max-items 10
```

**Expected log entries:**
```
[PHOTON] Initializing Photon listener...
[PHOTON] Connecting to: https://5eypbk.imsgd.photon.codes/
[PHOTON] ✅ Photon listener initialized and connected successfully
```

### 4. End-to-End Testing

**Test onboarding flow via iMessage:**

1. Clear test user data from database:
   ```bash
   python scripts/clear_user_data.py +1<your-test-phone-number>
   ```

2. Send test message from your test phone to Frank (`+13027242007`):
   ```
   Hi how u doing
   ```

3. Verify Frank's response:
   ```
   Hey! I'm Frank, your AI career counselor. 👋

   I help students discover amazing career opportunities. Let's get started!

   What's your name?
   ```

4. Reply with a test name:
   ```
   <test-name>
   ```

5. Verify Frank moves to Gmail OAuth stage:
   ```
   Thanks, <test-name>! To provide personalized career opportunities, we need to connect to your school email.
   ...
   ```

**If test fails:**
- Check application logs for errors
- Verify Photon connection is active
- Check database for user record creation
- Verify `greeting_sent` flag is set in `personal_facts`

### 5. Database Verification

```bash
# Check if personal_facts are being saved correctly
python scripts/check_user_profile.py +1<your-test-phone-number>
```

Verify `personal_facts` contains:
- `greeting_sent: true`
- `onboarding_stage: <current-stage>`
- Other flags preserved across updates

---

## Rollback Procedures

### When to Rollback

Rollback if any of these occur:
- Health checks failing after 15 minutes
- Critical functionality broken (e.g., iMessage not working)
- Database corruption or data loss
- Security vulnerability introduced
- High error rates in logs (>5% of requests)

### Quick Rollback (Recommended)

Deploy previous working version from ECR:

```bash
# 1. Find previous working version tag
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" ecr describe-images \
  --repository-name frank-ai \
  --region us-east-2 \
  --query 'sort_by(imageDetails,& imagePushedAt)[-5:].[imageTags[0], imagePushedAt]' \
  --output table

# 2. Tag previous version as latest
ROLLBACK_TAG="<previous-version-tag>"  # e.g., v20251120-102030-a49e273

docker pull 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:$ROLLBACK_TAG
docker tag 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:$ROLLBACK_TAG \
  065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:latest

# 3. Push as latest
docker push 065875799800.dkr.ecr.us-east-2.amazonaws.com/frank-ai:latest

# 4. Trigger deployment
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner start-deployment \
  --service-arn "arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346" \
  --region us-east-2

# 5. Monitor rollback deployment (same as normal deployment)
```

**Rollback time: 5-10 minutes**

### Git Rollback (If Code Changes Needed)

```bash
# 1. Revert to previous commit
git log --oneline -10  # Find the commit to revert to
git revert <commit-hash>

# Or hard reset (destructive)
git reset --hard <previous-commit-hash>

# 2. Follow normal deployment process from Step 1
```

### Database Rollback

**CRITICAL:** Always backup database before deployment if schema changes were made.

```bash
# Restore from Supabase backup
# 1. Go to Supabase Dashboard
# 2. Project Settings > Database > Backups
# 3. Select backup before deployment
# 4. Click "Restore"
```

---

## Troubleshooting

### Issue: Deployment Stuck in OPERATION_IN_PROGRESS

**Symptoms:**
- Status remains `OPERATION_IN_PROGRESS` for >15 minutes
- No logs appearing

**Solution:**
1. Check if container is failing to start:
   ```bash
   "C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner describe-service \
     --service-arn "arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346" \
     --region us-east-2 \
     --query "Service.HealthCheckConfiguration"
   ```

2. Review logs for startup errors:
   ```bash
   "C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs tail \
     /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
     --region us-east-2 \
     --since 15m
   ```

3. Common causes:
   - Missing environment variables
   - Port mismatch (must be 8000)
   - Health check endpoint not responding
   - Dependency installation failures

4. If no resolution after 20 minutes, rollback.

### Issue: Health Check Failures

**Symptoms:**
- Service status shows `RUNNING` but health checks fail
- `/health` endpoint returns 500 or timeout

**Solution:**
1. Check application logs for startup errors
2. Verify all required environment variables are set
3. Test database connectivity:
   ```bash
   # Look for database connection logs
   "C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs filter-log-events \
     --log-group-name /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
     --region us-east-2 \
     --filter-pattern "database" \
     --max-items 20
   ```
4. Verify Redis connection
5. Check external service dependencies (Azure OpenAI, Supabase, etc.)

### Issue: Photon Listener Not Connecting

**Symptoms:**
- No `[PHOTON] ✅ Photon listener initialized` in logs
- iMessage messages not being received

**Solution:**
1. Verify environment variables:
   - `PHOTON_SERVER_URL=https://5eypbk.imsgd.photon.codes/`
   - `PHOTON_ENABLE_LISTENER=true`
   - `PHOTON_DEFAULT_NUMBER=+13027242007`

2. Check Photon server status (external service)

3. Review Photon connection logs:
   ```bash
   "C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs filter-log-events \
     --log-group-name /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
     --region us-east-2 \
     --filter-pattern "PHOTON" \
     --max-items 50
   ```

4. Common issues:
   - Socket.IO connection timeout
   - Authentication failures
   - Network egress blocked (check App Runner network config)

### Issue: Database Connection Errors

**Symptoms:**
- `ConnectionError: could not connect to server`
- `OperationalError: SSL connection has been closed unexpectedly`

**Solution:**
1. Verify `POSTGRES_CONNECTION_STRING` environment variable
2. Check Supabase service status
3. Verify App Runner has network egress access
4. Test connection from local machine to rule out Supabase issues:
   ```bash
   python -c "from app.database.client import DatabaseClient; db = DatabaseClient(); print('Connected')"
   ```

### Issue: Image Push Fails

**Symptoms:**
- `unauthorized: authentication required`
- `denied: Your authorization token has expired`

**Solution:**
1. Re-authenticate with ECR:
   ```bash
   aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 065875799800.dkr.ecr.us-east-2.amazonaws.com
   ```

2. If still failing, check AWS credentials:
   ```bash
   aws sts get-caller-identity
   ```

3. Verify IAM permissions for ECR push

### Issue: Build Fails with Platform Error

**Symptoms:**
- `WARNING: The requested image's platform (linux/arm64) does not match the detected host platform`

**Solution:**
Always include `--platform linux/amd64` flag:
```bash
docker build --platform linux/amd64 -t frank-ai:$VERSION_TAG .
```

---

## Environment Configuration

### Complete Environment Variables List

**Copy from App Runner console or use this reference:**

```bash
# Core Application
APP_ENV=production
DEBUG=false
APP_HOST=0.0.0.0
APP_PORT=8000

# Photon (iMessage Integration)
PHOTON_SERVER_URL=https://5eypbk.imsgd.photon.codes/
PHOTON_DEFAULT_NUMBER=+13027242007
PHOTON_ENABLE_LISTENER=true

# Redis Cache
REDIS_URL=redis://default:<password>@redis-18970.c8.us-east-1-2.ec2.redns.redis-cloud.com:18970
REDIS_MAX_CONNECTIONS=50
REDIS_IDEMPOTENCY_TTL=86400
REDIS_CACHE_TTL=300
REDIS_RATE_LIMIT_WINDOW=60

# Azure OpenAI
AZURE_OPENAI_API_KEY=<from-azure-portal>
AZURE_OPENAI_ENDPOINT=https://franklink-openai.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-5-mini
AZURE_OPENAI_API_VERSION=2025-01-01-preview
AZURE_OPENAI_REASONING_DEPLOYMENT_NAME=gpt-5-mini

# Supabase Database
SUPABASE_URL=https://mvjefpgkozyftebhnjbp.supabase.co
SUPABASE_KEY=<from-supabase-dashboard>
POSTGRES_CONNECTION_STRING=postgresql://postgres:<password>@db.mvjefpgkozyftebhnjbp.supabase.co:5432/postgres

# Zep Memory
ZEP_API_KEY=<from-zep-dashboard>
ZEP_BASE_URL=https://api.getzep.com
ZEP_ENABLED=true

# Google OAuth
GOOGLE_CLIENT_ID=367116646662-uhtr2ep714pdkbogekvilbneujacppao.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=<from-google-console>
GOOGLE_REDIRECT_URI=https://dzmueh4wzy.us-east-2.awsapprunner.com/oauth/google/callback
GOOGLE_PROJECT_ID=franklink-career-bot

# Stripe Payments
STRIPE_API_KEY=<from-stripe-dashboard>
STRIPE_WEBHOOK_SECRET=<from-stripe-dashboard>

# Web Scraping
SCRAPINGDOG_API_KEY=<from-scrapingdog>
SCRAPINGDOG_TIMEOUT=60
SCRAPINGDOG_RETRY_ATTEMPTS=2
SCRAPINGDOG_USE_PREMIUM=true
SCRAPERAPI_KEY=<from-scraperapi>

# Reddit Integration
REDDIT_CLIENT_ID=<from-reddit-apps>
REDDIT_SECRET_KEY=<from-reddit-apps>
REDDIT_USERNAME=Franklink_API
REDDIT_PASSWORD=<reddit-account-password>

# Feature Flags
USE_LANGGRAPH=true
USE_AI_PERSONALITY=true
GMAIL_OAUTH_ENABLED=true

# Rate Limiting
RATE_LIMIT_PER_MINUTE=60
RATE_LIMIT_PER_HOUR=1000

# Legal URLs
PRIVACY_POLICY_URL=https://franklink.ai/privacy
TERMS_OF_SERVICE_URL=https://franklink.ai/terms
DATA_DELETION_URL=https://franklink.ai/data-deletion
```

### Updating Environment Variables

**Via AWS Console:**
1. Go to [App Runner Console](https://console.aws.amazon.com/apprunner/home?region=us-east-2)
2. Click `frank-career-counselor`
3. Configuration tab → Edit → Environment variables
4. Add/Update variables
5. Save changes (triggers automatic redeployment)

**Via AWS CLI:**
```bash
# Update single variable (triggers redeployment)
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner update-service \
  --service-arn "arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346" \
  --region us-east-2 \
  --source-configuration "ImageRepository={ImageConfiguration={RuntimeEnvironmentVariables={NEW_VAR=value}}}"
```

**WARNING:** Updating environment variables triggers a new deployment (5-10 min downtime).

---

## Monitoring and Logs

### CloudWatch Logs

**Application Logs:**
```bash
# Real-time log streaming
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs tail \
  /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
  --region us-east-2 \
  --follow

# Search for errors
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs filter-log-events \
  --log-group-name /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application \
  --region us-east-2 \
  --filter-pattern "ERROR" \
  --start-time $(date -d '1 hour ago' +%s)000
```

**System Logs:**
```bash
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs tail \
  /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/system \
  --region us-east-2 \
  --follow
```

### App Runner Metrics (CloudWatch)

Key metrics to monitor:
- `RequestCount` - Total requests
- `Http4xxCount` - Client errors
- `Http5xxCount` - Server errors
- `RequestLatency` - Response time
- `ActiveInstances` - Running containers
- `CPUUtilization` - CPU usage
- `MemoryUtilization` - Memory usage

```bash
# Get recent metrics
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" cloudwatch get-metric-statistics \
  --namespace AWS/AppRunner \
  --metric-name Http5xxCount \
  --dimensions Name=ServiceName,Value=frank-career-counselor \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum \
  --region us-east-2
```

### Setting Up Alarms

**Recommended CloudWatch Alarms:**

1. **High Error Rate:**
   - Metric: `Http5xxCount`
   - Threshold: > 10 errors in 5 minutes
   - Action: SNS notification

2. **Service Down:**
   - Metric: `ActiveInstances`
   - Threshold: = 0 for 2 minutes
   - Action: SNS notification + PagerDuty

3. **High Latency:**
   - Metric: `RequestLatency`
   - Threshold: > 5000ms (p99)
   - Action: SNS notification

4. **Memory Pressure:**
   - Metric: `MemoryUtilization`
   - Threshold: > 85% for 10 minutes
   - Action: SNS notification (consider scaling)

---

## Deployment Automation (Future Enhancement)

### GitHub Actions CI/CD Pipeline

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to App Runner

on:
  push:
    branches:
      - main
      - production

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-2

      - name: Login to Amazon ECR
        id: login-ecr
        uses: aws-actions/amazon-ecr-login@v1

      - name: Build and push Docker image
        env:
          ECR_REGISTRY: 065875799800.dkr.ecr.us-east-2.amazonaws.com
          ECR_REPOSITORY: frank-ai
          IMAGE_TAG: ${{ github.sha }}
        run: |
          docker build --platform linux/amd64 -t $ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG .
          docker tag $ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG $ECR_REGISTRY/$ECR_REPOSITORY:latest
          docker push $ECR_REGISTRY/$ECR_REPOSITORY:$IMAGE_TAG
          docker push $ECR_REGISTRY/$ECR_REPOSITORY:latest

      - name: Trigger App Runner deployment
        run: |
          aws apprunner start-deployment \
            --service-arn arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346 \
            --region us-east-2
```

---

## Security Best Practices

1. **Never commit secrets:**
   - Use environment variables for all sensitive data
   - Add `.env` to `.gitignore`
   - Use AWS Secrets Manager for rotation

2. **Minimize AWS IAM permissions:**
   - Use least-privilege principle
   - Create dedicated deployment user
   - Rotate access keys regularly

3. **Enable App Runner encryption:**
   - Environment variables encrypted at rest
   - Use HTTPS only for service URL

4. **Regular security updates:**
   - Update base Docker image monthly
   - Keep dependencies up to date
   - Monitor CVEs for used packages

5. **Audit logging:**
   - Enable CloudTrail for App Runner API calls
   - Review access logs weekly
   - Set up alerts for suspicious activity

---

## Support and Contacts

**Primary Contact:** <team-lead-name> (Deployment Lead)
**AWS Account ID:** 065875799800
**Region:** us-east-2
**Service Name:** frank-career-counselor

**External Services:**
- Photon Support: https://photon.codes/support
- Supabase Support: https://supabase.com/dashboard/support
- Azure OpenAI: https://portal.azure.com/

**Emergency Rollback:** Follow [Rollback Procedures](#rollback-procedures)

---

## Appendix

### A. Useful Scripts

**scripts/clear_user_data.py** - Clear test user data
```bash
python scripts/clear_user_data.py +1<phone-number>
```

**scripts/find_and_clear_user.py** - Find and clear user by phone pattern
```bash
python scripts/find_and_clear_user.py <phone-number-digits>
```

**scripts/test_photon_connection.py** - Test Photon connectivity
```bash
python scripts/test_photon_connection.py
```

### B. Common Commands Reference

```bash
# Docker
docker build --platform linux/amd64 -t frank-ai:latest .
docker images
docker system prune -a

# AWS ECR
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 065875799800.dkr.ecr.us-east-2.amazonaws.com
aws ecr describe-images --repository-name frank-ai --region us-east-2

# App Runner
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner list-services --region us-east-2
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner describe-service --service-arn <arn> --region us-east-2
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" apprunner start-deployment --service-arn <arn> --region us-east-2

# Logs
"C:\Program Files\Amazon\AWSCLIV2\aws.exe" logs tail /aws/apprunner/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346/application --region us-east-2 --follow

# Git
git status
git log --oneline -10
git tag -a v1.0.0 -m "Release version 1.0.0"
```

### C. Deployment Checklist (Printable)

```
PRE-DEPLOYMENT:
[ ] Tests passing
[ ] Git status clean
[ ] Branch up to date
[ ] Environment variables verified
[ ] Dependencies updated

BUILD:
[ ] ECR login successful
[ ] Docker build completed (linux/amd64)
[ ] Image tagged correctly
[ ] All tags pushed to ECR

DEPLOY:
[ ] Deployment triggered
[ ] Status changed to RUNNING
[ ] Health check passing
[ ] Logs show no errors

VERIFY:
[ ] /health endpoint working
[ ] /docs accessible
[ ] Photon listener connected
[ ] Test iMessage flow successful
[ ] Database writes working

POST-DEPLOYMENT:
[ ] Deployment documented
[ ] Team notified
[ ] Monitoring configured
[ ] No error spikes in logs
```

---

**End of Deployment Guide**
