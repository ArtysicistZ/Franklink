# AWS Infrastructure for Franklink

NOTE: App Runner workflows are deprecated in this repo. Current deployments use the ECS testing workflow. See `infrastructure/aws/ecs/README.md`.

Production-ready AWS deployment with version management and rollback capabilities.

---

## 📁 Folder Structure

```
infrastructure/aws/
├── README.md                    # This file - start here
├── scripts/                     # Executable deployment scripts
│   ├── deploy-apprunner-versioned.sh    # ✨ NEW: Versioned deployment
│   ├── deploy-apprunner.sh              # Legacy deployment (still works)
│   ├── rollback-apprunner.sh            # ✨ NEW: Rollback tool
│   └── setup-redis.sh                   # Redis setup
├── docs/                        # Documentation
│   ├── ROLLBACK_QUICKSTART.md          # ⭐ Quick rollback guide
│   ├── VERSION_MANAGEMENT.md           # Complete version guide
│   ├── DEPLOYMENT_CHECKLIST.md         # Deployment checklist
│   └── setup-redis.md                  # Redis setup guide
└── config/                      # Configuration files
    └── apprunner.yaml                  # App Runner config template
```

---

## 🚀 Quick Start

### First Time Deployment (30 minutes)

#### 1. Prerequisites
```bash
# Install AWS CLI
brew install awscli

# Configure AWS credentials
aws configure
# Enter: Access Key, Secret Key, Region (us-east-1), Format (json)
```

#### 2. Set Up Redis
```bash
./scripts/setup-redis.sh production
```
**Important**: Save the `REDIS_URL` from output!

**Current Production Redis**: Already configured at `redis-18970.c8.us-east-1-2.ec2.redns.redis-cloud.com`

#### 3. Deploy Application
```bash
# Recommended: Use versioned deployment
./scripts/deploy-apprunner-versioned.sh production

# Generates version like: v20250108-150530-a1b2c3d
```

#### 4. Create App Runner Service (First time only)

Since service doesn't exist yet, create via [AWS Console](https://console.aws.amazon.com/apprunner):

1. **Source**: Container registry → ECR
2. **Repository**: Select `frank-ai`
3. **Image tag**: `latest`
4. **Service name**: `frank-career-counselor`
5. **Instance size**: 0.25 vCPU, 0.5 GB memory
6. **Port**: `8000`
7. **Environment variables**: Copy from `.env` + add `REDIS_URL`
8. **Auto-deploy**: Enabled
9. Click **Create & Deploy**

#### 5. Update Photon Webhook
```
https://dzmueh4wzy.us-east-2.awsapprunner.com/webhook/photon
```

**Current Production Service**:
- **Service Name**: `frank-career-counselor`
- **Region**: `us-east-2` (Ohio)
- **Account ID**: `065875799800`
- **Service ARN**: `arn:aws:apprunner:us-east-2:065875799800:service/frank-career-counselor/ff3ed77707e14f808f71c625ce38f346`
- **Default Domain**: `https://dzmueh4wzy.us-east-2.awsapprunner.com`

---

## 🔄 Regular Deployments

After initial setup, deployments are simple:

```bash
# Deploy latest code with auto-versioning
./scripts/deploy-apprunner-versioned.sh production

# Takes ~5-7 minutes
# Auto-generates version: v20250108-150530-a1b2c3d
```

---

## ⏪ Rollback (Critical!)

**When something goes wrong, rollback in 3-5 minutes:**

```bash
# Quick rollback to previous version
./scripts/rollback-apprunner.sh production previous

# Or rollback to specific version
./scripts/rollback-apprunner.sh production v20250107-143000-b2c3d4e

# List all available versions
./scripts/rollback-apprunner.sh production list
```

**Read**: [docs/ROLLBACK_QUICKSTART.md](docs/ROLLBACK_QUICKSTART.md) for detailed rollback guide.

---

## 📚 Documentation

### For Daily Use
- **[ROLLBACK_QUICKSTART.md](docs/ROLLBACK_QUICKSTART.md)** - Quick rollback reference (⭐ bookmark this!)
- **[DEPLOYMENT_CHECKLIST.md](docs/DEPLOYMENT_CHECKLIST.md)** - Pre/post deployment checklist

### For Deep Dives
- **[VERSION_MANAGEMENT.md](docs/VERSION_MANAGEMENT.md)** - Complete version management guide
- **[setup-redis.md](docs/setup-redis.md)** - Redis infrastructure setup

---

## 🔧 Available Scripts

### Deployment Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `deploy-apprunner-versioned.sh` | ✨ Deploy with version tracking | `./scripts/deploy-apprunner-versioned.sh production` |
| `deploy-apprunner.sh` | Legacy deployment | `./scripts/deploy-apprunner.sh production` |
| `rollback-apprunner.sh` | ✨ Rollback to previous version | `./scripts/rollback-apprunner.sh production previous` |
| `setup-redis.sh` | Create Redis cluster | `./scripts/setup-redis.sh production` |

### Script Features

**deploy-apprunner-versioned.sh** (Recommended):
- ✅ Auto-generates semantic versions
- ✅ Multiple image tags for flexibility
- ✅ Saves rollback history
- ✅ Checks for uncommitted changes
- ✅ Full deployment monitoring

**rollback-apprunner.sh**:
- ✅ List all available versions
- ✅ Rollback to previous or specific version
- ✅ Confirmation prompts
- ✅ Automatic verification

---

## 📊 Version Naming

### Automatic (Recommended)
```bash
./scripts/deploy-apprunner-versioned.sh production
# Generates: v20250108-150530-a1b2c3d
#            └─ timestamp ─┘└─ git SHA ─┘
```

### Manual (Optional)
```bash
./scripts/deploy-apprunner-versioned.sh production v1.2.3
./scripts/deploy-apprunner-versioned.sh production v1.2.3-hotfix
```

---

## 💰 Estimated Costs

**Monthly**: ~$50-70

| Service | Cost | Notes |
|---------|------|-------|
| App Runner | $25-40 | 0.25 vCPU, 0.5 GB, auto-scaling |
| ElastiCache Redis | $13 | cache.t2.micro |
| Data Transfer | $5-10 | Outbound data |
| ECR Storage | $3-5 | ~20 Docker images |
| CloudWatch Logs | $2-5 | 7-day retention |

### Cost Optimization Tips
- Clean up old ECR images (keep last 20)
- Use staging environment sparingly
- Monitor CloudWatch costs

---

## 🆘 Troubleshooting

### Deployment Failed?

```bash
# Check logs
SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
  --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
  --output text)

aws logs tail /aws/apprunner/frank-career-counselor/${SERVICE_ARN##*/}/application --follow
```

Common issues:
- Missing environment variables → Check App Runner console
- Database connection fails → Verify Supabase credentials
- Redis connection fails → Check REDIS_URL

### Need to Rollback?

```bash
# Immediate rollback
./scripts/rollback-apprunner.sh production previous

# Check docs/ROLLBACK_QUICKSTART.md for details
```

### Emergency Contacts
- **AWS Support**: [Support Console](https://console.aws.amazon.com/support)
- **Deployment Issues**: Check [docs/VERSION_MANAGEMENT.md](docs/VERSION_MANAGEMENT.md)

---

## 🎯 Best Practices

### Before Deployment
- [ ] All tests pass locally
- [ ] Code reviewed and approved
- [ ] Database migrations tested
- [ ] Environment variables updated
- [ ] Note current production version (for rollback)

### After Deployment
- [ ] Test health endpoint: `curl https://<service-url>/health`
- [ ] Send test message to SendBlue
- [ ] Monitor logs for 5-10 minutes
- [ ] Verify webhook works

### If Issues Occur
1. **Don't panic** - rollback is quick (3-5 min)
2. **Check logs** first to understand the issue
3. **Rollback** if critical: `./scripts/rollback-apprunner.sh production previous`
4. **Investigate** root cause offline
5. **Fix and redeploy** when ready

---

## 🔐 Security Notes

- **Never commit** AWS credentials
- **Never commit** `.env` file
- **Use AWS Secrets Manager** for production secrets (recommended)
- **Enable MFA** on AWS account
- **Review IAM permissions** regularly

---

## 📈 Monitoring

### Check Service Status
```bash
SERVICE_ARN=$(aws apprunner list-services --region us-east-1 \
  --query "ServiceSummaryList[?ServiceName=='frank-career-counselor'].ServiceArn" \
  --output text)

aws apprunner describe-service --service-arn $SERVICE_ARN \
  --region us-east-1 --query 'Service.Status'
```

### View Logs
```bash
aws logs tail /aws/apprunner/frank-career-counselor/${SERVICE_ARN##*/}/application --follow
```

### Check Deployment History
```bash
cat ~/.frank-deployments/production-rollback.txt
```

---

## 🆕 What's New

### Version Management System (v1.0)
- ✨ Automatic semantic versioning
- ✨ Easy rollback to any version
- ✨ Deployment history tracking
- ✨ Git integration

**Migration**: If you've been using old `deploy-apprunner.sh`, just start using the new scripts. Old deployments still work!

---

## 📞 Support

- **Quick questions**: Check [docs/ROLLBACK_QUICKSTART.md](docs/ROLLBACK_QUICKSTART.md)
- **Detailed info**: Read [docs/VERSION_MANAGEMENT.md](docs/VERSION_MANAGEMENT.md)
- **Issues**: Review [docs/DEPLOYMENT_CHECKLIST.md](docs/DEPLOYMENT_CHECKLIST.md)
- **AWS Docs**: [App Runner Documentation](https://docs.aws.amazon.com/apprunner/)

---

## 📝 Quick Command Reference

```bash
# Deploy
./scripts/deploy-apprunner-versioned.sh production

# Rollback
./scripts/rollback-apprunner.sh production previous

# List versions
./scripts/rollback-apprunner.sh production list

# Check status
aws apprunner describe-service --service-arn <arn> --query 'Service.Status'

# View logs
aws logs tail /aws/apprunner/frank-career-counselor/<id>/application --follow

# Health check
curl https://<service-url>/health
```

---

**Version**: 2.0.0 (with version management)
**Last Updated**: January 8, 2025
**Maintained by**: Franklink Engineering Team
