# Franklink AWS Architecture

Visual overview of the deployment and version management system.

---

## 🏗️ AWS Infrastructure

```
┌─────────────────────────────────────────────────────────────────┐
│                    AWS App Runner Service                        │
│                  frank-career-counselor                          │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  Container: frank-ai:latest                                │  │
│  │  • CPU: 0.25 vCPU                                          │  │
│  │  • Memory: 0.5 GB                                          │  │
│  │  • Port: 8000                                              │  │
│  │  • Auto-scaling: Enabled                                   │  │
│  │  • Auto-deployment: Enabled (watches ECR :latest tag)     │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                   │
│  Webhook: https://<service-id>.awsapprunner.com/webhook/sendblue│
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Pulls image from
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│            Amazon ECR (Elastic Container Registry)               │
│                  Repository: frank-ai                            │
│                                                                   │
│  Images (each deployment creates multiple tags):                 │
│  ├── v20250108-150530-a1b2c3d  ← Version-specific tag           │
│  ├── latest                     ← Triggers App Runner           │
│  ├── production                 ← Environment tag               │
│  └── production-latest          ← Env + latest combo            │
│                                                                   │
│  Storage: ~20 images × 500MB = ~10GB                            │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Connects to
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│              Amazon ElastiCache (Redis)                          │
│                                                                   │
│  • Instance: cache.t2.micro                                      │
│  • Engine: Redis 7.0                                             │
│  • Used for: Session state, caching                              │
│  • Connection: REDIS_URL env var                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Deployment Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    Developer Workflow                             │
└──────────────────────────────────────────────────────────────────┘

1. CODE CHANGES
   │
   ├─> Git commit
   │   └─> SHA: a1b2c3d
   │
2. RUN DEPLOYMENT SCRIPT
   │
   ├─> ./infrastructure/aws/scripts/deploy-apprunner-versioned.sh production
   │
   ├─> Auto-generate version
   │   └─> v20250108-150530-a1b2c3d
   │       (timestamp + git SHA)
   │
3. BUILD & PUSH
   │
   ├─> Docker build (5-7 min)
   │   └─> Platform: linux/amd64
   │   └─> Labels: VERSION, BUILD_DATE, GIT_COMMIT
   │
   ├─> Tag with multiple tags
   │   ├─> frank-ai:v20250108-150530-a1b2c3d
   │   ├─> frank-ai:latest
   │   ├─> frank-ai:production
   │   └─> frank-ai:production-latest
   │
   └─> Push to ECR
       └─> All 4 tags pushed
   │
4. AUTO-DEPLOYMENT
   │
   ├─> App Runner detects :latest tag change
   │   └─> Triggers automatic deployment
   │
   ├─> Rolling update (3-5 min)
   │   ├─> Start new container
   │   ├─> Health check passes
   │   └─> Stop old container
   │
5. SAVE ROLLBACK INFO
   │
   └─> ~/.frank-deployments/production-rollback.txt
       v20250108-150530-a1b2c3d | ...frank-ai:v20250107... | 2025-01-08T15:05:30Z

Total time: ~8-12 minutes
```

---

## ⏪ Rollback Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    Rollback Workflow                              │
└──────────────────────────────────────────────────────────────────┘

1. TRIGGER ROLLBACK
   │
   ├─> ./infrastructure/aws/scripts/rollback-apprunner.sh production previous
   │
   └─> Script determines target version
       └─> v20250107-143000-b2c3d4e (from rollback history)
   │
2. CONFIRMATION
   │
   ├─> Display current vs target version
   ├─> User confirms: YES
   │
3. RE-TAG IMAGE
   │
   ├─> Get image manifest from ECR
   │   └─> v20250107-143000-b2c3d4e
   │
   ├─> Re-tag as :latest
   │   ├─> latest
   │   ├─> production
   │   └─> production-latest
   │
4. TRIGGER DEPLOYMENT
   │
   ├─> aws apprunner start-deployment
   │   └─> Detects new :latest tag
   │
   ├─> Rolling update (3-5 min)
   │   └─> Deploys old version
   │
5. VERIFICATION
   │
   ├─> Check service status
   ├─> Test health endpoint
   └─> Record rollback in history

Total time: ~3-5 minutes
```

---

## 📊 Version Management System

```
┌──────────────────────────────────────────────────────────────────┐
│              Version Tracking Architecture                        │
└──────────────────────────────────────────────────────────────────┘

LOCAL STORAGE (~/.frank-deployments/)
│
├── production-rollback.txt
│   │
│   ├─> Line format: VERSION | PREVIOUS_IMAGE | TIMESTAMP
│   │
│   └─> Example:
│       v20250108-150530-a1b2c3d | frank-ai:v20250107-143000 | 2025-01-08T15:05:30Z
│       v20250107-143000-b2c3d4e | frank-ai:v20250106-120000 | 2025-01-07T14:30:00Z
│       rollback-to-v20250107... | frank-ai:v20250108-150530 | 2025-01-08T15:10:00Z
│
└── staging-rollback.txt
    └─> Same format for staging environment

ECR IMAGE METADATA
│
├── Image Labels (in Docker image)
│   ├─> VERSION=v20250108-150530-a1b2c3d
│   ├─> BUILD_DATE=2025-01-08T15:05:30Z
│   ├─> GIT_COMMIT=a1b2c3d4e5f6...
│   └─> ENVIRONMENT=production
│
└── Image Tags (in ECR)
    ├─> Specific: v20250108-150530-a1b2c3d
    ├─> Generic: latest, production, production-latest
    └─> Searchable via: aws ecr describe-images
```

---

## 🔍 Monitoring & Observability

```
┌──────────────────────────────────────────────────────────────────┐
│                    Monitoring Stack                               │
└──────────────────────────────────────────────────────────────────┘

CloudWatch Logs
│
├── Log Group: /aws/apprunner/frank-career-counselor/<service-id>/application
│   │
│   ├─> Application logs (stdout/stderr)
│   ├─> Retention: 7 days
│   └─> View: aws logs tail ... --follow
│
└── Log Group: /aws/apprunner/frank-career-counselor/<service-id>/system
    └─> App Runner system logs

CloudWatch Metrics
│
├── Service Metrics (auto-collected)
│   ├─> Requests (count, 2xx, 4xx, 5xx)
│   ├─> Response Time (avg, p50, p90, p99)
│   ├─> CPU Utilization
│   ├─> Memory Utilization
│   └─> Active Instances
│
└── Custom Metrics (optional)
    └─> Application-specific metrics

AWS X-Ray (optional)
│
└── Distributed tracing
    ├─> Request flow
    ├─> Service dependencies
    └─> Performance bottlenecks
```

---

## 🔐 Security Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Security Layers                                │
└──────────────────────────────────────────────────────────────────┘

IAM Roles & Permissions
│
├── App Runner Service Role
│   ├─> Pull images from ECR
│   └─> Write logs to CloudWatch
│
├── ECR Access Role
│   ├─> Push images (deployment)
│   └─> Pull images (App Runner)
│
└── Developer Access
    ├─> Deploy scripts
    ├─> View logs
    └─> Trigger deployments

Network Security
│
├── App Runner
│   ├─> HTTPS only (automatic)
│   ├─> TLS termination (AWS-managed)
│   └─> Public endpoint (webhook requirement)
│
└── ElastiCache Redis
    ├─> VPC-only access
    ├─> Security group rules
    └─> Encryption at rest & in transit

Secrets Management
│
├── Environment Variables (App Runner)
│   ├─> API keys (Supabase, Azure OpenAI, etc.)
│   └─> Not in code/git
│
└── AWS Secrets Manager (recommended upgrade)
    └─> Automatic rotation
```

---

## 💾 Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    Request Flow                                   │
└──────────────────────────────────────────────────────────────────┘

User sends iMessage
    │
    └─> SendBlue API
        │
        └─> Webhook POST
            │
            └─> https://<app-runner>.amazonaws.com/webhook/sendblue
                │
                ├─> App Runner (FastAPI)
                │   │
                │   ├─> Load from Redis (session state)
                │   │
                │   ├─> Query Supabase (user data, facts)
                │   │
                │   ├─> Call Azure OpenAI (LangGraph agent)
                │   │
                │   ├─> Call Zep (memory management)
                │   │
                │   └─> Save to Redis (updated state)
                │
                └─> SendBlue API (send response)
                    │
                    └─> User receives iMessage

Response time target: < 5 seconds
```

---

## 🔄 CI/CD Roadmap (Future)

```
Current: Manual deployment with version management
│
└─> Future: Automated CI/CD

    GitHub Actions (or similar)
    │
    ├─> On push to main
    │   ├─> Run tests
    │   ├─> Build Docker image
    │   ├─> Push to ECR
    │   └─> Deploy to staging
    │
    ├─> On manual trigger
    │   └─> Deploy to production
    │
    └─> On rollback needed
        └─> Automated rollback script
```

---

## 📈 Scalability

```
Current Configuration:
├── CPU: 0.25 vCPU
├── Memory: 0.5 GB
├── Concurrency: 10 requests
└── Auto-scaling: 1-10 instances

Expected Traffic:
├── Users: 100-1000
├── Messages/day: 1,000-10,000
├── Peak requests/sec: 5-10
└── Cost: ~$50-70/month

Scale-up Path:
├── 1 vCPU, 2 GB → ~$150/month
├── 2 vCPU, 4 GB → ~$300/month
└── Consider ECS/EKS for higher scale
```

---

## 🛠️ Troubleshooting Decision Tree

```
Deployment Failed?
    │
    ├─> Check logs
    │   └─> aws logs tail ...
    │       │
    │       ├─> Port binding error? → Check PORT=8000
    │       ├─> Module not found? → Check requirements.txt
    │       ├─> Connection error? → Check env vars
    │       └─> Memory error? → Increase instance size
    │
    ├─> Build failed?
    │   └─> Test locally
    │       └─> docker build -t test .
    │
    └─> Push failed?
        └─> Re-authenticate
            └─> aws ecr get-login-password ...

Service Running But Not Working?
    │
    ├─> Check health endpoint
    │   └─> curl https://<url>/health
    │       │
    │       ├─> 200 OK → Service healthy
    │       └─> Error → Check logs
    │
    ├─> Webhook not working?
    │   └─> Check SendBlue webhook URL
    │       └─> Must end with /webhook/sendblue
    │
    └─> Slow responses?
        └─> Check CloudWatch metrics
            ├─> High CPU → Scale up
            ├─> High memory → Scale up
            └─> Slow queries → Optimize DB

Need to Rollback?
    │
    └─> ./scripts/rollback-apprunner.sh production previous
        └─> Takes 3-5 minutes
```

---

**Created**: January 8, 2025
**Last Updated**: January 8, 2025
