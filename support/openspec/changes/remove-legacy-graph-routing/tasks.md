## 1. Implementation
- [x] 1.1 Remove `use_multi_agent` flag from config and environment usage
- [x] 1.2 Update orchestrator and entrypoints to use interaction agent by default
- [x] 1.3 Remove legacy graph runner and graph routing code paths
- [x] 1.4 Delete legacy `app/graphs/**` directories and unused utilities
- [x] 1.5 Update imports and references to point at execution agents
- [x] 1.6 Update docs and support notes to reflect the single routing path

## 2. Validation
- [x] 2.1 Onboarding E2E passes via interaction agent
- [ ] 2.2 Networking flow works with pending confirmations
- [ ] 2.3 Recommendation flow still returns resources
- [ ] 2.4 Update flow requires confirmation before applying changes
