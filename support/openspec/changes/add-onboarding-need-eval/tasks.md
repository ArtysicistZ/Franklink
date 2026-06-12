## 1. Implementation
- [x] 1.1 Add `all_value` and `all_demand` columns to `users` (Supabase migration)
- [x] 1.2 Extend user models/state hydration to read/write `all_value` and `all_demand`
- [x] 1.3 Add value summary helper (LLM + fallback) and wire into value accept path
- [x] 1.4 Add needs-eval prompt + evaluator (ask/accept only, min 2 turns, max 4)
- [x] 1.5 Add `collect_need_proof` node and route `needs_eval` in onboarding graph
- [x] 1.6 Update onboarding stage logic (`needs_eval` in state, router, waiting_for, general re-ask)
- [x] 1.7 Add needs summary helper and finalize onboarding on accept (store `all_demand`)
- [x] 1.8 Update docs (support/docs) with final flow details

## 2. Validation
- [ ] 2.1 Manual happy path: value accept -> combined accept+needs question -> needs accept -> complete
- [ ] 2.2 Needs loop min/max turns enforced (force accept on max)
- [ ] 2.3 Summary fallbacks on LLM failure
