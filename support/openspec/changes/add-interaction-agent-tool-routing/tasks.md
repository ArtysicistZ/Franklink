## 1. Implementation
- [x] 1.1 Add interaction engine runtime (action schema, decision loop, prompt)
- [x] 1.2 Add `pending_confirmation` helpers + universal reply classifier
- [x] 1.3 Update update graph to draft + confirm profile updates
- [x] 1.4 Harden networking confirmations (persist pending_confirmation, repair_explain on meta/unrelated)
- [x] 1.5 Add Composio read-only integration (connect + fetch signals)
- [x] 1.6 Add DM + groupchat interaction engine routing with channel constraints
- [x] 1.7 Add structured logging/trace labels for decisions + confirmations
- [x] 1.8 Update support/docs with the new interaction-engine flow

## 2. Validation
- [ ] 2.1 Profile update draft: draft created, DB unchanged until confirm
- [ ] 2.2 Profile update confirm: DB updates after yes
- [ ] 2.3 Match proposal meta reply: repair_explain + re-ask, then confirm proceeds
- [ ] 2.4 Group chat inbox connect: refused with DM instruction
- [ ] 2.5 Composio read-only: signals stored; no send/draft tools exposed
