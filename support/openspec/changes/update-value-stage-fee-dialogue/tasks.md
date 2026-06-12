## 1. Implementation
- [ ] 1.1 Add/confirm a persisted flag for whether "$0" has been mentioned in value-eval (personal_facts or metadata)
- [ ] 1.2 Update value-eval prompt to require integrated fee+joke+question and remove length caps
- [ ] 1.3 Remove fee-line injection and rely on re-prompt/repair for missing fee or "$0"
- [ ] 1.4 Ensure accept responses mention "$0" only if not yet mentioned; otherwise say fee is waived

## 2. Validation
- [ ] 2.1 Add/adjust tests or scripted checks for: fee always mentioned on ask, "$0" only once, no prompt length caps
- [ ] 2.2 Run targeted lint/compile checks (python -m py_compile ...)
