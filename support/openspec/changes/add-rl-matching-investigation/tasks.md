## 1. Investigation
- [ ] 1.1 Document current matching pipeline, inputs/outputs, and decision points
- [ ] 1.2 Define offline evaluation metrics (acceptance rate, response rate, NDCG/MRR, diversity)
- [ ] 1.3 Specify data logging schema for candidate pools, decisions, and outcomes

## 2. Data & Labeling
- [ ] 2.1 Draft privacy/PII redaction requirements for training datasets
- [ ] 2.2 Create preference-labeling guidelines (pairwise ranking rubric)
- [ ] 2.3 Prototype RLAIF labeling with a top-tier LLM judge + human audit sampling

## 3. Modeling Experiments
- [ ] 3.1 Build SFT baseline on historical best-match labels
- [ ] 3.2 Train reward model from pairwise preferences
- [ ] 3.3 Run PPO on 7B policy with KL control vs reference model
- [ ] 3.4 Compare with DPO/IPO/KTO/ORPO alternatives

## 4. Evaluation & Rollout
- [ ] 4.1 Offline evaluation report and error analysis
- [ ] 4.2 Online A/B test plan with guardrails and rollback
- [ ] 4.3 Cost/compute report (cloud GPU, labeling spend)
