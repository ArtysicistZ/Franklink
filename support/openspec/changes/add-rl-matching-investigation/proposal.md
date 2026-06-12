## Why
The current networking match selection relies on a general-purpose LLM (via Azure OpenAI) to choose the best candidate from an embedding-based pool. We want to investigate whether a dedicated, smaller policy model (¡Ö7B) trained with preference optimization can improve match quality, reduce inference cost/latency, and increase controllability for domain-specific networking decisions.

## What Changes
- Define an investigation plan for RL/RLAIF-based match selection, including data collection, privacy controls, and evaluation metrics.
- Specify a labeling pipeline using a top-tier LLM judge (e.g., GPT-class) plus human audits to create preference datasets.
- Compare PPO against alternative preference-optimization methods (e.g., DPO/IPO/KTO/ORPO) and ranking baselines.
- Produce a compute and cost plan for 7B-scale training (LoRA and full fine-tuning scenarios).

## Impact
- Affected specs: `networking-rl-matching` (new capability)
- Likely future touchpoints: `app/agents/execution/networking/utils/adaptive_matcher.py`, `app/agents/tools/networking.py`, data logging in `app/database/`
- Data: new training/evaluation datasets derived from matchmaking history; privacy/PII handling required
