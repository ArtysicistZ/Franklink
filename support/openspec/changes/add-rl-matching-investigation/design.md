## Context
Franklink networking currently uses `AdaptiveMatcher` to generate a candidate pool via multi-signal embeddings and then uses a general-purpose LLM to select the best candidate and rationale. This is effective but expensive, less controllable, and not optimized for domain-specific outcomes like acceptance, response, and follow-through.

Code touchpoints observed:
- Candidate generation and LLM selection: `app/agents/execution/networking/utils/adaptive_matcher.py`
- Request clarity, demand interpretation, and match creation: `app/agents/tools/networking.py`
- Vector search RPCs: `app/database/client/embeddings.py`
- Connection lifecycle: `app/agents/execution/networking/utils/handshake_manager.py`

## Goals / Non-Goals
Goals:
- Improve match quality for the initiator while maintaining mutual benefit.
- Reduce per-decision latency and inference cost vs a large general-purpose LLM.
- Produce auditable, concrete rationales for why a match is correct.
- Enable a fine-tuned 7B policy that can replace or shadow the current LLM selector.

Non-Goals:
- Replacing the embedding retrieval stack in this phase.
- Deploying a new policy without offline evaluation and staged A/B rollout.

## Problem Definition
The core task is a ranking/selection problem: given a candidate pool for a networking request, pick the best person and produce a concise, specific rationale.

Observed success signals to operationalize:
- Initiator accepts the suggested match.
- Target accepts the request.
- Group chat created.
- Follow-through signals (message replies, meeting scheduled).

We must treat this as both a ranking problem (pick best from pool) and a preference alignment problem (optimize for human notions of fit).

## Data And Labeling Strategy
- Log the candidate pool snapshot, initiator profile summary, selected candidate, and outcomes.
- Create preference pairs from outcomes (accepted vs declined) and from historical best matches.
- Use LLM-as-judge to create AI preference labels for large-scale bootstrapping, with periodic human audits.
- Redact PII before export (names, phone numbers, raw email text).

## Methods Landscape
Below is a comprehensive inventory of RL and non-RL approaches applicable to this project, with project-specific pros/cons.

### Non-RL Methods

| Method | What It Is | Pros For Franklink | Cons / Risks | Fit For 7B Fine-Tune |
|---|---|---|---|---|
| Heuristic reranker | Rules on top of embeddings (same school, reciprocal demand/value) | Fast, transparent, no training | Brittle, hard to scale, misses nuanced fit | Low as long-term solution |
| Supervised ranking (LTR) | Pairwise/listwise ranking models (RankNet, LambdaRank, LambdaMART) | Strong for ranking, stable, simple eval metrics (NDCG/MRR) | Requires labeled ranking data; less flexible on nuance | Medium if we train a small reranker model |
| Cross-encoder reranker | BERT-style pairwise scorer on (initiator, candidate) | High accuracy on relevance, can be trained with weak labels | Expensive at inference if candidate pool is large | Medium (can be distilled to 7B) |
| SFT on best-match labels | Train policy to output selected candidate and rationale | Simple pipeline, can leverage logs | Label noise if historical matches are weak | High as baseline |
| Distillation | Train 7B to mimic a strong judge selector | Cheap inference, fast rollout | Teacher bias, needs careful eval | High for initial baseline |

### RLHF / RLAIF Methods

| Method | What It Is | Pros For Franklink | Cons / Risks | Fit For 7B Fine-Tune |
|---|---|---|---|---|
| PPO (RLHF) | Policy optimized with a learned reward model | Industry-proven for alignment | Training instability, reward hacking, heavy compute | Medium-high if dataset is strong |
| TRPO | Alternative policy gradient method | Sometimes more stable than PPO | More complex; less common in LLM alignment | Low-medium |
| RLAIF (Constitutional AI) | AI judge provides preference labels and reward | Scales labels, less human cost | Judge bias, risk of misalignment | Medium-high for bootstrapping |

### Direct Preference Optimization (No RL Loop)

| Method | What It Is | Pros For Franklink | Cons / Risks | Fit For 7B Fine-Tune |
|---|---|---|---|---|
| DPO | Directly optimize preference pairs without RL | Stable, simpler than PPO | Needs paired preference data | High |
| IPO | Treat model as preference classifier, reduce RM dependence | Reduces need for external RM | Newer method; less proven at scale | Medium-high |
| ORPO | Monolithic preference-optimized SFT without reference model | Single-stage, simpler training | Requires careful calibration; newer method | Medium-high |
| KTO | Prospect-theory inspired loss using binary desirable/undesirable labels | Works with binary signals; can be cheaper to label | Newer method; needs calibration to avoid drift | Medium |

### Online Learning / Bandits

| Method | What It Is | Pros For Franklink | Cons / Risks | Fit For 7B Fine-Tune |
|---|---|---|---|---|
| Contextual bandits | Online reranking from real outcomes | Efficient online adaptation | Requires careful exploration safety | Medium for long-term tuning |

## Pros And Cons Summary (Project-Specific)
- PPO: Best if we have reliable reward labels and want strong alignment, but risk of instability and reward hacking.
- DPO: Best first-line alignment method for a ranking task if preference pairs are available.
- ORPO: Simpler pipeline, but still relatively new; worth an experiment after DPO.
- KTO: Good if we can only label binary accept/decline; less data complexity.
- IPO: Promising for self-improvement and reduced RM reliance, but needs validation on our data.
- LTR (RankNet/LambdaRank/LambdaMART): Strong baseline for pure ranking; does not model rationale quality as well.

## Few-Shot Data Quality Findings
Key evidence and takeaways for achieving strong results with small, high-quality datasets:

1) Quality beats volume when the task is narrow and well-defined.
   - LIMA shows strong alignment from ~1,000 carefully curated examples (no RLHF), emphasizing diversity and instruction quality.
   - InstructGPT shows a smaller aligned model beating a much larger baseline, reinforcing the value of clean alignment data.

2) Preference optimization can multiply the value of a small dataset.
   - From a single candidate pool, generate multiple pairwise preferences (selected vs each non-selected).
   - DPO and related methods can learn strong selectors from these pairwise signals without an unstable RL loop.

3) Parameter-efficient fine-tuning reduces overfitting risk in few-shot regimes.
   - LoRA (and QLoRA) allow fast iteration while keeping the base model stable.
   - This is the recommended starting point for a 7B policy model.

4) Synthetic data can be used, but must be aggressively filtered.
   - Self-Instruct and LLM-as-judge pipelines can bootstrap labels.
   - Human audits on a fixed panel are required to avoid judge bias and reward hacking.

5) Few-shot prompting is a necessary baseline.
   - GPT-3 few-shot results show in-context learning can be strong without any fine-tuning.
   - The fine-tuned model must clearly outperform this baseline to justify training.

## Few-Shot Training Playbook (Project-Specific)
Stepwise approach to make few-shot training work for match selection:

1) Build a gold set of 50–200 high-quality examples with hard negatives.
2) Expand into preference pairs from each candidate pool (selected vs non-selected).
3) Train LoRA/QLoRA SFT on the gold set to establish a minimal viable policy.
4) Run DPO on the preference pairs as the first alignment method.
5) Evaluate against few-shot prompt baseline; proceed to PPO only if DPO plateaus.

## Recommended Experimental Path
1. Establish evaluation harness and offline metrics (NDCG/MRR, acceptance proxy, group-created rate).
2. Build SFT baseline on historical selections + distilled LLM-judge labels.
3. Run DPO with pairwise preferences.
4. Run ORPO and KTO if data is limited or we want single-stage training.
5. Run PPO only after we confirm reward model stability and judge calibration.

## Implementation Notes For This Codebase
- Keep candidate pool generation unchanged initially (embedding + Zep + holistic enrichment).
- Replace only the final selector in `AdaptiveMatcher._llm_select_best_match` with a fine-tuned 7B policy or a reranker service.
- Store candidate pools and selection rationales in a new logging table keyed by connection_request_id.
- Add a shadow-mode evaluator that scores with the new policy but does not alter match creation.

## Risks And Mitigations
- Reward hacking: compare AI-judge scores to human audits on a fixed panel.
- Label leakage: strict PII redaction and access controls.
- Drift: schedule periodic re-training with new outcomes; keep rollback path to current LLM selector.

## Open Questions
- Which outcome metric best captures "correct person"? (acceptance vs follow-through)
- What level of human auditing is acceptable per week?
- How large is the candidate pool distribution in production (affects reranker cost)?

## References (Key Papers To Review)
- Proximal Policy Optimization Algorithms (Schulman et al., 2017)
- Trust Region Policy Optimization (Schulman et al., 2015)
- Training Language Models to Follow Instructions with Human Feedback (Ouyang et al., 2022)
- Training a Helpful and Harmless Assistant with RLHF (Bai et al., 2022)
- Constitutional AI: Harmlessness from AI Feedback (Bai et al., 2022)
- Direct Preference Optimization (Rafailov et al., 2023)
- ORPO: Monolithic Preference Optimization without Reference Model (Hong et al., 2024)
- KTO: Model Alignment as Prospect Theoretic Optimization (Ethayarajh et al., 2024)
- IPO: Your Language Model is Secretly a Preference Classifier (Garg et al., 2025)
- RankNet (Burges et al., 2005)
- LambdaRank (Burges et al., 2006)
- From RankNet to LambdaRank to LambdaMART (Burges, 2010)
- A Contextual-Bandit Approach to Personalized News Article Recommendation (Li et al., 2010)
- LIMA: Less Is More for Alignment (Zhou et al., 2023)
- LoRA: Low-Rank Adaptation of Large Language Models (Hu et al., 2021)
- QLoRA: Efficient Finetuning of Quantized LLMs (Dettmers et al., 2023)
- Self-Instruct: Aligning LMs with Self-Generated Instructions (Wang et al., 2022)
- Language Models are Few-Shot Learners (Brown et al., 2020)
