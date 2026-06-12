## Why
Value-stage replies feel stitched together and repetitive. We need a single natural response that blends humor, fee, and follow-up, while avoiding repeated "$0" mentions after the first time.

## What Changes
- Generate a single integrated value-stage response that includes fee + joke + follow-up without post-processing fee line injection.
- Always mention the current intro fee on each value-stage ask; mention "$0" only once per value-eval session (and on accept only if not mentioned yet).
- Remove explicit length limits from the value-stage system prompt to allow longer, more detailed replies.
- Keep humor medium-aggressive: roast vague answers, respect specific ones.

## Impact
- Affected specs: onboarding
- Affected code: app/graphs/onboarding/utils/value_proof.py, app/graphs/onboarding/prompts/value_stage_frank.py, state/persistence for zero-fee tracking
