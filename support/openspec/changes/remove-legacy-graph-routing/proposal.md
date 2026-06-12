## Why
The interaction/execution agent architecture is now the primary, validated routing path. Keeping legacy graph routing increases maintenance cost and creates ambiguity over which path is authoritative.

## What Changes
- Remove legacy graph-first routing and related files under `app/graphs/` that are superseded by execution agents.
- Remove the `use_multi_agent` feature flag and default to the interaction/execution agent path.
- Update orchestrator wiring and imports to use the interaction agent directly.
- Clean up documentation and references to legacy routing.

## Impact
- Affected code: `app/orchestrator.py`, `app/config.py`, `app/graphs/**`, `app/agents/**`, `support/docs/**`, tests and scripts that reference the legacy graph runner.
- Behavior: Interaction agent becomes the single routing path post-onboarding; legacy paths are removed.
