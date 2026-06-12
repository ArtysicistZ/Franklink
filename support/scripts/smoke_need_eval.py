from __future__ import annotations

import asyncio
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


async def _fake_generate_response(self, *args, **kwargs) -> str:  # noqa: ANN001
    label = str(kwargs.get("trace_label") or "")
    if label == "onboarding_need_eval_gate":
        return (
            '{"decision":"ask","response_text":"inbox is giving career-mode.\\n\\n'
            'you said startups, so let\\u2019s not be vague.\\n\\n'
            'who exactly are you trying to meet and what do you want from them?",'
            '"question_type":"targets","user_need":{"targets":["investors"],"outcomes":["go-to-market advice"]},"confidence":0.7}'
        )
    return (
        '{"decision":"ask","response_text":"cool, investors.\\n\\n'
        'you want gtm advice for a startup, not generic hype.\\n\\n'
        'what stage investors and what specific gtm problem are you solving right now?",'
        '"question_type":"targets","user_need":{"targets":["early stage vcs"],"outcomes":["go-to-market advice"]},"confidence":0.7}'
    )


def _assert_multibubble(text: str) -> None:
    bubbles = [b.strip() for b in str(text or "").split("\n\n") if b.strip()]
    assert 2 <= len(bubbles) <= 3, f"expected 2-3 bubbles, got {len(bubbles)}"
    assert str(text).count("?") == 1, "expected exactly one question mark total"
    assert bubbles[-1].endswith("?"), "expected final bubble to end with question"


async def main() -> int:
    from app.agents.execution.onboarding.utils import need_proof
    from app.integrations.azure_openai_client import AzureOpenAIClient

    AzureOpenAIClient.generate_response = _fake_generate_response  # type: ignore[method-assign]

    user_profile = {
        "name": "Eric",
        "university": "Upenn",
        "career_interests": ["startups"],
        "personal_facts": {
            "email_signals": {
                "status": "ready",
                "summary": "events and recruiting heavy",
                "topics": ["events", "recruiting/interviews"],
                "evidence": ["events: 5", "recruiting: 3"],
                "updated_at": "2026-01-12T00:00:00Z",
            }
        },
    }

    first = await need_proof.build_initial_need_prompt(user_profile=user_profile)
    _assert_multibubble(first)

    result = await need_proof.evaluate_user_need(
        user_message="i wanna meet with investors",
        user_profile=user_profile,
        prior_state={
            "asked_questions": [first],
            "turn_history": [{"role": "frank", "content": first}],
            "user_need": {"targets": ["investors"], "outcomes": ["go-to-market advice"]},
        },
    )
    _assert_multibubble(result.get("response_text"))

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

