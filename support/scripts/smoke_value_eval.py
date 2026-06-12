from __future__ import annotations

import asyncio
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


async def _fake_generate_response(self, *args, **kwargs) -> str:  # noqa: ANN001
    # Valid JSON payload expected by value_proof.evaluate_user_value().
    return (
        '{"decision":"ask","response_text":"brought you down to $9.75.\\n\\n'
        'what exact vibe coding workflow do you teach that saves someone 30 minutes this week?",'
        '"mode":"evaluating","weakest_signal":"clarity","signals":{"clarity":2,"credibility":2,"judgment":2},'
        '"question_type":"clarity_sharpening","user_value":{},"intro_fee_cents":975,"price_note":"","confidence":0.6}'
    )


async def main() -> int:
    from app.agents.execution.onboarding.utils import value_proof
    from app.integrations.azure_openai_client import AzureOpenAIClient

    AzureOpenAIClient.generate_response = _fake_generate_response  # type: ignore[method-assign]

    result = await value_proof.evaluate_user_value(
        phone_number="+15551234567",
        user_message="I can teach others how to do vibe coding",
        user_profile={
            "name": "Eric",
            "university": "Upenn",
            "career_interests": ["startups"],
            "intro_fee_cents": 9900,
            "personal_facts": {},
        },
        prior_state={
            "asked_questions": ["what specific skills, experiences, or resources do you have that others would find valuable?"],
            "intro_fee_cents": 9900,
            "turn_history": [{"role": "frank", "content": "what specific skills, experiences, or resources do you have that others would find valuable?"}],
        },
    )

    response_text = str(result.get("response_text") or "").strip().lower()
    bubbles = [b.strip() for b in response_text.split("\n\n") if b.strip()]
    if not (2 <= len(bubbles) <= 3):
        print(f"FAIL: expected 2-3 bubbles, got {len(bubbles)}")
        return 1
    if response_text.count("?") != 1 or not bubbles[-1].endswith("?"):
        print("FAIL: expected exactly one final question mark")
        return 1
    if response_text == "i glitched. resend that.":
        print("FAIL: value eval returned glitch fallback")
        return 1
    if not response_text:
        print("FAIL: empty response_text")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
