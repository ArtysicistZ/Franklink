#!/usr/bin/env python3
"""
Real-world E2E test for onboarding connect workflow (email + calendar).

This script checks:
- user profile status
- next missing field logic
- link generation for email + calendar

Usage:
  python support/scripts/e2e_onboarding_real_workflow.py --user-id <USER_ID>
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


async def main() -> int:
    parser = argparse.ArgumentParser(description="Real-world onboarding connect workflow test.")
    parser.add_argument("--user-id", required=True, help="User ID to test onboarding flow.")
    args = parser.parse_args()

    load_dotenv(override=False)

    from app.database.client import DatabaseClient
    from app.agents.tools.onboarding.tools import get_next_missing_field, initiate_email_connect

    db = DatabaseClient()
    user = await db.get_user_by_id(args.user_id.strip())
    if not user:
        print("user not found")
        return 1

    print("== user profile ==")
    print(
        {
            "user_id": str(user.get("id")),
            "name_present": bool(user.get("name")),
            "university_present": bool(user.get("university")),
            "career_interests_present": bool(user.get("career_interests")),
            "is_onboarded": bool(user.get("is_onboarded")),
        }
    )

    print("== next missing field ==")
    next_field = await get_next_missing_field(user)
    next_data = next_field.data if hasattr(next_field, "data") else next_field.get("data") or {}
    print(next_data)

    print("== connect links ==")
    link_result = await initiate_email_connect(user_id=str(user.get("id")))
    link_data = link_result.data if hasattr(link_result, "data") else link_result.get("data") or {}
    print(
        {
            "success": link_result.success,
            "email_link_present": bool(link_data.get("email_auth_link")),
            "calendar_link_present": bool(link_data.get("calendar_auth_link")),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
