#!/usr/bin/env python3
"""
Real-world E2E test for the calendar workflow.

This script checks:
- Composio availability
- Gmail + calendar connection status
- Optional group chat meeting scheduling (only if --create-event is passed)

Usage:
  python support/scripts/e2e_calendar_real_workflow.py --user-id <USER_ID>
  python support/scripts/e2e_calendar_real_workflow.py --user-id <USER_ID> --create-event
  python support/scripts/e2e_calendar_real_workflow.py --user-id <USER_ID> --chat-guid <GUID> --organizer-user-id <USER_ID> --create-event
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


async def _find_chat_with_emails(db) -> Optional[Tuple[str, str, List[str]]]:
    chats = db.client.table("group_chats").select("*").limit(200).execute().data
    for chat in chats or []:
        chat_guid = chat.get("chat_guid")
        if not chat_guid:
            continue
        participants = await db.get_group_chat_participants(chat_guid)
        if len(participants) < 2:
            continue

        emails: List[str] = []
        organizer_user_id = str(participants[0].get("user_id") or "").strip()
        if not organizer_user_id:
            continue

        for p in participants:
            uid = str(p.get("user_id") or "").strip()
            if not uid:
                continue
            user = await db.get_user_by_id(uid)
            if user and user.get("email"):
                emails.append(str(user.get("email")))

        if len(emails) >= 2:
            return (chat_guid, organizer_user_id, emails)
    return None


async def main() -> int:
    parser = argparse.ArgumentParser(description="Real-world calendar workflow test.")
    parser.add_argument("--user-id", required=True, help="User ID to test calendar auth.")
    parser.add_argument("--chat-guid", default=None, help="Group chat GUID for scheduling.")
    parser.add_argument("--organizer-user-id", default=None, help="Organizer user ID for scheduling.")
    parser.add_argument("--time", default=None, help="Time description for meeting (e.g. 'Feb 15 at 2pm').")
    parser.add_argument("--timezone", default="America/New_York", help="Timezone for parsing.")
    parser.add_argument(
        "--create-event",
        action="store_true",
        help="Actually create a calendar event if connected.",
    )
    args = parser.parse_args()

    load_dotenv(override=False)

    from app.integrations.composio_client import ComposioClient
    from app.database.client import DatabaseClient
    from app.agents.tools.groupchat_maintenance import schedule_meeting

    user_id = args.user_id.strip()
    composio = ComposioClient()

    print("== composio preflight ==")
    print(f"composio_available: {composio.is_available()}")
    email_connected = await composio.verify_gmail_connection(user_id=user_id)
    calendar_connected = await composio.verify_calendar_connection(user_id=user_id)
    print(f"gmail_connected: {email_connected}")
    print(f"calendar_connected: {calendar_connected}")

    if not calendar_connected:
        link = await composio.initiate_calendar_connect(user_id=user_id)
        print(f"calendar_auth_link_generated: {bool(link)}")
        if link:
            print("note: open the link to connect calendar, then rerun with --create-event")
        return 1

    if not args.create_event:
        print("create_event disabled; preflight complete.")
        return 0

    db = DatabaseClient()
    chat_guid = args.chat_guid
    organizer_user_id = args.organizer_user_id
    attendee_emails: List[str] = []

    if not chat_guid or not organizer_user_id:
        found = await _find_chat_with_emails(db)
        if not found:
            print("no suitable group chat with 2+ participant emails found.")
            print("pass --chat-guid and --organizer-user-id to schedule explicitly.")
            return 2
        chat_guid, organizer_user_id, attendee_emails = found

    time_description = args.time
    if not time_description:
        time_description = (datetime.utcnow() + timedelta(days=2)).strftime("%B %d at 3pm")

    print("== scheduling ==")
    result = await schedule_meeting(
        chat_guid=chat_guid,
        time_description=time_description,
        meeting_purpose="E2E Real Workflow Meeting",
        organizer_user_id=organizer_user_id,
        user_timezone=args.timezone,
    )

    data: Dict[str, Any] = result.data if hasattr(result, "data") else result.get("data") or {}
    print(
        {
            "success": result.success,
            "feature_status": data.get("feature_status"),
            "clarification_type": data.get("clarification_type"),
            "event_id": data.get("event_id"),
            "event_link_present": bool(data.get("event_link")),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
