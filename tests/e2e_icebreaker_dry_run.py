from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class PhotonStub:
    def __init__(self):
        self.calls: list[tuple] = []

    async def send_message_to_chat(self, chat_guid: str, content: str, **kwargs):
        self.calls.append(("send_message_to_chat", chat_guid, content, kwargs))
        return {"messageId": "stub-message"}

    async def create_poll(self, chat_guid: str, *, title: str, options: list[str], **kwargs):
        self.calls.append(("create_poll", chat_guid, title, options, kwargs))
        return {"data": {"guid": "stub-poll"}}

class SenderStub:
    def __init__(self, photon: PhotonStub):
        self._photon = photon

    async def send_and_record(self, *, chat_guid: str, content: str, **kwargs):
        return await self._photon.send_message_to_chat(chat_guid, content, **kwargs)


async def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run groupchat icebreaker (topic + poll).")
    parser.add_argument("--user-a-name", default="alex")
    parser.add_argument("--user-b-name", default="taylor")
    parser.add_argument(
        "--shared-interests",
        default="product management,machine learning",
        help="Comma-separated interests used to seed the icebreaker.",
    )
    args = parser.parse_args()

    load_dotenv()

    from app.groupchat.features.provisioning import GroupChatService

    service = GroupChatService()
    service.photon = PhotonStub()
    service.sender = SenderStub(service.photon)

    chat_guid = f"iMessage;+;testchat-{uuid.uuid4()}"
    shared_interests = [s.strip() for s in (args.shared_interests or "").split(",") if s.strip()]

    await service._maybe_send_post_intro_icebreaker(
        chat_guid=chat_guid,
        user_a_id=None,
        user_b_id=None,
        user_a_name=args.user_a_name,
        user_b_name=args.user_b_name,
        shared_interests=shared_interests,
        db_record={"user_a_mode": "active", "user_b_mode": "active"},
    )

    print(f"chat_guid: {chat_guid}")
    print(f"calls: {len(service.photon.calls)}")
    for call in service.photon.calls:
        kind = call[0]
        print(f"- {kind}")
        if kind == "send_message_to_chat":
            print(f"  msg_preview: {call[2][:140]}")
        elif kind == "create_poll":
            print(f"  title: {call[2]}")
            print(f"  options: {call[3]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
