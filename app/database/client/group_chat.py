"""Internal database client implementation (group chats).

Unified storage model:
- group_chats: Identity table for ALL group chats (2-person or multi-person)
- group_chat_participants: Membership table for ALL participants

Every group chat should have:
1. One record in group_chats (with chat_guid, member_count, etc.)
2. N records in group_chat_participants (one per participant)
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.database.models import GroupChatMode

logger = logging.getLogger(__name__)


class _GroupChatMethods:
    # ========================================================================
    # Group Chat Identity Methods (group_chats table)
    # ========================================================================

    async def create_group_chat_record(
        self,
        chat_guid: str,
        user_a_id: str,
        user_b_id: str,
        connection_request_id: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a group chat identity record AND participant records.

        This is the unified creation method that ensures both tables are populated.
        For 2-person chats, creates the group_chats record and 2 participant records.

        Args:
            chat_guid: The iMessage/Photon chat GUID
            user_a_id: First participant user ID
            user_b_id: Second participant user ID
            connection_request_id: Optional connection request that created this chat

        Returns:
            The created group_chats record
        """
        try:
            # Create the group chat identity record
            chat_data = {
                "chat_guid": chat_guid,
                "display_name": display_name,
                "member_count": 2,
                "connection_request_id": connection_request_id,
                "created_at": datetime.utcnow().isoformat(),
            }

            result = self.client.table("group_chats").insert(chat_data).execute()
            chat_record = result.data[0]
            logger.info(f"[DB] Created group chat record: {chat_guid}")

            # Create participant records for both users
            for user_id in [user_a_id, user_b_id]:
                try:
                    await self.add_group_chat_participant(
                        chat_guid=chat_guid,
                        user_id=user_id,
                        role="member",
                        mode="active",
                        connection_request_id=connection_request_id,
                    )
                except Exception as e:
                    # Log but don't fail - participant might already exist
                    logger.warning(f"[DB] Could not add participant {user_id}: {e}")

            return chat_record

        except Exception as e:
            logger.error(f"Error creating group chat record: {e}", exc_info=True)
            raise

    async def create_group_chat_identity(
        self,
        chat_guid: str,
        member_count: int = 2,
        connection_request_id: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a group chat identity record (without legacy user_a/user_b fields).

        Use this for multi-person chats where participants are added separately.

        Args:
            chat_guid: The iMessage/Photon chat GUID
            member_count: Initial member count
            connection_request_id: Optional connection request that created this chat

        Returns:
            The created group_chats record
        """
        try:
            chat_data = {
                "chat_guid": chat_guid,
                "display_name": display_name,
                "member_count": member_count,
                "connection_request_id": connection_request_id,
                "created_at": datetime.utcnow().isoformat(),
            }

            result = self.client.table("group_chats").insert(chat_data).execute()
            logger.info(f"[DB] Created group chat identity: {chat_guid} (members={member_count})")
            return result.data[0]

        except Exception as e:
            logger.error(f"Error creating group chat identity: {e}", exc_info=True)
            raise

    async def get_group_chat_by_guid(self, chat_guid: str) -> Optional[Dict[str, Any]]:
        """
        Get a group chat by its GUID.

        Args:
            chat_guid: The iMessage/Photon chat GUID

        Returns:
            Group chat record or None if not found
        """
        try:
            result = self.client.table("group_chats").select("*").eq("chat_guid", chat_guid).execute()
            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error getting group chat: {e}", exc_info=True)
            return None

    async def update_group_chat_member_count(self, chat_guid: str) -> None:
        """
        Recalculate and update the member_count for a group chat.

        Call this after adding or removing participants.

        Args:
            chat_guid: The chat GUID to update
        """
        try:
            # Count participants
            count_result = self.client.table("group_chat_participants").select(
                "id", count="exact"
            ).eq("chat_guid", chat_guid).execute()
            count = count_result.count if count_result.count else 0

            # Update group_chats record
            self.client.table("group_chats").update({
                "member_count": count
            }).eq("chat_guid", chat_guid).execute()

            logger.info(f"[DB] Updated member_count for {chat_guid}: {count}")

        except Exception as e:
            logger.error(f"Error updating member count: {e}", exc_info=True)

    # ========================================================================
    # Participant Methods (group_chat_participants table)
    # ========================================================================

    async def add_group_chat_participant(
        self,
        chat_guid: str,
        user_id: str,
        role: str = "member",
        mode: str = "active",
        connection_request_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Add a participant to a group chat.

        Uses upsert to handle cases where participant already exists.

        Args:
            chat_guid: Chat GUID
            user_id: User ID to add
            role: Participant role (initiator, member)
            mode: Participant mode (active, quiet, muted)
            connection_request_id: Optional associated connection request

        Returns:
            Created/updated participant record or None
        """
        try:
            participant_data = {
                "chat_guid": chat_guid,
                "user_id": user_id,
                "role": role,
                "mode": mode,
                "connection_request_id": connection_request_id,
                "joined_at": datetime.utcnow().isoformat(),
                "created_at": datetime.utcnow().isoformat(),
            }

            # Use upsert to handle existing participants
            result = self.client.table("group_chat_participants").upsert(
                participant_data,
                on_conflict="chat_guid,user_id"
            ).execute()

            logger.info(f"Added participant {user_id} to chat {chat_guid}")
            return result.data[0] if result.data else None

        except Exception as e:
            logger.error(f"Error adding group chat participant: {e}", exc_info=True)
            return None

    async def remove_group_chat_participant(
        self,
        chat_guid: str,
        user_id: str,
    ) -> bool:
        """
        Remove a participant from a group chat.

        Updates member_count after successful removal.

        Args:
            chat_guid: Chat GUID
            user_id: User ID to remove

        Returns:
            True if removed, False otherwise
        """
        try:
            result = (
                self.client.table("group_chat_participants")
                .delete()
                .eq("chat_guid", chat_guid)
                .eq("user_id", user_id)
                .execute()
            )

            if result.data:
                logger.info(f"[DB] Removed participant {user_id[:8]}... from chat {chat_guid[:20]}...")
                # Update member count after removal
                await self.update_group_chat_member_count(chat_guid)
                return True
            return False

        except Exception as e:
            logger.error(f"Error removing participant: {e}", exc_info=True)
            return False

    async def get_group_chat_participants(
        self,
        chat_guid: str,
    ) -> List[Dict[str, Any]]:
        """
        Get all participants for a group chat.

        Args:
            chat_guid: Chat GUID

        Returns:
            List of participant records
        """
        try:
            result = self.client.table("group_chat_participants").select(
                "*"
            ).eq(
                "chat_guid", chat_guid
            ).order("joined_at").execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Error getting group chat participants: {e}", exc_info=True)
            return []

    async def get_user_group_chats(
        self,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Get all group chats a user is participating in.

        Uses the unified participants table to find all chats.

        Args:
            user_id: User ID

        Returns:
            List of group chat records
        """
        try:
            # Get all chat_guids where user is a participant
            participants_result = self.client.table("group_chat_participants").select(
                "chat_guid"
            ).eq("user_id", user_id).execute()

            if not participants_result.data:
                return []

            chat_guids = list(set(p["chat_guid"] for p in participants_result.data))

            # Fetch full group_chats records
            result = self.client.table("group_chats").select("*").in_(
                "chat_guid", chat_guids
            ).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Error getting user group chats: {e}", exc_info=True)
            return []

    async def get_group_chat_for_users(
        self,
        user_a_id: str,
        user_b_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Find an existing group chat that contains BOTH specified users.

        Uses the unified participants table for lookup.

        Args:
            user_a_id: First user ID
            user_b_id: Second user ID

        Returns:
            Group chat record if found, None otherwise
        """
        try:
            # Get chats where user_a is a participant
            result_a = self.client.table("group_chat_participants").select(
                "chat_guid"
            ).eq("user_id", user_a_id).execute()

            if not result_a.data:
                return None

            chat_guids_a = set(p["chat_guid"] for p in result_a.data)

            # Get chats where user_b is a participant
            result_b = self.client.table("group_chat_participants").select(
                "chat_guid"
            ).eq("user_id", user_b_id).execute()

            if not result_b.data:
                return None

            chat_guids_b = set(p["chat_guid"] for p in result_b.data)

            # Find intersection
            shared_guids = chat_guids_a & chat_guids_b

            if shared_guids:
                # Return the first shared chat
                chat_guid = next(iter(shared_guids))
                return await self.get_group_chat_by_guid(chat_guid)

            return None

        except Exception as e:
            logger.error(f"Error getting group chat for users: {e}", exc_info=True)
            return None

    async def update_participant_mode(
        self,
        chat_guid: str,
        user_id: str,
        mode: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Update a participant's mode in a group chat.

        Args:
            chat_guid: Chat GUID
            user_id: User ID
            mode: New mode (active, quiet, muted)

        Returns:
            Updated participant record or None
        """
        try:
            result = self.client.table("group_chat_participants").update({
                "mode": mode,
            }).eq(
                "chat_guid", chat_guid
            ).eq(
                "user_id", user_id
            ).execute()

            if result.data:
                logger.info(f"Updated participant mode: {user_id} in {chat_guid} -> {mode}")
                return result.data[0]

            return None

        except Exception as e:
            logger.error(f"Error updating participant mode: {e}", exc_info=True)
            return None
