"""Main orchestrator agent for handling conversations via InteractionAgent."""

import asyncio
import logging
from typing import Any, Dict, Optional

from app.config import settings
from app.context import set_llm_context, clear_llm_context
from app.database.client import DatabaseClient
from app.integrations.photon_client import PhotonClient
from app.integrations.azure_openai_client import AzureOpenAIClient
from app.utils.message_chunker import chunk_message
from app.services.cancellation import CancellationToken

logger = logging.getLogger(__name__)

class MainOrchestrator:
    """
    Main orchestrator agent that coordinates conversation handling.

    This class serves as the entry point for all messages, routing them through
    the InteractionAgent and handling response delivery.
    """

    def __init__(self):
        """Initialize the orchestrator with required clients."""
        self.db = DatabaseClient()
        self.photon = PhotonClient(
            server_url=settings.photon_server_url,
            default_number=settings.photon_default_number
        )
        self.openai = AzureOpenAIClient()

        # Initialize interaction agent (lazy-loaded on first message)
        self.interaction_agent = None

    async def handle_message(
        self,
        webhook: Any,
        cancel_token: Optional[CancellationToken] = None,
    ) -> None:
        """
        Handle an incoming message from Photon webhook.

        Args:
            webhook: The webhook data from Photon
            cancel_token: Optional cancellation token for graceful cancellation
        """
        import os
        pid = os.getpid()
        logger.info(f"[ORCHESTRATOR] Handling message pid={pid} from={webhook.from_number} to={webhook.to_number}")

        # Check for cancellation before starting
        if cancel_token and cancel_token.is_cancelled():
            logger.info("[ORCHESTRATOR] Message cancelled before processing started")
            return

        user = None
        try:
            # 1. Get or create user profile
            logger.info(f"[ORCHESTRATOR] Getting/creating user for {webhook.from_number}")
            user = await self.db.get_or_create_user(webhook.from_number)
            logger.info(f"[ORCHESTRATOR] Processing message for user {user['id']}")

            # Set LLM context for usage tracking
            chat_guid = getattr(webhook, "chat_guid", None)
            set_llm_context(
                user_id=str(user['id']),
                chat_guid=chat_guid if chat_guid and ";+;" in str(chat_guid) else None,
            )

            # Store the incoming user message in conversation history
            try:
                await self.db.store_message(
                    user_id=user['id'],
                    content=webhook.content,
                    message_type="user",
                    metadata={
                        "message_id": getattr(webhook, "message_id", None),
                        "chat_guid": getattr(webhook, "chat_guid", None),
                    }
                )
                logger.debug(f"[ORCHESTRATOR] Stored user message for {user['id']}")
            except Exception as e:
                logger.warning(f"[ORCHESTRATOR] Failed to store user message: {e}")

            # Handle location share: detect, correlate with Find My, link handle
            if getattr(webhook, "is_location_share", False):
                chat_guid = getattr(webhook, "chat_guid", None)
                await self._handle_location_share(webhook, user, chat_guid)
                return

            # Handle unsupported media types (images, videos, polls, stickers, audio, etc.)
            # Detect: has media_url but no meaningful text (empty or single character like U+FFFC)
            media_url = getattr(webhook, "media_url", None)
            content = webhook.content or ""
            is_media_message = media_url and len(content.strip()) <= 1
            if is_media_message:
                # Check if user is in share_to_complete stage - screenshots are expected there
                onboarding_stage = user.get("onboarding_stage")
                personal_facts = user.get("personal_facts") or {}
                pf_stage = personal_facts.get("onboarding_stage") if isinstance(personal_facts, dict) else None
                effective_stage = pf_stage or onboarding_stage

                if effective_stage == "share_to_complete":
                    # Allow through - the onboarding executor handles screenshot validation
                    logger.info("[ORCHESTRATOR] Allowing media for share_to_complete stage, media_url=%s", media_url)
                    # Continue processing (don't return)
                else:
                    # Not expecting media - silently ignore
                    logger.info("[ORCHESTRATOR] Ignoring unsupported media (silent), media_url=%s", media_url)
                    return

            # Check for pending location link confirmation
            from app.utils.redis_client import redis_client
            pending_key = f"location_link_pending:{user['id']}"
            pending_data = redis_client.get_cached(pending_key)
            if pending_data and webhook.content:
                await self._handle_pending_location_link(webhook, user, pending_data, pending_key)
                return

            # Group chat messages are handled separately (never DM-reply to group messages).
            chat_guid = getattr(webhook, "chat_guid", None)
            if chat_guid and (";+;" in str(chat_guid) or str(chat_guid).startswith("chat")):
                try:
                    from app.groupchat.runtime.router import GroupChatRouter

                    router = GroupChatRouter(
                        db=self.db,
                        photon=self.photon,
                        openai=self.openai,
                    )
                    handled = await router.handle_inbound(webhook, sender_user_id=str(user.get("id") or ""))
                    logger.info(
                        "[ORCHESTRATOR] Group chat routed handled=%s chat_guid=%s msg_id=%s sender_user_id=%s",
                        handled,
                        str(chat_guid)[:40],
                        str(getattr(webhook, "message_id", "") or "")[:18],
                        str(user.get("id") or "")[:8],
                    )
                except Exception as e:
                    logger.error(f"[ORCHESTRATOR] Group chat handler failed: {e}", exc_info=True)
                return

            # 2. Process via InteractionAgent
            logger.info("[ORCHESTRATOR] Processing message via InteractionAgent")

            if self.interaction_agent is None:
                from app.agents.interaction import get_interaction_agent
                self.interaction_agent = get_interaction_agent(
                    db=self.db,
                    photon=self.photon,
                    openai=self.openai,
                )
                logger.info("[ORCHESTRATOR] InteractionAgent initialized")

            # Mark chat as read before processing
            try:
                await self.photon.mark_chat_read(chat_guid)
            except Exception as e:
                logger.debug(f"[ORCHESTRATOR] Failed to mark chat as read: {e}")

            # Show typing indicator while processing (typically 3-4 seconds)
            try:
                await self.photon.start_typing(webhook.from_number, chat_guid=chat_guid)
                logger.info(f"[ORCHESTRATOR] Started typing indicator for {webhook.from_number}")
            except Exception as e:
                logger.warning(f"[ORCHESTRATOR] Failed to start typing indicator: {e}")

            result = None
            try:
                # Check for cancellation before processing
                if cancel_token and cancel_token.is_cancelled():
                    logger.info("[ORCHESTRATOR] Message cancelled before InteractionAgent processing")
                    return

                webhook_data = {
                    "message_id": getattr(webhook, "message_id", None),
                    "timestamp": getattr(webhook, "timestamp", None),
                    "media_url": getattr(webhook, "media_url", None),
                    "chat_guid": chat_guid,
                }

                # Filter user profile to only include necessary fields to reduce context size
                filtered_user = {
                    "id": user.get("id"),
                    "phone_number": user.get("phone_number"),
                    "name": user.get("name"),
                    "email": user.get("email"),
                    "university": user.get("university"),
                    "location": user.get("location"),
                    "major": user.get("major"),
                    "year": user.get("year"),
                    "career_interests": user.get("career_interests"),
                    "networking_clarification": user.get("networking_clarification"),
                    "is_onboarded": user.get("is_onboarded"),
                    # Networking-required fields
                    "latest_demand": user.get("latest_demand"),
                    "all_demand": user.get("all_demand"),
                    "all_value": user.get("all_value"),
                    # Onboarding-required fields (stores email_connect status, eval states)
                    "personal_facts": user.get("personal_facts"),
                    "onboarding_stage": user.get("onboarding_stage"),
                    "linkedin_url": user.get("linkedin_url"),
                    "demand_history": user.get("demand_history"),
                    "value_history": user.get("value_history"),
                    "intro_fee_cents": user.get("intro_fee_cents"),
                    "needs": user.get("needs"),
                    "career_goals": user.get("career_goals"),
                    "networking_limitation": user.get("networking_limitation"),
                    # Skills for complementary matching
                    "seeking_skills": user.get("seeking_skills"),
                    "offering_skills": user.get("offering_skills"),
                    "seeking_relationship_types": user.get("seeking_relationship_types"),
                    "offering_relationship_types": user.get("offering_relationship_types"),
                }

                result = await self.interaction_agent.process_message(
                    phone_number=webhook.from_number,
                    message_content=webhook.content,
                    user=filtered_user,
                    webhook_data=webhook_data,
                    cancel_token=cancel_token,
                )
            finally:
                # Always stop typing indicator when processing completes
                try:
                    await self.photon.stop_typing(webhook.from_number, chat_guid=chat_guid)
                    logger.info(f"[ORCHESTRATOR] Stopped typing indicator for {webhook.from_number}")
                except Exception as e:
                    logger.debug(f"[ORCHESTRATOR] Failed to stop typing indicator: {e}")

            # Handle response (check for cancellation first)
            if result is None or result.get("cancelled"):
                logger.info("[ORCHESTRATOR] Processing was cancelled - not sending response")
                return

            if result["success"]:
                responses = result.get("responses")
                if isinstance(responses, list) and responses:
                    inbound_guid = str(getattr(webhook, "message_id", "") or "").strip()
                    legacy_outbound_ok = None
                    for idx, item in enumerate(responses):
                        # Add 0.3s delay between onboarding messages (not before first)
                        if idx > 0 and item.get("intent") == "onboarding":
                            await asyncio.sleep(0.3)

                        response_text = str(item.get("response_text") or "").strip()
                        task_intent = item.get("intent")
                        task_name = item.get("task")

                        if response_text:
                            # Don't chunk URLs - they break when split. Use rich links for better preview.
                            if response_text.startswith("http://") or response_text.startswith("https://"):
                                await self.photon.send_message(to_number=webhook.from_number, content=response_text, rich_link=True)
                            else:
                                await self._send_message_chunks(
                                    phone_number=webhook.from_number,
                                    response=response_text,
                                    user_id=user['id']
                                )

                            await self.db.store_message(
                                user_id=user['id'],
                                content=response_text,
                                message_type="bot",
                                metadata={
                                    "intent": task_intent,
                                    "task": task_name,
                                    "task_index": idx,
                                }
                            )

                            # Sync conversation to Zep knowledge graph (background task)
                            asyncio.create_task(self._sync_conversation_to_zep(
                                user_id=user['id'],
                                user_message=webhook.content,
                                bot_response=response_text,
                                user_name=user.get('name'),
                                intent=task_intent or task_name,
                            ))

                        resource_urls = item.get("resource_urls", []) or []
                        if resource_urls:
                            urls_only = [r.get("url", "") for r in resource_urls if r.get("url")]
                            if urls_only:
                                url_message = "\n".join(urls_only)
                                await self._send_message_chunks(
                                    phone_number=webhook.from_number,
                                    response=url_message,
                                    user_id=user['id']
                                )
                                await self.db.store_message(
                                    user_id=user['id'],
                                    content=url_message,
                                    message_type="bot",
                                    metadata={
                                        "intent": task_intent,
                                        "task": task_name,
                                        "message_part": "urls",
                                        "task_index": idx,
                                    }
                                )

                        outbound = item.get("outbound_messages", []) or []
                        if isinstance(outbound, list) and outbound:
                            redis_client = None
                            if inbound_guid:
                                try:
                                    from app.utils.redis_client import redis_client as redis_client
                                except Exception:
                                    redis_client = None

                            if inbound_guid and redis_client and legacy_outbound_ok is None:
                                try:
                                    legacy_outbound_ok = redis_client.check_idempotency(
                                        f"outbound_messages:v1:{inbound_guid}",
                                        ttl=60 * 60 * 24 * 30
                                    )
                                except Exception:
                                    legacy_outbound_ok = True
                            should_send_extras = True
                            if inbound_guid:
                                if legacy_outbound_ok is False:
                                    should_send_extras = False
                                elif redis_client:
                                    try:
                                        should_send_extras = redis_client.check_idempotency(
                                            f"outbound_messages:v2:{inbound_guid}:{idx}",
                                            ttl=60 * 60 * 24 * 30
                                        )
                                    except Exception:
                                        should_send_extras = True

                            if should_send_extras:
                                for i, text in enumerate(outbound[:3]):
                                    msg = str(text or "").strip()
                                    if not msg:
                                        continue
                                    # Don't chunk URLs - they break when split
                                    if msg.startswith("http://") or msg.startswith("https://"):
                                        await self.photon.send_message(to_number=webhook.from_number, content=msg)
                                    else:
                                        await self._send_message_chunks(
                                            phone_number=webhook.from_number,
                                            response=msg,
                                            user_id=user['id']
                                        )
                                    await self.db.store_message(
                                        user_id=user['id'],
                                        content=msg,
                                        message_type="bot",
                                        metadata={
                                            "intent": task_intent,
                                            "task": task_name,
                                            "message_part": f"outbound_{i}",
                                            "task_index": idx,
                                        }
                                    )

                    # Maybe send a lightweight reaction based on the last task
                    last = responses[-1] if responses else {}
                    asyncio.create_task(self._maybe_send_reaction(
                        phone_number=webhook.from_number,
                        chat_guid=getattr(webhook, "chat_guid", None),
                        message_guid=getattr(webhook, "message_id", None),
                        message_content=webhook.content,
                        context={
                            "intent": last.get("intent"),
                            "task": last.get("task"),
                            "onboarding_stage": (result.get("state", {}) or {}).get("user_profile", {}).get("onboarding_stage"),
                        },
                    ))

                    logger.info(
                        "[ORCHESTRATOR] Multi-task processing complete responses=%s",
                        len(responses),
                    )
                    return

                # Fallback: single response_text without responses list
                if result.get("response_text"):
                    response_text = result["response_text"]
                    # Don't chunk URLs - they break when split. Use rich links for better preview.
                    if response_text.startswith("http://") or response_text.startswith("https://"):
                        await self.photon.send_message(to_number=webhook.from_number, content=response_text, rich_link=True)
                    else:
                        await self._send_message_chunks(
                            phone_number=webhook.from_number,
                            response=response_text,
                            user_id=user['id']
                        )

                    await self.db.store_message(
                        user_id=user['id'],
                        content=response_text,
                        message_type="bot",
                        metadata={
                            "intent": result.get("intent"),
                            "task": result.get("intent"),
                        }
                    )

                    logger.info(f"[ORCHESTRATOR] Processing complete - Task: {result.get('intent')}")
                    return

                # No response generated - send a fallback
                logger.warning("[ORCHESTRATOR] Success but no response generated, sending fallback")
                fallback_response = "hey! what can i help you with?"
                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response=fallback_response,
                    user_id=user['id']
                )
                await self.db.store_message(
                    user_id=user['id'],
                    content=fallback_response,
                    message_type="bot",
                    metadata={
                        "intent": result.get("intent"),
                        "fallback": True
                    }
                )
                return

            else:
                # Processing failed - send simple error message
                logger.error(f"[ORCHESTRATOR] Processing failed: {result.get('error', 'Unknown error')}")

                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response="Sorry, I'm having technical difficulties right now. Please try again in a moment!",
                    user_id=user['id']
                )
                return

        except asyncio.CancelledError:
            # Message processing was cancelled (likely due to new message arriving)
            # IMPORTANT: Must re-raise so the coalescer knows to restore in-flight messages
            logger.info("[ORCHESTRATOR] Message processing cancelled - new message received")
            raise
        except SystemExit as e:
            logger.critical(f"[CRASH DETECT] SystemExit in orchestrator - PID={pid}: {e}", exc_info=True)
            raise  # Re-raise to preserve exit behavior
        except KeyboardInterrupt as e:
            logger.critical(f"[CRASH DETECT] KeyboardInterrupt in orchestrator - PID={pid}: {e}", exc_info=True)
            raise  # Re-raise to preserve interrupt behavior
        except Exception as e:
            logger.error(f"[CRASH DETECT] Exception in orchestrator - PID={pid}: {e}", exc_info=True)
            logger.error(f"[ORCHESTRATOR] Critical error in orchestrator: {str(e)}", exc_info=True)
            # Send a fallback error message to user
            try:
                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response="Sorry, something went wrong. Please try again later!",
                    user_id=user.get('id') if user else None
                )
            except Exception as send_error:
                logger.error(f"[ORCHESTRATOR] Failed to send error message: {send_error}", exc_info=True)
        finally:
            # Clear LLM context to prevent leaking between requests
            clear_llm_context()

    async def _send_message_chunks(
        self,
        phone_number: str,
        response: str,
        user_id: str | None,
        max_length: int = 280
    ) -> None:
        """
        Send a response message in chunks if it's too long.

        First checks for natural bubble separators (\n\n), then falls back to smart chunking.

        Args:
            phone_number: Recipient's phone number or email (Apple ID)
            response: The full response text to send
            user_id: User's UUID for logging (optional)
            max_length: Maximum characters per chunk (default 280)
        """
        try:
            # Check if response has natural bubble separators (\n\n)
            if "\n\n" in response:
                # Split on double newlines to get natural bubbles
                natural_bubbles = [bubble.strip() for bubble in response.split("\n\n") if bubble.strip()]

                if natural_bubbles:
                    logger.info(f"[ORCHESTRATOR] Sending {len(natural_bubbles)} natural bubbles to {phone_number}")
                    results = await self.photon.send_chunked_messages(
                        to_number=phone_number,
                        message_chunks=natural_bubbles,
                        delay_range=(0.3, 0.3),  # 0.3s delay between bubbles
                        show_typing=True
                    )
                    failed = [r for r in results if not r.get("success")]
                    if failed:
                        logger.error(f"[ORCHESTRATOR] Failed to send {len(failed)}/{len(natural_bubbles)} bubbles to {phone_number}")
                    else:
                        logger.info(f"[ORCHESTRATOR] Successfully sent all bubbles to {phone_number}")
                    return

            # Fallback: original chunking logic for single-bubble or too-long messages
            # If short enough, send as a single message
            if len(response) <= max_length:
                await self.photon.send_message(to_number=phone_number, content=response)
                return

            # For iMessage, keep responses to at most 2 bubbles (best-effort) to stay natural.
            chunks = chunk_message(response, max_length=max_length, max_chunks=2)
            if not chunks:
                logger.warning(f"[ORCHESTRATOR] No chunks generated for message to {phone_number}")
                return

            logger.info(f"[ORCHESTRATOR] Sending message in {len(chunks)} chunk(s) to {phone_number}")
            results = await self.photon.send_chunked_messages(
                to_number=phone_number,
                message_chunks=chunks,
                delay_range=(0.5, 1.0),
                show_typing=True
            )
            failed = [r for r in results if not r.get("success")]
            if failed:
                logger.error(f"[ORCHESTRATOR] Failed to send {len(failed)}/{len(chunks)} chunks to {phone_number}")
            else:
                logger.info(f"[ORCHESTRATOR] Successfully sent all chunks to {phone_number}")
        except Exception as e:
            logger.error(f"[ORCHESTRATOR] Error in _send_message_chunks: {e}", exc_info=True)
            try:
                await self.photon.send_message(
                    to_number=phone_number,
                    content="Sorry, I had trouble sending that message. Please try again!"
                )
            except Exception:
                pass

    async def _maybe_send_reaction(
        self,
        phone_number: str,
        message_guid: str | None,
        message_content: str,
        chat_guid: str | None = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send an optional tapback reaction to the user's message (LLM-decided).
        """
        if not message_guid:
            return

        try:
            from app.reactions.service import ReactionService

            await ReactionService(photon=self.photon, openai=self.openai).maybe_send_reaction(
                to_number=phone_number,
                message_guid=message_guid,
                message_content=message_content,
                chat_guid=chat_guid,
                context=context or {},
            )
        except Exception as e:
            logger.debug(f"[REACTION] Failed to send reaction: {e}")

    async def _sync_conversation_to_zep(
        self,
        user_id: str,
        user_message: str,
        bot_response: str,
        user_name: Optional[str] = None,
        intent: Optional[str] = None,
    ) -> None:
        """
        Sync a conversation exchange to Zep's knowledge graph.

        Runs as a background task so it doesn't slow down response delivery.
        Enriches Zep with conversation context for better understanding.
        """
        try:
            from app.agents.tools.conversation_zep_sync import sync_conversation_to_zep

            result = await sync_conversation_to_zep(
                user_id=user_id,
                user_message=user_message,
                bot_response=bot_response,
                user_name=user_name,
                intent=intent,
            )

            if result.get("synced"):
                logger.debug(
                    "[ORCHESTRATOR] Synced conversation to Zep user=%s intent=%s",
                    user_id[:8] if user_id else "unknown",
                    intent or "unknown",
                )
        except Exception as e:
            # Don't let Zep sync failures affect the main flow
            logger.debug(f"[ORCHESTRATOR] Zep conversation sync failed: {e}")

    async def _generate_witty_location_response(
        self,
        latitude: float,
        longitude: float,
        short_address: str,
        user_name: str,
    ) -> str:
        """Generate a witty response based on the user's location and nearby places.

        Uses Google Places API to find nearby spots and creates a fun, contextual response.

        Args:
            latitude: User's latitude
            longitude: User's longitude
            short_address: Short address string (e.g., "Philadelphia, PA")
            user_name: User's first name for personalization

        Returns:
            A witty response string
        """
        import os

        # Get nearby places for context
        nearby_cafes = []
        nearby_bars = []
        nearby_restaurants = []

        api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        if api_key and latitude and longitude:
            try:
                from app.integrations.google_places_client import GooglePlacesClient

                async with GooglePlacesClient(api_key) as client:
                    places = await client.get_nearby_places(latitude, longitude, max_results=10)

                    from app.utils.location_context import _categorize_place

                    for p in places:
                        name = p.get("name", "")
                        if not name:
                            continue
                        category = _categorize_place(p)
                        if category == "cafe" and len(nearby_cafes) < 2:
                            nearby_cafes.append(name)
                        elif category == "bar" and len(nearby_bars) < 2:
                            nearby_bars.append(name)
                        elif category == "restaurant" and len(nearby_restaurants) < 2:
                            nearby_restaurants.append(name)

            except Exception as e:
                logger.debug(f"[LOCATION_LINK] Failed to fetch nearby places: {e}")

        # Build witty response based on what we found
        first_name = user_name.split()[0] if user_name else "friend"
        area = short_address or "your area"

        # Generate contextual witty responses
        if nearby_cafes and nearby_bars:
            cafe = nearby_cafes[0]
            bar = nearby_bars[0]
            return f"locked in 📍 i see you near {area}. if you need a caffeine fix, {cafe} is close by. and if networking calls for happy hour vibes, {bar} isn't far either 🍻"
        elif nearby_cafes:
            cafe = nearby_cafes[0]
            return f"got you 📍 {area} huh? nice. i see {cafe} nearby - solid spot if you ever need to meet someone for coffee ☕"
        elif nearby_restaurants:
            restaurant = nearby_restaurants[0]
            return f"locked in 📍 you're in {area}. {restaurant} is right there if you need a spot for a networking lunch 🍽️"
        elif nearby_bars:
            bar = nearby_bars[0]
            return f"📍 {area} - noted! {bar} is nearby for those after-work networking drinks when the time comes"
        else:
            # Fallback without places
            return f"got your location 📍 {area}. now i can help connect you with people nearby!"

    async def _handle_location_share(self, webhook: Any, user: Dict, chat_guid: str | None) -> None:
        """Handle an iMessage location share: correlate with Find My and link handle.

        1. Mark chat as read immediately
        2. Trigger immediate worker run to fetch fresh Find My data
        3. Find unrecognized handles with recent timestamps
        4. Auto-link if one candidate, ask if multiple
        5. Send witty response based on location context
        """
        import time
        from app.utils.location_service import _normalize_handle
        from app.utils.redis_client import redis_client

        user_id = str(user.get("id", ""))
        phone = webhook.from_number
        logger.info("[LOCATION_LINK] Location share detected from user=%s phone=%s", user_id[:8], phone)

        # Mark chat as read immediately to show we received the location
        try:
            if chat_guid:
                await self.photon.mark_chat_read(chat_guid)
                logger.debug("[LOCATION_LINK] Marked chat as read: %s", chat_guid)
        except Exception as e:
            logger.warning("[LOCATION_LINK] Failed to mark chat as read: %s", e)

        try:
            # 1. Trigger immediate worker run for fresh Find My data
            from app.proactive.location.service import LocationUpdateService
            service = LocationUpdateService(db=self.db, photon=self.photon)
            updated = await service.run_once()
            logger.info("[LOCATION_LINK] Immediate worker run updated %d locations", updated)

            # 2. Check if user already has a stored location
            existing_loc = await self.db.get_user_location(user_id)
            if existing_loc:
                logger.info("[LOCATION_LINK] User %s already has location, updating with fresh data", user_id[:8])
                # Generate witty response based on their current location
                user_name = user.get("name", "")
                witty_response = await self._generate_witty_location_response(
                    latitude=existing_loc.get("latitude", 0),
                    longitude=existing_loc.get("longitude", 0),
                    short_address=existing_loc.get("short_address", ""),
                    user_name=user_name,
                )
                await self.photon.send_message(
                    to_number=phone,
                    content=witty_response,
                    chat_guid=chat_guid,
                )
                return

            # 3. Build set of all known handles (phone + email + linked)
            known_handles = set()
            try:
                users_result = (
                    self.db.client.table("users")
                    .select("phone_number, email")
                    .eq("is_onboarded", True)
                    .execute()
                )
                for u in (users_result.data or []):
                    p = _normalize_handle(u.get("phone_number", ""))
                    e = _normalize_handle(u.get("email", ""))
                    if p:
                        known_handles.add(p)
                    if e:
                        known_handles.add(e)
            except Exception as e:
                logger.warning("[LOCATION_LINK] Failed to fetch known handles: %s", e)

            # Also add already-linked handles
            try:
                all_links = await self.db.get_all_linked_handles()
                for link in all_links:
                    h = _normalize_handle(link.get("handle", ""))
                    if h:
                        known_handles.add(h)
            except Exception as e:
                logger.warning("[LOCATION_LINK] Failed to fetch linked handles: %s", e)

            # 4. Fetch FRESH Find My locations directly (bypass cache since worker
            #    already fetched fresh data but didn't update the cache)
            locations = await self.photon.refresh_find_my_friends()
            if not locations:
                logger.info("[LOCATION_LINK] No Find My locations available")
                await self.photon.send_message(
                    to_number=phone,
                    content="thanks for sharing your location! i couldn't find any Find My data right now though. try again later?",
                    chat_guid=chat_guid,
                )
                return

            # 5. FIRST: Try to match the user's own phone number or email in Find My
            user_phone = _normalize_handle(user.get("phone_number", ""))
            user_email = _normalize_handle(user.get("email", ""))
            user_handles = {h for h in [user_phone, user_email] if h}

            # Debug: log all Find My handles
            findmy_handles = [_normalize_handle(loc.get("handle", "")) for loc in locations]
            logger.info("[LOCATION_LINK] Find My returned %d locations with handles: %s", len(locations), findmy_handles)
            logger.info("[LOCATION_LINK] Looking for user handles: %s", user_handles)

            matched_own_loc = None
            matched_handle_no_coords = None  # Track if handle matches but coords are invalid
            for loc in locations:
                handle = _normalize_handle(loc.get("handle", ""))
                if not handle:
                    continue
                # Check if this handle matches the user's phone or email
                handle_matches = handle in user_handles
                # Also check last 10 digits for phone number matching
                if not handle_matches and user_phone and "@" not in handle:
                    handle_digits = "".join(c for c in handle if c.isdigit())[-10:]
                    user_digits = "".join(c for c in user_phone if c.isdigit())[-10:]
                    handle_matches = handle_digits and user_digits and handle_digits == user_digits

                if handle_matches:
                    coords = loc.get("coordinates", [])
                    logger.info("[LOCATION_LINK] Handle %s matches! coords=%s", handle, coords)
                    if coords and len(coords) >= 2 and not (coords[0] == 0 and coords[1] == 0):
                        matched_own_loc = loc
                        logger.info("[LOCATION_LINK] Found user's own location via handle=%s", handle)
                        break
                    else:
                        # Handle matches but coords are invalid - Find My hasn't refreshed yet
                        matched_handle_no_coords = handle
                        logger.warning("[LOCATION_LINK] Handle %s matched but coords invalid: %s", handle, coords)

            # If handle matched but coords were [0,0], Find My hasn't synced yet.
            # Tell user to wait a moment and try again - do NOT assign someone else's location.
            if not matched_own_loc and matched_handle_no_coords:
                logger.info("[LOCATION_LINK] Handle %s matched but coords [0,0], asking user to retry", matched_handle_no_coords)
                await self.photon.send_message(
                    to_number=phone,
                    content="i can see you started sharing your location, but Find My hasn't synced yet. give it 30 seconds and share again?",
                    chat_guid=chat_guid,
                )
                return

            if matched_own_loc:
                # Auto-link: user's own handle found
                handle = matched_own_loc.get("handle", "")
                coords = matched_own_loc.get("coordinates", [])
                await self.db.link_handle_to_user(user_id, handle, handle_type="findmy")
                await self.db.upsert_user_location(
                    user_id=user_id,
                    latitude=coords[0],
                    longitude=coords[1],
                    findmy_handle=handle,
                    long_address=matched_own_loc.get("long_address"),
                    short_address=matched_own_loc.get("short_address"),
                    findmy_status=matched_own_loc.get("status"),
                )
                logger.info("[LOCATION_LINK] Auto-linked user's own handle=%s to user=%s", handle, user_id[:8])

                # Generate witty response based on nearby places
                user_name = user.get("name", "")
                short_address = matched_own_loc.get("short_address") or matched_own_loc.get("long_address") or "your area"
                witty_response = await self._generate_witty_location_response(
                    latitude=coords[0],
                    longitude=coords[1],
                    short_address=short_address,
                    user_name=user_name,
                )
                await self.photon.send_message(
                    to_number=phone,
                    content=witty_response,
                    chat_guid=chat_guid,
                )
                return

            # 6. FALLBACK: Filter for unrecognized handles with recent last_updated
            # (for users whose Find My handle differs from their registered phone/email)
            now_ms = time.time() * 1000
            recency_window_ms = 5 * 60 * 1000  # 5 minutes

            candidates = []
            for loc in locations:
                handle = _normalize_handle(loc.get("handle", ""))
                if not handle or handle in known_handles:
                    continue
                coords = loc.get("coordinates", [])
                if not coords or len(coords) < 2 or (coords[0] == 0 and coords[1] == 0):
                    continue
                last_updated = loc.get("last_updated")
                if last_updated:
                    try:
                        ts = int(last_updated)
                        if (now_ms - ts) <= recency_window_ms:
                            candidates.append(loc)
                    except (ValueError, TypeError):
                        pass

            logger.info("[LOCATION_LINK] Found %d candidate unrecognized handles", len(candidates))

            if not candidates:
                await self.photon.send_message(
                    to_number=phone,
                    content="thanks for sharing! i couldn't match your location to a Find My account right now. make sure location sharing is enabled in Find My.",
                    chat_guid=chat_guid,
                )
                return

            if len(candidates) == 1:
                # Auto-link: one clear candidate
                loc = candidates[0]
                handle = loc.get("handle", "")
                coords = loc.get("coordinates", [])
                await self.db.link_handle_to_user(user_id, handle, handle_type="findmy")
                await self.db.upsert_user_location(
                    user_id=user_id,
                    latitude=coords[0],
                    longitude=coords[1],
                    findmy_handle=handle,
                    long_address=loc.get("long_address"),
                    short_address=loc.get("short_address"),
                    findmy_status=loc.get("status"),
                )
                logger.info("[LOCATION_LINK] Auto-linked handle=%s to user=%s", handle, user_id[:8])

                # Generate witty response based on nearby places
                user_name = user.get("name", "")
                short_address = loc.get("short_address") or loc.get("long_address") or "your area"
                witty_response = await self._generate_witty_location_response(
                    latitude=coords[0],
                    longitude=coords[1],
                    short_address=short_address,
                    user_name=user_name,
                )
                await self.photon.send_message(
                    to_number=phone,
                    content=witty_response,
                    chat_guid=chat_guid,
                )
                return

            # Multiple candidates: ask user to pick
            candidate_handles = [loc.get("handle", "") for loc in candidates]
            # Pass dict directly — set_cached handles JSON serialization
            redis_client.set_cached(
                f"location_link_pending:{user_id}",
                {"handles": candidate_handles, "locations": candidates},
                ttl=300,
            )
            handles_list = ", ".join(f'"{h}"' for h in candidate_handles)
            await self.photon.send_message(
                to_number=phone,
                content=f"i see a few location accounts that could be yours: {handles_list}. which one is you?",
                chat_guid=chat_guid,
            )

        except Exception as e:
            logger.error("[LOCATION_LINK] Error handling location share: %s", e, exc_info=True)
            try:
                await self.photon.send_message(
                    to_number=phone,
                    content="thanks for sharing your location! had a small hiccup processing it though.",
                    chat_guid=chat_guid,
                )
            except Exception:
                pass

    async def _handle_pending_location_link(
        self, webhook: Any, user: Dict, pending_data: Any, pending_key: str
    ) -> None:
        """Handle user's reply to a multi-candidate location link prompt."""
        from app.utils.redis_client import redis_client

        user_id = str(user.get("id", ""))
        phone = webhook.from_number
        chat_guid = getattr(webhook, "chat_guid", None)
        reply = (webhook.content or "").strip().lower()

        try:
            # pending_data is already a dict (deserialized by redis_client.get_cached)
            pending = pending_data if isinstance(pending_data, dict) else {}
            handles = pending.get("handles", [])
            locations = pending.get("locations", [])

            # Match user reply to one of the candidate handles
            matched_loc = None
            for i, handle in enumerate(handles):
                if handle.lower() in reply or reply in handle.lower():
                    matched_loc = locations[i] if i < len(locations) else None
                    break

            if not matched_loc:
                # Try matching by index (e.g., "1", "first", "2", "second")
                for keyword, idx in [("1", 0), ("first", 0), ("2", 1), ("second", 1), ("3", 2), ("third", 2)]:
                    if keyword in reply and idx < len(locations):
                        matched_loc = locations[idx]
                        break

            if not matched_loc:
                await self.photon.send_message(
                    to_number=phone,
                    content="hmm, i couldn't figure out which one. can you reply with the exact email/handle?",
                    chat_guid=chat_guid,
                )
                return

            # Link the matched handle
            handle = matched_loc.get("handle", "")
            coords = matched_loc.get("coordinates", [])
            await self.db.link_handle_to_user(user_id, handle, handle_type="findmy")
            await self.db.upsert_user_location(
                user_id=user_id,
                latitude=coords[0],
                longitude=coords[1],
                findmy_handle=handle,
                long_address=matched_loc.get("long_address"),
                short_address=matched_loc.get("short_address"),
                findmy_status=matched_loc.get("status"),
            )

            # Clear pending state
            redis_client.invalidate_cache(pending_key)

            address = matched_loc.get("short_address") or matched_loc.get("long_address") or "your area"
            logger.info("[LOCATION_LINK] Linked handle=%s to user=%s via confirmation", handle, user_id[:8])
            await self.photon.send_message(
                to_number=phone,
                content=f'got it! linked "{handle}" to your account. i can see you\'re in {address}.',
                chat_guid=chat_guid,
            )

        except Exception as e:
            logger.error("[LOCATION_LINK] Error handling pending link: %s", e, exc_info=True)
            redis_client.invalidate_cache(pending_key)
            await self.photon.send_message(
                to_number=phone,
                content="had trouble linking that. try sharing your location again?",
                chat_guid=chat_guid,
            )
