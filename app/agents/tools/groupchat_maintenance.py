"""Group chat maintenance tools.

Tools for:
- Generating news and polls (using IcebreakerService)
- Scheduling meetings (Composio calendar integration)
- Getting group chat context
- Sending content to group chats from DM
- Resolving group chat from description
"""

import hashlib
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from app.agents.tools.base import tool, ToolResult
from app.database.client import DatabaseClient
from app.groupchat.features.icebreaker import IcebreakerService
from app.integrations.composio_client import ComposioClient
from app.integrations.photon_client import PhotonClient
from app.config import settings

logger = logging.getLogger(__name__)


@tool(
    name="get_group_chat_context",
    description="Get context about a group chat including participants and their interests.",
)
async def get_group_chat_context(
    chat_guid: str,
    user_id: Optional[str] = None,
) -> ToolResult:
    """Get group chat context.

    Args:
        chat_guid: The group chat GUID
        user_id: Optional requesting user ID (for permissions check)

    Returns:
        ToolResult with group chat info
    """
    try:
        db = DatabaseClient()

        # Get group chat record (unified storage handles both 2-person and multi-person)
        chat = await db.get_group_chat_by_guid(chat_guid)
        if not chat:
            return ToolResult(
                success=False,
                error=f"Group chat not found: {chat_guid}"
            )

        # Get participants from unified participants table
        participants = await db.get_group_chat_participants(chat_guid)

        if not participants:
            return ToolResult(
                success=False,
                error=f"No participants found for group chat: {chat_guid}"
            )

        # Get user details for each participant
        participant_details = []
        combined_interests = []
        for p in participants:
            user = await db.get_user_by_id(p.get("user_id"))
            if user:
                participant_details.append({
                    "user_id": str(user.get("id")),
                    "name": user.get("name"),
                    "university": user.get("university"),
                    "career_interests": user.get("career_interests", []),
                })
                combined_interests.extend(user.get("career_interests", []))

        # Dedupe interests
        combined_interests = list(set(combined_interests))

        return ToolResult(
            success=True,
            data={
                "chat_guid": chat_guid,
                "participants": participant_details,
                "participant_count": len(participant_details),
                "combined_interests": combined_interests,
                "connection_request_id": chat.get("connection_request_id"),
                "member_count": chat.get("member_count", len(participant_details)),
            }
        )

    except Exception as e:
        logger.error(f"[GROUPCHAT_MAINT] get_group_chat_context failed: {e}", exc_info=True)
        return ToolResult(success=False, error=str(e))


@tool(
    name="generate_news_poll",
    description="Generate a relevant news article summary with discussion question and poll for a group chat. Uses participants' interests to find relevant content.",
)
async def generate_news_poll(
    chat_guid: str,
    participant_interests: Optional[List[str]] = None,
    custom_topic: Optional[str] = None,
) -> ToolResult:
    """Generate news and poll content using IcebreakerService.

    Args:
        chat_guid: Target group chat GUID
        participant_interests: Combined interests of participants (optional, fetched if not provided)
        custom_topic: Optional specific topic to focus on

    Returns:
        ToolResult with generated content ready to send
    """
    try:
        db = DatabaseClient()
        icebreaker_service = IcebreakerService(db=db)

        # Get chat record (unified storage)
        chat = await db.get_group_chat_by_guid(chat_guid)

        # Get participants from unified participants table
        participants = await db.get_group_chat_participants(chat_guid)

        # Get participant info if interests not provided
        if not participant_interests:
            participant_interests = []
            for p in participants:
                user = await db.get_user_by_id(p.get("user_id"))
                if user:
                    participant_interests.extend(user.get("career_interests", []))
            participant_interests = list(set(participant_interests))

        # Add custom topic to interests for relevance scoring
        if custom_topic:
            participant_interests = [custom_topic] + participant_interests

        if len(participants) < 2:
            return ToolResult(
                success=False,
                error="Group chat needs at least 2 participants"
            )

        user_a_id = participants[0].get("user_id") if len(participants) > 0 else ""
        user_b_id = participants[1].get("user_id") if len(participants) > 1 else ""

        user_a = await db.get_user_by_id(user_a_id) if user_a_id else {}
        user_b = await db.get_user_by_id(user_b_id) if user_b_id else {}

        content = await icebreaker_service.build_icebreaker(
            user_a_id=user_a_id,
            user_b_id=user_b_id,
            user_a_name=user_a.get("name", "") if user_a else "",
            user_b_name=user_b.get("name", "") if user_b else "",
            shared_interests=participant_interests,
        )

        return ToolResult(
            success=True,
            data={
                "news_title_message": content.news_title_message,
                "news_url": content.news_url,
                "discussion_message": content.discussion_message,
                "poll_title": content.poll_title,
                "poll_options": content.poll_options,
                "chat_guid": chat_guid,
                "custom_topic": custom_topic,
            }
        )

    except Exception as e:
        logger.error(f"[GROUPCHAT_MAINT] generate_news_poll failed: {e}", exc_info=True)
        return ToolResult(success=False, error=str(e))


@tool(
    name="send_news_poll_to_chat",
    description="Send generated news and poll content to a group chat. Sends the news message first, then creates a native iMessage poll.",
)
async def send_news_poll_to_chat(
    chat_guid: str,
    news_title_message: Optional[str] = None,
    news_url: Optional[str] = None,
    discussion_message: str = "",
    poll_title: str = "",
    poll_options: Optional[List[str]] = None,
) -> ToolResult:
    """Send news and poll content to a group chat.

    Args:
        chat_guid: Target group chat GUID
        news_title_message: News headline message (optional)
        news_url: URL to the news article (optional)
        discussion_message: Discussion prompt message
        poll_title: Title for the poll
        poll_options: List of poll options (2-6 options)

    Returns:
        ToolResult with send confirmation
    """
    try:
        photon = PhotonClient()
        sent_messages = []

        # Send news title if present
        if news_title_message:
            await photon.send_message_to_chat(chat_guid, news_title_message)
            sent_messages.append("news_title")

        # Send news URL if present
        if news_url:
            await photon.send_message_to_chat(chat_guid, news_url)
            sent_messages.append("news_url")

        # Send discussion message
        if discussion_message:
            await photon.send_message_to_chat(chat_guid, discussion_message)
            sent_messages.append("discussion")

        # Create poll if we have valid options
        if poll_title and poll_options and len(poll_options) >= 2:
            try:
                await photon.create_poll(
                    chat_guid=chat_guid,
                    title=poll_title,
                    options=poll_options,
                )
                sent_messages.append("poll")
            except Exception as poll_error:
                logger.warning(f"[GROUPCHAT_MAINT] Poll creation failed: {poll_error}")
                # Fall back to text-based poll
                poll_text = f"{poll_title}\n" + "\n".join(
                    f"{i+1}. {opt}" for i, opt in enumerate(poll_options)
                )
                await photon.send_message_to_chat(chat_guid, poll_text)
                sent_messages.append("poll_text_fallback")

        return ToolResult(
            success=True,
            data={
                "sent": True,
                "chat_guid": chat_guid,
                "sent_messages": sent_messages,
            }
        )

    except Exception as e:
        logger.error(f"[GROUPCHAT_MAINT] send_news_poll_to_chat failed: {e}", exc_info=True)
        return ToolResult(success=False, error=str(e))


@tool(
    name="schedule_meeting",
    description="Parse meeting time from user request and create a calendar event via Composio (if connected).",
)
async def schedule_meeting(
    chat_guid: str,
    time_description: str,
    meeting_purpose: Optional[str] = None,
    user_timezone: str = "America/New_York",
    organizer_user_id: Optional[str] = None,
) -> ToolResult:
    """Parse and schedule a meeting for the group.

    Args:
        chat_guid: Target group chat GUID
        time_description: Natural language time (e.g., "Jan 17 at 2pm EST")
        meeting_purpose: Optional purpose/title for the meeting
        user_timezone: User's timezone for interpretation
        organizer_user_id: User ID of the organizer (requester)

    Returns:
        ToolResult with scheduling outcome or clarification request
    """
    try:
        def _clip(value: str, limit: int = 120) -> str:
            s = str(value or "")
            return s if len(s) <= limit else s[:limit] + "..."

        def _tz_from_token(token: Optional[str], fallback_tz) -> Optional[Any]:
            if not token:
                return fallback_tz
            key = token.strip().upper()
            tz_map = {
                "EST": "America/New_York",
                "EDT": "America/New_York",
                "ET": "America/New_York",
                "CST": "America/Chicago",
                "CDT": "America/Chicago",
                "CT": "America/Chicago",
                "MST": "America/Denver",
                "MDT": "America/Denver",
                "MT": "America/Denver",
                "PST": "America/Los_Angeles",
                "PDT": "America/Los_Angeles",
                "PT": "America/Los_Angeles",
                "UTC": "UTC",
                "GMT": "UTC",
            }
            name = tz_map.get(key)
            if not name:
                return fallback_tz
            try:
                import pytz
                return pytz.timezone(name)
            except Exception:
                try:
                    from zoneinfo import ZoneInfo
                    return ZoneInfo(name)
                except Exception:
                    return fallback_tz

        def _extract_timezone_token(text: str) -> tuple[str, Optional[str]]:
            if not text:
                return text, None
            match = re.search(r"\b(EST|EDT|ET|CST|CDT|CT|MST|MDT|MT|PST|PDT|PT|UTC|GMT)\b", text, flags=re.IGNORECASE)
            if not match:
                return text, None
            token = match.group(1)
            cleaned = re.sub(r"\b" + re.escape(match.group(1)) + r"\b", "", text, flags=re.IGNORECASE).strip()
            return cleaned, token

        def _parse_time_component(text: str, *, assume_pm: bool = False) -> Optional[tuple[int, int]]:
            if not text:
                return None
            time_match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", text, flags=re.IGNORECASE)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2) or 0)
                meridiem = (time_match.group(3) or "").lower()
                if meridiem:
                    if meridiem == "pm" and hour != 12:
                        hour += 12
                    if meridiem == "am" and hour == 12:
                        hour = 0
                elif assume_pm and hour < 12:
                    hour += 12
                return hour, minute
            compact = re.search(r"\b(\d{2})(\d{2})\b", text)
            if compact:
                hour = int(compact.group(1))
                minute = int(compact.group(2))
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return hour, minute
            return None

        def _parse_date_component(text: str, now_local: datetime) -> Optional[datetime]:
            if not text:
                return None
            lower = text.lower()
            if "tomorrow" in lower:
                return now_local + timedelta(days=1)
            if "today" in lower or "tonight" in lower:
                return now_local
            iso = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", text)
            if iso:
                year = int(iso.group(1))
                month = int(iso.group(2))
                day = int(iso.group(3))
                return now_local.replace(year=year, month=month, day=day)
            month_match = re.search(
                r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*[\s\-/]*([0-9]{1,2})(?:[\s,]+([0-9]{4}))?\b",
                text,
                flags=re.IGNORECASE,
            )
            if month_match:
                month_map = {
                    "jan": 1,
                    "feb": 2,
                    "mar": 3,
                    "apr": 4,
                    "may": 5,
                    "jun": 6,
                    "jul": 7,
                    "aug": 8,
                    "sep": 9,
                    "sept": 9,
                    "oct": 10,
                    "nov": 11,
                    "dec": 12,
                }
                month = month_map.get(month_match.group(1).lower(), now_local.month)
                day = int(month_match.group(2))
                year = int(month_match.group(3)) if month_match.group(3) else now_local.year
                candidate = now_local.replace(year=year, month=month, day=day)
                if candidate.date() < now_local.date():
                    candidate = candidate.replace(year=candidate.year + 1)
                return candidate
            next_match = re.search(r"\bnext\s+(mon|tue|wed|thu|fri|sat|sun)(day)?\b", text, flags=re.IGNORECASE)
            if next_match:
                weekdays = {
                    "mon": 0,
                    "tue": 1,
                    "wed": 2,
                    "thu": 3,
                    "fri": 4,
                    "sat": 5,
                    "sun": 6,
                }
                target = weekdays[next_match.group(1).lower()]
                days_ahead = (target - now_local.weekday() + 7) % 7
                days_ahead = 7 if days_ahead == 0 else days_ahead
                return now_local + timedelta(days=days_ahead)
            return None

        def _fallback_parse_datetime(text: str, tz) -> tuple[Optional[datetime], Optional[str]]:
            cleaned, tz_token = _extract_timezone_token(text or "")
            resolved_tz = _tz_from_token(tz_token, tz)
            now_local = datetime.now(resolved_tz)
            base_date = _parse_date_component(cleaned, now_local)
            if not base_date:
                return None, "missing date"
            assume_pm = "tonight" in cleaned.lower()
            time_part = _parse_time_component(cleaned, assume_pm=assume_pm)
            if not time_part:
                return None, "missing time"
            hour, minute = time_part
            parsed = base_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
            try:
                if parsed.tzinfo is None and hasattr(resolved_tz, "localize"):
                    parsed = resolved_tz.localize(parsed)
            except Exception:
                pass
            return parsed, None

        def _extract_duration_minutes(text: str, default_minutes: int) -> int:
            if not text:
                return default_minutes
            match = re.search(r"(\d+)\s*(hours?|hrs?|hr|minutes?|mins?|min)\b", text.lower())
            if not match:
                return default_minutes
            value = int(match.group(1))
            unit = match.group(2)
            if unit.startswith("hour") or unit.startswith("hr"):
                return max(15, value * 60)
            return max(15, value)

        def _has_meridiem(text: str) -> bool:
            return bool(re.search(r"\b(am|pm)\b", text, flags=re.IGNORECASE))

        def _resolve_relative_date(
            text: str,
            tz,
        ) -> Optional[tuple[str, datetime, str]]:
            if not text:
                return None
            match = re.search(r"\b(tomorrow|today|tonight)\b", text, flags=re.IGNORECASE)
            if not match:
                return None
            token = match.group(1).lower()
            now_local = datetime.now(tz)
            if token == "tomorrow":
                base_date = now_local + timedelta(days=1)
            else:
                base_date = now_local
            cleaned = re.sub(r"\b(tomorrow|today|tonight)\b", "", text, flags=re.IGNORECASE).strip()
            cleaned = re.sub(r"^\s*at\s+", "", cleaned, flags=re.IGNORECASE).strip()
            return cleaned, base_date, token

        def _dedupe_emails(emails: List[str]) -> List[str]:
            seen = set()
            deduped = []
            for email in emails:
                normalized = str(email or "").strip().lower()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                deduped.append(normalized)
            return deduped

        def _request_hash(*parts: str) -> str:
            raw = "|".join(str(p or "") for p in parts)
            return hashlib.sha256(raw.encode("utf-8")).hexdigest()

        logger.info(
            "[GROUPCHAT_MAINT][SCHEDULE] request chat=%s organizer=%s time=%s purpose=%s",
            _clip(chat_guid, 48),
            _clip(organizer_user_id or "", 16),
            _clip(time_description, 80),
            _clip(meeting_purpose or "", 80),
        )

        # Try to parse the time to validate the format
        parsed_time = None
        parse_error = None

        try:
            if re.search(r"\byesterday\b", (time_description or "").lower()):
                return ToolResult(
                    success=True,
                    data={
                        "needs_clarification": True,
                        "clarification_type": "meeting_time_clarification",
                        "original_text": time_description,
                        "message": "that time looks like it's in the past. what future time should i schedule?",
                    }
                )
            tz = None
            try:
                import pytz
                tz = pytz.timezone(user_timezone)
            except ImportError:
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo(user_timezone)
                    logger.warning(
                        "[GROUPCHAT_MAINT][SCHEDULE] pytz missing, using zoneinfo for timezone=%s",
                        user_timezone,
                    )
                except Exception:
                    tz = timezone.utc
                    logger.warning(
                        "[GROUPCHAT_MAINT][SCHEDULE] pytz/zoneinfo unavailable, falling back to UTC",
                    )
            dateutil_available = False
            try:
                from dateutil import parser as date_parser
                from dateutil import tz as dateutil_tz
                dateutil_available = True
            except ImportError:
                dateutil_available = False

            if dateutil_available:
                tzinfos = {
                    "EST": dateutil_tz.tzoffset("EST", -5 * 3600),
                    "EDT": dateutil_tz.tzoffset("EDT", -4 * 3600),
                    "CST": dateutil_tz.tzoffset("CST", -6 * 3600),
                    "CDT": dateutil_tz.tzoffset("CDT", -5 * 3600),
                    "MST": dateutil_tz.tzoffset("MST", -7 * 3600),
                    "MDT": dateutil_tz.tzoffset("MDT", -6 * 3600),
                    "PST": dateutil_tz.tzoffset("PST", -8 * 3600),
                    "PDT": dateutil_tz.tzoffset("PDT", -7 * 3600),
                    "ET": dateutil_tz.gettz("America/New_York"),
                    "CT": dateutil_tz.gettz("America/Chicago"),
                    "MT": dateutil_tz.gettz("America/Denver"),
                    "PT": dateutil_tz.gettz("America/Los_Angeles"),
                }

                relative = _resolve_relative_date(time_description, tz)
                if relative:
                    cleaned, base_date, token = relative
                    if not cleaned:
                        parse_error = "missing time"
                    else:
                        if token == "tonight" and not _has_meridiem(cleaned):
                            cleaned = f"{cleaned} pm"
                        parsed_time = date_parser.parse(
                            cleaned,
                            fuzzy=True,
                            default=base_date,
                            tzinfos=tzinfos,
                        )
                if not parsed_time and not parse_error:
                    parsed_time = date_parser.parse(
                        time_description,
                        fuzzy=True,
                        tzinfos=tzinfos,
                    )

                # Handle timezone
                if parsed_time and parsed_time.tzinfo is None:
                    if hasattr(tz, "localize"):
                        parsed_time = tz.localize(parsed_time)
                    else:
                        parsed_time = parsed_time.replace(tzinfo=tz)

            if not parsed_time or parse_error:
                parsed_time, fallback_error = _fallback_parse_datetime(time_description, tz)
                if fallback_error:
                    parse_error = fallback_error

        except Exception as e:
            parse_error = str(e)

        if parse_error or not parsed_time:
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] parse_failed chat=%s time=%s error=%s",
                _clip(chat_guid, 48),
                _clip(time_description, 80),
                parse_error,
            )
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "clarification_type": "meeting_time_clarification",
                    "original_text": time_description,
                    "parse_error": parse_error,
                    "message": "i couldn't parse that time. send it as a date + time, e.g., 'feb 1 8:30pm et' or '2026-02-01 20:30 america/new_york'",
                    "clarification_message": "i couldn't parse that time. send it as a date + time, e.g., 'feb 1 8:30pm et' or '2026-02-01 20:30 america/new_york'",
                }
            )

        logger.info(
            "[GROUPCHAT_MAINT][SCHEDULE] parsed_time chat=%s time=%s parsed=%s tz=%s",
            _clip(chat_guid, 48),
            _clip(time_description, 80),
            parsed_time.isoformat() if parsed_time else "None",
            parsed_time.tzinfo if parsed_time else "None",
        )

        # Guard against past times to avoid creating stale meetings
        now_local = datetime.now(parsed_time.tzinfo)
        if parsed_time < now_local - timedelta(minutes=1):
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] time_in_past chat=%s time=%s parsed=%s now=%s",
                _clip(chat_guid, 48),
                _clip(time_description, 80),
                parsed_time,
                now_local,
            )
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "clarification_type": "meeting_time_clarification",
                    "original_text": time_description,
                    "message": "that time looks like it's in the past. what future time should i schedule?",
                }
            )

        # Resolve organizer
        organizer_user_id = str(organizer_user_id or "").strip() or None
        if not organizer_user_id:
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] missing_organizer chat=%s time=%s",
                _clip(chat_guid, 48),
                _clip(time_description, 80),
            )
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "clarification_type": "meeting_organizer_clarification",
                    "message": "i already got the time. i still need to know who is organizing this meeting to create the calendar event",
                    "clarification_message": "i already got the time. i still need to know who is organizing this meeting to create the calendar event",
                },
            )

        # Build attendees from group chat participants (unified storage)
        db = DatabaseClient()
        participants = await db.get_group_chat_participants(chat_guid)

        attendee_emails: List[str] = []
        missing_emails: List[str] = []
        for p in participants:
            uid = str(p.get("user_id") or "").strip()
            if not uid:
                continue
            user = await db.get_user_by_id(uid)
            if user and user.get("email"):
                attendee_emails.append(str(user.get("email")))
            else:
                name = user.get("name") if user else None
                missing_emails.append(name or uid)

        attendee_emails = _dedupe_emails(attendee_emails)

        if missing_emails:
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] missing_attendees chat=%s missing=%s",
                _clip(chat_guid, 48),
                _clip(", ".join(missing_emails), 120),
            )
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "clarification_type": "meeting_attendee_clarification",
                    "missing_attendees": missing_emails,
                    "message": f"i already got the time. i still need email addresses for: {', '.join(missing_emails)}",
                    "clarification_message": f"i already got the time. i still need email addresses for: {', '.join(missing_emails)}",
                },
            )

        # Ensure calendar connection
        composio = ComposioClient()
        if not composio.is_available():
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] composio_unavailable chat=%s",
                _clip(chat_guid, 48),
            )
            return ToolResult(
                success=True,
                data={
                    "feature_status": "unavailable",
                    "message": "calendar integration is unavailable right now. please try again later",
                },
            )

        if not await composio.verify_calendar_connection(user_id=organizer_user_id):
            auth_link = await composio.initiate_calendar_connect(user_id=organizer_user_id)
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] calendar_not_connected chat=%s organizer=%s link=%s",
                _clip(chat_guid, 48),
                _clip(organizer_user_id or "", 16),
                "yes" if auth_link else "no",
            )
            link_text = f" {auth_link}" if auth_link else ""
            connect_message = (
                "i can’t create the event until your calendar is connected. "
                f"use this link to connect, then say done.{link_text}"
            )
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "clarification_type": "calendar_connect",
                    "auth_link": auth_link,
                    "error_code": composio.get_last_calendar_connect_error_code(),
                    "message": connect_message,
                    "clarification_message": connect_message,
                },
            )

        # Build event payload
        duration_minutes = _extract_duration_minutes(
            f"{time_description} {meeting_purpose or ''}",
            int(getattr(settings, "groupchat_meeting_default_minutes", 30) or 30),
        )
        end_time = parsed_time + timedelta(minutes=duration_minutes)
        formatted_time = parsed_time.strftime("%A, %B %d at %I:%M %p %Z")

        # Composio expects a local start time aligned with the provided timezone.
        # Passing UTC while also providing a non-UTC timezone double-shifts the event.
        start_datetime_local = parsed_time.isoformat()

        event_title = meeting_purpose or "Group chat meeting"
        request_hash = _request_hash(
            chat_guid,
            organizer_user_id,
            event_title,
            parsed_time.isoformat(),
            end_time.isoformat(),
            ",".join(attendee_emails),
        )

        # Idempotency check
        existing = await db.get_group_chat_calendar_event_by_hash(request_hash)
        if existing:
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] already_scheduled chat=%s event_id=%s",
                _clip(chat_guid, 48),
                _clip(str(existing.get("event_id") or ""), 32),
            )
            return ToolResult(
                success=True,
                data={
                    "feature_status": "already_scheduled",
                    "event_id": existing.get("event_id"),
                    "start_time": existing.get("start_time"),
                    "end_time": existing.get("end_time"),
                    "formatted_time": formatted_time,
                    "attendees": existing.get("attendees") or attendee_emails,
                    "chat_guid": chat_guid,
                },
            )

        response = await composio.create_calendar_event(
            user_id=organizer_user_id,
            start_datetime_utc=start_datetime_local,
            duration_minutes=duration_minutes,
            summary=event_title,
            timezone=user_timezone,
            attendees=attendee_emails,
            description=None,
            location=None,
            send_updates=bool(getattr(settings, "groupchat_meeting_send_updates", True)),
            calendar_id=getattr(settings, "groupchat_meeting_calendar_id", "primary"),
            create_meeting_room=bool(getattr(settings, "groupchat_meeting_create_meeting_room", False)),
        )

        if not isinstance(response, dict):
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] calendar_error chat=%s reason=non_dict_response",
                _clip(chat_guid, 48),
            )
            return ToolResult(
                success=True,
                data={
                    "feature_status": "calendar_error",
                    "message": "calendar event creation failed",
                },
            )

        if response.get("successful") is False:
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] calendar_error chat=%s error=%s",
                _clip(chat_guid, 48),
                response.get("error"),
            )
            return ToolResult(
                success=True,
                data={
                    "feature_status": "calendar_error",
                    "message": "calendar event creation failed",
                    "error": response.get("error"),
                },
            )

        data = response.get("data") if isinstance(response.get("data"), dict) else {}
        event_id = data.get("id") or data.get("event_id")
        event_link = data.get("htmlLink") or data.get("html_link") or data.get("link")

        stored = await db.create_group_chat_calendar_event(
            chat_guid=chat_guid,
            organizer_user_id=organizer_user_id,
            event_id=event_id,
            title=event_title,
            start_time=parsed_time.isoformat(),
            end_time=end_time.isoformat(),
            timezone=user_timezone,
            attendees=attendee_emails,
            request_hash=request_hash,
            event_link=event_link,
            status="created",
        )
        if not stored:
            logger.warning("[GROUPCHAT_MAINT] failed to persist calendar event record for %s", chat_guid)
        else:
            logger.info(
                "[GROUPCHAT_MAINT][SCHEDULE] scheduled chat=%s event_id=%s start=%s",
                _clip(chat_guid, 48),
                _clip(str(event_id or ""), 32),
                parsed_time.isoformat(),
            )

        return ToolResult(
            success=True,
            data={
                "feature_status": "scheduled",
                "event_id": event_id,
                "event_link": event_link,
                "start_time": parsed_time.isoformat(),
                "end_time": end_time.isoformat(),
                "formatted_time": formatted_time,
                "attendees": attendee_emails,
                "meeting_purpose": meeting_purpose,
                "chat_guid": chat_guid,
            },
        )

    except Exception as e:
        logger.error(f"[GROUPCHAT_MAINT] schedule_meeting failed: {e}", exc_info=True)
        return ToolResult(success=False, error=str(e))


@tool(
    name="resolve_group_chat_from_description",
    description="Resolve a group chat from a description like 'my chat with Alice'. Returns the chat_guid if found, or asks for clarification.",
)
async def resolve_group_chat_from_description(
    user_id: str,
    description: str,
) -> ToolResult:
    """Resolve a group chat from a user's description.

    Args:
        user_id: The requesting user's ID
        description: Description like "my chat with Alice" or "the study group"

    Returns:
        ToolResult with resolved chat_guid or needs_clarification
    """
    try:
        db = DatabaseClient()

        # Get all group chats for this user
        user_chats = await db.get_user_group_chats(user_id)

        if not user_chats:
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "no_chats": True,
                    "message": "you don't have any group chats yet"
                }
            )

        # Try to match based on description
        description_lower = description.lower()
        matches = []

        for chat_record in user_chats:
            chat_guid = chat_record.get("chat_guid")
            if not chat_guid:
                continue

            # Get participants for this chat from unified participants table
            participants = await db.get_group_chat_participants(chat_guid)

            # Find other participants (not the requesting user)
            other_ids = [
                str(p.get("user_id"))
                for p in participants
                if str(p.get("user_id")) != str(user_id) and p.get("user_id")
            ]

            for other_id in other_ids:
                other_user = await db.get_user_by_id(other_id)
                if other_user:
                    other_name = (other_user.get("name") or "").lower()
                    # Check if the name appears in the description
                    if other_name and other_name in description_lower:
                        matches.append({
                            "chat_guid": chat_guid,
                            "participant_name": other_user.get("name"),
                            "participant_id": str(other_user.get("id")),
                        })

        if len(matches) == 1:
            return ToolResult(
                success=True,
                data={
                    "resolved": True,
                    "chat_guid": matches[0]["chat_guid"],
                    "matched_participant": matches[0]["participant_name"],
                }
            )
        elif len(matches) > 1:
            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "multiple_matches": True,
                    "candidates": matches,
                    "message": f"found multiple chats. which one do you mean? {', '.join(m['participant_name'] for m in matches)}"
                }
            )
        else:
            # No name match found, list available chats
            available_chats = []
            for chat_record in user_chats[:5]:  # Limit to 5
                chat_guid = chat_record.get("chat_guid")
                if not chat_guid:
                    continue

                # Get participants for this chat from unified participants table
                participants = await db.get_group_chat_participants(chat_guid)

                # Find other participants (not the requesting user)
                other_names = []
                for p in participants:
                    p_user_id = str(p.get("user_id") or "")
                    if p_user_id and p_user_id != str(user_id):
                        other_user = await db.get_user_by_id(p_user_id)
                        if other_user and other_user.get("name"):
                            other_names.append(other_user.get("name"))

                if other_names:
                    available_chats.append({
                        "chat_guid": chat_guid,
                        "participants": other_names,
                    })

            return ToolResult(
                success=True,
                data={
                    "needs_clarification": True,
                    "no_match": True,
                    "available_chats": available_chats,
                    "message": f"couldn't find a chat matching '{description}'. which chat do you mean?"
                }
            )

    except Exception as e:
        logger.error(f"[GROUPCHAT_MAINT] resolve_group_chat_from_description failed: {e}", exc_info=True)
        return ToolResult(success=False, error=str(e))


@tool(
    name="send_message_to_group_chat",
    description="Send a text message to a specific group chat. Used when user requests from DM to send content to their group chat.",
)
async def send_message_to_group_chat(
    chat_guid: str,
    content: str,
) -> ToolResult:
    """Send a message to a group chat.

    Args:
        chat_guid: Target group chat GUID
        content: Text content to send

    Returns:
        ToolResult with send confirmation
    """
    try:
        if not content or not content.strip():
            return ToolResult(
                success=False,
                error="Message content cannot be empty"
            )

        photon = PhotonClient()
        await photon.send_message_to_chat(chat_guid, content.strip())

        return ToolResult(
            success=True,
            data={
                "sent": True,
                "chat_guid": chat_guid,
            }
        )

    except Exception as e:
        logger.error(f"[GROUPCHAT_MAINT] send_message_to_group_chat failed: {e}", exc_info=True)
        return ToolResult(success=False, error=str(e))
