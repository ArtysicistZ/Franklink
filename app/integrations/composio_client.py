"""Composio client wrapper for Gmail context + Calendar scheduling."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

from app.config import settings

logger = logging.getLogger(__name__)

# Module-level cache for connected account IDs (shared across instances)
# Format: {(entity_id, toolkit_slug): (account_id, cached_at)}
_connected_account_cache: Dict[Tuple[str, str], Tuple[str, datetime]] = {}
_CONNECTED_ACCOUNT_CACHE_TTL = timedelta(minutes=5)

try:
    from composio import Composio  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Composio = None


READ_ONLY_GMAIL_TOOLS = {
    "GMAIL_FETCH_EMAILS",
    "GMAIL_FETCH_MESSAGE_BY_THREAD_ID",
    "GMAIL_GET_PROFILE",
}
_CALENDAR_TOOL_PREFIXES = ("GOOGLECALENDAR_", "GOOGLE_CALENDAR_")

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


class ComposioClient:
    """Minimal Composio wrapper with Gmail + Calendar tool access."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.composio_api_key
        self.base_url = getattr(settings, "composio_base_url", None)
        self.entity_prefix = getattr(settings, "composio_entity_prefix", "franklink")
        self.provider = getattr(settings, "composio_gmail_provider", "gmail")
        self.gmail_toolkit_slug = getattr(settings, "composio_gmail_toolkit_slug", "gmail")
        self.auth_config_id = getattr(settings, "composio_auth_config_id", None)
        self.gmail_toolkit_version = getattr(settings, "composio_gmail_toolkit_version", None)
        self.callback_url = getattr(settings, "composio_callback_url", None)
        self.calendar_provider = getattr(settings, "composio_calendar_provider", "googlecalendar")
        self.calendar_toolkit_slug = getattr(settings, "composio_calendar_toolkit_slug", "googlecalendar")
        self.calendar_auth_config_id = getattr(settings, "composio_calendar_auth_config_id", None)
        self.calendar_toolkit_version = getattr(settings, "composio_calendar_toolkit_version", None)
        self.calendar_create_tool = getattr(settings, "composio_calendar_create_tool", "GOOGLECALENDAR_CREATE_EVENT")
        self._client = None
        self._cached_auth_config_id = None
        self._cached_calendar_auth_config_id = None
        self._last_connect_error_code: Optional[str] = None
        self._last_calendar_connect_error_code: Optional[str] = None

        if not self.api_key or Composio is None:
            self._last_connect_error_code = "missing_api_key_or_sdk"
            return

        try:
            kwargs = {"api_key": self.api_key}
            if self.base_url:
                kwargs["base_url"] = self.base_url
            self._client = Composio(**kwargs)
        except Exception as exc:
            logger.warning("[COMPOSIO] failed to init: %s", exc)
            self._client = None
            self._last_connect_error_code = f"init_failed:{type(exc).__name__}"

    def get_last_connect_error_code(self) -> Optional[str]:
        return self._last_connect_error_code

    def get_last_calendar_connect_error_code(self) -> Optional[str]:
        return self._last_calendar_connect_error_code

    def is_available(self) -> bool:
        return self._client is not None

    def _entity_id(self, user_id: str) -> str:
        return f"{self.entity_prefix}:{user_id}"

    def _wrap_with_login_page(self, composio_url: str) -> str:
        """Wrap Composio OAuth URL with franklink.ai/login redirect.

        If login_page_url is configured, returns a URL like:
        https://franklink.ai/login?redirect=https%3A%2F%2Fbackend.composio.dev%2F...

        If not configured, returns the original Composio URL unchanged.
        """
        login_page_url = getattr(settings, "login_page_url", None)
        if not login_page_url:
            return composio_url
        return f"{login_page_url}?{urlencode({'redirect': composio_url})}"

    async def _initiate_connect(
        self,
        *,
        user_id: str,
        provider: str,
        auth_config_id: Optional[str],
        cached_attr_name: str,
        error_attr_name: str,
    ) -> Optional[str]:
        setattr(self, error_attr_name, None)
        if not self._client:
            logger.warning(
                "[COMPOSIO] connect initiate requested but client unavailable (api_key_present=%s composio_imported=%s)",
                bool(self.api_key),
                Composio is not None,
            )
            setattr(self, error_attr_name, "client_unavailable")
            return None
        entity_id = self._entity_id(user_id)

        auth_config_id_resolved = await self._resolve_auth_config_id_for_provider(
            provider=provider,
            auth_config_id=auth_config_id,
            cached_attr_name=cached_attr_name,
        )
        if not auth_config_id_resolved:
            logger.warning("[COMPOSIO] auth_config_id not found for provider=%s", provider)
            setattr(self, error_attr_name, "auth_config_missing")
            return None

        async def _attempt(auth_config_id_to_use: str) -> Optional[str]:
            def _call() -> Any:
                # Be compatible with slight signature differences across Composio SDK versions.
                kwargs = {
                    "user_id": entity_id,
                    "auth_config_id": auth_config_id_to_use,
                    "allow_multiple": True,
                }
                if self.callback_url:
                    kwargs["callback_url"] = self.callback_url
                try:
                    return self._client.connected_accounts.initiate(**kwargs)
                except TypeError as exc:
                    # Fallbacks for older SDKs / parameter naming differences.
                    msg = str(exc)
                    if "callback_url" in msg:
                        kwargs.pop("callback_url", None)
                        return self._client.connected_accounts.initiate(**kwargs)
                    if "user_id" in msg and "unexpected" in msg.lower():
                        kwargs["entity_id"] = kwargs.pop("user_id")
                        return self._client.connected_accounts.initiate(**kwargs)
                    raise

            result = await asyncio.to_thread(_call)
            redirect_url = _extract_redirect_url(result)
            if redirect_url:
                return self._wrap_with_login_page(redirect_url)
            return None

        try:
            redirect_url = await _attempt(auth_config_id_resolved)
            if redirect_url:
                return redirect_url

            if auth_config_id:
                fallback_id = await self._resolve_auth_config_id_for_provider(
                    provider=provider,
                    auth_config_id=auth_config_id,
                    cached_attr_name=cached_attr_name,
                    force_lookup=True,
                )
                if fallback_id and fallback_id != auth_config_id_resolved:
                    redirect_url = await _attempt(fallback_id)
                    if redirect_url:
                        logger.info(
                            "[COMPOSIO] connect initiate succeeded after auth_config_id fallback (provider=%s auth_config_id_prefix=%s)",
                            provider,
                            f"{fallback_id[:6]}...",
                        )
                        setattr(self, error_attr_name, None)
                        return redirect_url
                    logger.warning(
                        "[COMPOSIO] connect initiate fallback returned no redirect_url (provider=%s auth_config_id_prefix=%s)",
                        provider,
                        f"{fallback_id[:6]}...",
                    )
                    setattr(self, error_attr_name, "no_redirect_url_after_fallback")
                    return None

            logger.warning(
                "[COMPOSIO] connect initiate returned no redirect_url (provider=%s auth_config_id_prefix=%s)",
                provider,
                f"{auth_config_id_resolved[:6]}..." if auth_config_id_resolved else "",
            )
            setattr(self, error_attr_name, "no_redirect_url")
            return None
        except Exception as exc:
            logger.warning(
                "[COMPOSIO] connect initiate failed (provider=%s auth_config_id_prefix=%s callback_url_present=%s): %s",
                provider,
                f"{auth_config_id_resolved[:6]}..." if auth_config_id_resolved else "",
                bool(self.callback_url),
                exc,
                exc_info=True,
            )
            setattr(self, error_attr_name, f"initiate_failed:{type(exc).__name__}")

            # If an explicit auth_config_id is set and it's stale/invalid, retry once by auto-resolving.
            if auth_config_id:
                fallback_id = await self._resolve_auth_config_id_for_provider(
                    provider=provider,
                    auth_config_id=auth_config_id,
                    cached_attr_name=cached_attr_name,
                    force_lookup=True,
                )
                if fallback_id and fallback_id != auth_config_id_resolved:
                    try:
                        redirect_url = await _attempt(fallback_id)
                        if redirect_url:
                            logger.info(
                                "[COMPOSIO] connect initiate succeeded after auth_config_id fallback (provider=%s auth_config_id_prefix=%s)",
                                provider,
                                f"{fallback_id[:6]}...",
                            )
                            setattr(self, error_attr_name, None)
                            return redirect_url
                        logger.warning(
                            "[COMPOSIO] connect initiate fallback returned no redirect_url (provider=%s auth_config_id_prefix=%s)",
                            provider,
                            f"{fallback_id[:6]}...",
                        )
                        setattr(self, error_attr_name, "no_redirect_url_after_fallback")
                    except Exception as retry_exc:
                        logger.warning(
                            "[COMPOSIO] connect initiate retry failed (provider=%s auth_config_id_prefix=%s): %s",
                            provider,
                            f"{fallback_id[:6]}...",
                            retry_exc,
                            exc_info=True,
                        )
                        setattr(self, error_attr_name, f"retry_failed:{type(retry_exc).__name__}")
            return None

    async def initiate_gmail_connect(self, *, user_id: str) -> Optional[str]:
        return await self._initiate_connect(
            user_id=user_id,
            provider=self.provider,
            auth_config_id=self.auth_config_id,
            cached_attr_name="_cached_auth_config_id",
            error_attr_name="_last_connect_error_code",
        )

    async def initiate_calendar_connect(self, *, user_id: str) -> Optional[str]:
        return await self._initiate_connect(
            user_id=user_id,
            provider=self.calendar_provider,
            auth_config_id=self.calendar_auth_config_id,
            cached_attr_name="_cached_calendar_auth_config_id",
            error_attr_name="_last_calendar_connect_error_code",
        )

    def _is_tool_allowed(self, tool_name: str, *, allowlist: Optional[set[str]] = None) -> bool:
        name = str(tool_name or "").strip()
        if not name:
            return False
        upper = name.upper()
        if allowlist and (name in allowlist or upper in allowlist):
            return True
        if allowlist is None and any(upper.startswith(prefix) for prefix in _CALENDAR_TOOL_PREFIXES):
            return True
        return False

    async def _execute_tool_any(
        self,
        *,
        tool_name: str,
        user_id: str,
        connected_account_id: Optional[str],
        params: Optional[Dict[str, Any]],
        toolkit_version: Optional[str],
        allowlist: Optional[set[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self._client or not self._is_tool_allowed(tool_name, allowlist=allowlist):
            return None
        entity_id = self._entity_id(user_id)
        payload = params or {}

        def _call() -> Any:
            # Try new SDK API first (v0.10+), fall back to old API
            try:
                # New SDK API: uses slug, arguments, connected_account_id, user_id
                kwargs: Dict[str, Any] = {
                    "slug": tool_name,
                    "arguments": payload,
                    "user_id": entity_id,
                    "dangerously_skip_version_check": True,
                }
                if connected_account_id:
                    kwargs["connected_account_id"] = connected_account_id
                return self._client.tools.execute(**kwargs)
            except TypeError:
                # Fall back to old SDK API (v0.9.x)
                return self._client.tools.execute(
                    tool_name,
                    payload,
                    user_id=entity_id,
                    version=toolkit_version,
                )

        try:
            result = await asyncio.to_thread(_call)
            if isinstance(result, dict):
                return result
            if hasattr(result, "model_dump"):
                return result.model_dump()
            if hasattr(result, "__dict__"):
                return dict(result.__dict__)
            return None
        except Exception as exc:
            logger.warning("[COMPOSIO] tool execute failed: %s", exc, exc_info=True)
            return None

    async def execute_tool(
        self,
        *,
        tool_name: str,
        user_id: str,
        connected_account_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._execute_tool_any(
            tool_name=tool_name,
            user_id=user_id,
            connected_account_id=connected_account_id,
            params=params,
            toolkit_version=self.gmail_toolkit_version,
            allowlist=READ_ONLY_GMAIL_TOOLS,
        )

    async def execute_calendar_tool(
        self,
        *,
        tool_name: str,
        user_id: str,
        connected_account_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._execute_tool_any(
            tool_name=tool_name,
            user_id=user_id,
            connected_account_id=connected_account_id,
            params=params,
            toolkit_version=self.calendar_toolkit_version,
            allowlist=None,
        )

    async def fetch_recent_threads(
        self,
        *,
        user_id: str,
        connected_account_id: Optional[str] = None,
        query: str = "newer_than:30d",
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        response = await self.execute_tool(
            tool_name="GMAIL_FETCH_EMAILS",
            user_id=user_id,
            connected_account_id=connected_account_id,
            params={
                "query": query,
                "max_results": limit,
                "include_payload": True,
                "ids_only": False,
                "verbose": False,
            },
        )
        messages = []
        if isinstance(response, dict):
            data = response.get("data") if isinstance(response.get("data"), dict) else {}
            raw_messages = data.get("messages") if isinstance(data, dict) else None
            if isinstance(raw_messages, list):
                messages = raw_messages
        return list(messages or [])

    async def get_connected_account_id(
        self,
        *,
        user_id: str,
        toolkit_slug: Optional[str] = None,
        bypass_cache: bool = False,
    ) -> Optional[str]:
        """Get the connected account ID for a user (required for SDK v0.10+).

        Args:
            user_id: The user's ID
            toolkit_slug: Toolkit slug to filter connected accounts (default: gmail)
            bypass_cache: If True, skip cache and make fresh API call

        Returns:
            The connected account ID if found, None otherwise
        """
        if not self._client:
            return None
        entity_id = self._entity_id(user_id)
        toolkit_slug_filter = (toolkit_slug or self.gmail_toolkit_slug or "gmail").lower()

        # Check cache first (unless bypassed)
        cache_key = (entity_id, toolkit_slug_filter)
        if not bypass_cache and cache_key in _connected_account_cache:
            cached_id, cached_at = _connected_account_cache[cache_key]
            if datetime.utcnow() - cached_at < _CONNECTED_ACCOUNT_CACHE_TTL:
                return cached_id
            # Cache expired, remove it
            del _connected_account_cache[cache_key]

        def _call() -> Optional[str]:
            try:
                # List connected accounts for this entity
                # SDK v0.10+ uses user_ids (plural), toolkit_slugs, statuses
                try:
                    accounts = self._client.connected_accounts.list(
                        user_ids=[entity_id],
                        toolkit_slugs=[toolkit_slug_filter],
                        statuses=["ACTIVE"],
                    )
                except TypeError:
                    # Fallback for older SDK
                    accounts = self._client.connected_accounts.list()

                items = None
                if hasattr(accounts, "items"):
                    items = accounts.items
                elif hasattr(accounts, "data"):
                    items = accounts.data
                elif isinstance(accounts, list):
                    items = accounts
                elif isinstance(accounts, dict):
                    items = accounts.get("items") or accounts.get("data")

                if not items:
                    return None

                # Find the Gmail connected account for this user
                for account in items:
                    if hasattr(account, "model_dump"):
                        account = account.model_dump()
                    if not isinstance(account, dict):
                        continue
                    # Check if it's for the right user
                    acc_user_id = str(account.get("user_id", ""))
                    if acc_user_id and acc_user_id != entity_id:
                        continue
                    # Check if it's a Gmail account
                    toolkit = account.get("toolkit", {})
                    toolkit_slug = toolkit.get("slug", "") if isinstance(toolkit, dict) else str(toolkit)
                    toolkit_slug_val = str(toolkit_slug or "").lower()
                    status = str(account.get("status", "")).upper()
                    if toolkit_slug_val and toolkit_slug_val == toolkit_slug_filter and status == "ACTIVE":
                        return account.get("id")
                return None
            except Exception as exc:
                logger.warning("[COMPOSIO] get_connected_account_id failed: %s", exc)
                return None

        try:
            account_id = await asyncio.to_thread(_call)
            # Cache the result if found
            if account_id:
                _connected_account_cache[cache_key] = (account_id, datetime.utcnow())
            return account_id
        except Exception as exc:
            logger.warning("[COMPOSIO] get_connected_account_id failed: %s", exc)
            return None

    async def fetch_gmail_profile(
        self,
        *,
        user_id: str,
        connected_account_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch Gmail profile data to resolve the connected email address."""
        response = await self.execute_tool(
            tool_name="GMAIL_GET_PROFILE",
            user_id=user_id,
            connected_account_id=connected_account_id,
            params={},
        )
        return response if isinstance(response, dict) else None

    async def get_connected_gmail_address(self, *, user_id: str) -> Optional[str]:
        """Resolve the connected Gmail address for a user."""
        if not self._client:
            return None

        entity_id = self._entity_id(user_id)

        def _list_accounts() -> list[dict]:
            try:
                try:
                    accounts = self._client.connected_accounts.list(
                        user_ids=[entity_id],
                        toolkit_slugs=["gmail"],
                        statuses=["ACTIVE"],
                    )
                except TypeError:
                    accounts = self._client.connected_accounts.list()

                items = None
                if hasattr(accounts, "items"):
                    items = accounts.items
                elif hasattr(accounts, "data"):
                    items = accounts.data
                elif isinstance(accounts, list):
                    items = accounts
                elif isinstance(accounts, dict):
                    items = accounts.get("items") or accounts.get("data")
                if not items:
                    return []
                out: list[dict] = []
                for account in items:
                    if hasattr(account, "model_dump"):
                        account = account.model_dump()
                    if isinstance(account, dict):
                        out.append(account)
                return out
            except Exception as exc:
                logger.warning("[COMPOSIO] get_connected_gmail_address list failed: %s", exc)
                return []

        accounts = await asyncio.to_thread(_list_accounts)
        for account in accounts:
            acc_user_id = str(account.get("user_id", "") or "")
            if acc_user_id and acc_user_id != entity_id:
                continue
            toolkit = account.get("toolkit", {})
            toolkit_slug = toolkit.get("slug", "") if isinstance(toolkit, dict) else str(toolkit)
            status = str(account.get("status", "")).upper()
            if toolkit_slug.lower() != "gmail" or status != "ACTIVE":
                continue

            email = _extract_email_from_account(account)
            if email:
                return email.lower()

            connected_account_id = account.get("id")
            if connected_account_id:
                profile = await self.fetch_gmail_profile(
                    user_id=user_id,
                    connected_account_id=connected_account_id,
                )
                profile_email = _extract_email_from_profile(profile)
                if profile_email:
                    return profile_email.lower()

        return None

    async def get_connected_calendar_account_id(
        self, *, user_id: str, bypass_cache: bool = False
    ) -> Optional[str]:
        return await self.get_connected_account_id(
            user_id=user_id,
            toolkit_slug=self.calendar_toolkit_slug,
            bypass_cache=bypass_cache,
        )
    async def verify_gmail_connection(self, *, user_id: str) -> bool:
        """Verify that a user has an active Gmail connection.

        Makes a fresh API call (bypasses cache) to verify the connection exists.

        Args:
            user_id: The user's ID

        Returns:
            True if an active Gmail connection exists, False otherwise
        """
        account_id = await self.get_connected_account_id(
            user_id=user_id,
            toolkit_slug=self.gmail_toolkit_slug,
            bypass_cache=True,
        )
        return account_id is not None

    async def verify_calendar_connection(self, *, user_id: str) -> bool:
        """Verify that a user has an active Calendar connection."""
        account_id = await self.get_connected_account_id(
            user_id=user_id,
            toolkit_slug=self.calendar_toolkit_slug,
            bypass_cache=True,
        )
        return account_id is not None

    async def create_calendar_event(
        self,
        *,
        user_id: str,
        start_datetime_utc: str,
        duration_minutes: int,
        summary: str,
        timezone: str,
        attendees: Optional[List[str]] = None,
        description: Optional[str] = None,
        location: Optional[str] = None,
        send_updates: bool = True,
        calendar_id: Optional[str] = None,
        create_meeting_room: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Create a calendar event via Composio Calendar toolkit."""
        if not self._client:
            return None

        connected_account_id = await self.get_connected_calendar_account_id(user_id=user_id)
        if not connected_account_id:
            return None

        tool_name = str(self.calendar_create_tool or "GOOGLECALENDAR_CREATE_EVENT").strip()
        params: Dict[str, Any] = {
            "start_datetime": start_datetime_utc,
            "event_duration_minutes": int(duration_minutes),
            "summary": summary,
            "timezone": timezone,
            "attendees": attendees or [],
            "send_updates": bool(send_updates),
        }
        if description:
            params["description"] = description
        if location:
            params["location"] = location
        if calendar_id:
            params["calendar_id"] = calendar_id
        if create_meeting_room:
            params["create_meeting_room"] = True

        return await self.execute_calendar_tool(
            tool_name=tool_name,
            user_id=user_id,
            connected_account_id=connected_account_id,
            params=params,
        )

    async def _resolve_auth_config_id_for_provider(
        self,
        *,
        provider: str,
        auth_config_id: Optional[str],
        cached_attr_name: str,
        force_lookup: bool = False,
    ) -> Optional[str]:
        if auth_config_id and not force_lookup:
            return auth_config_id
        cached = getattr(self, cached_attr_name, None)
        if cached and not force_lookup:
            return cached
        if not self._client:
            return None
        def _normalize(value: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", value.lower())
        try:
            result = await asyncio.to_thread(self._client.auth_configs.list)
            items = None
            if hasattr(result, "items"):
                items = result.items
            elif hasattr(result, "data"):
                items = result.data
            elif isinstance(result, dict):
                items = result.get("items") or result.get("data")
            if not items:
                return None
            preferred: Optional[str] = None
            target = str(provider or "").lower()
            target_norm = _normalize(target)
            for cfg in items:
                if hasattr(cfg, "model_dump"):
                    cfg = cfg.model_dump()
                provider_name = str(cfg.get("provider") or cfg.get("name") or "").lower()
                provider_norm = _normalize(provider_name)
                cfg_id = str(cfg.get("id") or "").strip()
                if not provider_name or not cfg_id:
                    continue
                if target and (provider_name.startswith(target) or (target_norm and provider_norm.startswith(target_norm))):
                    setattr(self, cached_attr_name, cfg_id)
                    return cfg_id
                if target and ((target in provider_name) or (target_norm and target_norm in provider_norm)) and not preferred:
                    preferred = cfg_id
            if preferred:
                setattr(self, cached_attr_name, preferred)
                return preferred
        except Exception as exc:
            logger.warning("[COMPOSIO] auth config lookup failed: %s", exc, exc_info=True)
            return None
        return None

    async def _resolve_auth_config_id(self, *, force_lookup: bool = False) -> Optional[str]:
        return await self._resolve_auth_config_id_for_provider(
            provider=self.provider,
            auth_config_id=self.auth_config_id,
            cached_attr_name="_cached_auth_config_id",
            force_lookup=force_lookup,
        )


def _extract_redirect_url(result: Any) -> Optional[str]:
    if not result:
        return None
    if hasattr(result, "model_dump"):
        try:
            return _extract_redirect_url(result.model_dump())
        except Exception:
            pass
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            inner = _extract_redirect_url(data)
            if inner:
                return inner
        for key in ("redirect_url", "redirectUrl", "url", "link"):
            val = result.get(key)
            if val:
                return str(val)
    if hasattr(result, "redirect_url"):
        return str(getattr(result, "redirect_url"))
    if hasattr(result, "redirectUrl"):
        return str(getattr(result, "redirectUrl"))
    if hasattr(result, "url"):
        return str(getattr(result, "url"))
    if hasattr(result, "data"):
        try:
            inner = _extract_redirect_url(getattr(result, "data"))
            if inner:
                return inner
        except Exception:
            pass
    return None


def _extract_thread_ids(result: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(result, dict):
        return []
    ids: List[str] = []
    messages = result.get("messages")
    if isinstance(messages, list):
        for item in messages:
            if isinstance(item, dict):
                thread_id = item.get("threadId") or item.get("thread_id")
                if thread_id:
                    ids.append(str(thread_id))
    seen = set()
    deduped = []
    for tid in ids:
        if tid in seen:
            continue
        seen.add(tid)
        deduped.append(tid)
    return deduped


def _extract_email_from_profile(profile: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(profile, dict):
        return None
    data = profile.get("data") if isinstance(profile.get("data"), dict) else profile
    if isinstance(data, dict):
        for key in ("emailAddress", "email", "email_address", "address"):
            val = data.get(key)
            if isinstance(val, str):
                match = _EMAIL_RE.search(val)
                if match:
                    return match.group(0)
    return _find_email_in_payload(data)


def _extract_email_from_account(account: Dict[str, Any]) -> Optional[str]:
    for key in ("email", "account_email", "email_address", "user_email", "account_identifier"):
        val = account.get(key)
        if isinstance(val, str):
            match = _EMAIL_RE.search(val)
            if match:
                return match.group(0)
    for nested_key in ("account", "metadata", "auth", "profile"):
        nested = account.get(nested_key)
        email = _find_email_in_payload(nested)
        if email:
            return email
    return _find_email_in_payload(account)


def _find_email_in_payload(payload: Any) -> Optional[str]:
    if isinstance(payload, str):
        match = _EMAIL_RE.search(payload)
        return match.group(0) if match else None
    if isinstance(payload, dict):
        for value in payload.values():
            found = _find_email_in_payload(value)
            if found:
                return found
        return None
    if isinstance(payload, list):
        for item in payload:
            found = _find_email_in_payload(item)
            if found:
                return found
    return None
