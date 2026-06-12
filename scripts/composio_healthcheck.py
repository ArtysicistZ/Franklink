from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict
from urllib.parse import urlparse

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


def _safe_url_hint(url: str) -> str:
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        hint_path = path[:18] + "…" if len(path) > 18 else path
        return f"{parsed.scheme}://{parsed.netloc}{hint_path}"
    except Exception:
        return "unparseable"


async def _run(*, user_id: str, print_url: bool) -> Dict[str, Any]:
    from app.integrations.composio_client import ComposioClient

    load_dotenv(override=False)

    client = ComposioClient()
    diagnostics: Dict[str, Any] = {
        "composio_available": client.is_available(),
        "api_key_present": bool(getattr(client, "api_key", None)),
        "base_url_set": bool(getattr(client, "base_url", None)),
        "provider": getattr(client, "provider", None),
        "calendar_provider": getattr(client, "calendar_provider", None),
        "entity_prefix": getattr(client, "entity_prefix", None),
        "auth_config_id_present": bool(getattr(client, "auth_config_id", None)),
        "calendar_auth_config_id_present": bool(getattr(client, "calendar_auth_config_id", None)),
        "callback_url_present": bool(getattr(client, "callback_url", None)),
        "gmail_toolkit_version": getattr(client, "gmail_toolkit_version", None),
        "calendar_toolkit_version": getattr(client, "calendar_toolkit_version", None),
    }

    try:
        resolved = await client._resolve_auth_config_id(force_lookup=True)  # noqa
    except Exception as exc:
        resolved = None
        diagnostics["resolve_auth_config_error"] = f"{type(exc).__name__}: {exc}"
    if resolved:
        diagnostics["resolved_auth_config_id_prefix"] = f"{resolved[:6]}..."

    try:
        auth_link = await client.initiate_gmail_connect(user_id=user_id)
        diagnostics["auth_link_generated"] = bool(auth_link)
        if auth_link:
            diagnostics["auth_link_hint"] = _safe_url_hint(auth_link)
            if print_url:
                diagnostics["auth_link"] = auth_link
    except Exception as exc:
        diagnostics["auth_link_generated"] = False
        diagnostics["auth_link_error"] = f"{type(exc).__name__}: {exc}"

    try:
        cal_link = await client.initiate_calendar_connect(user_id=user_id)
        diagnostics["calendar_auth_link_generated"] = bool(cal_link)
        if cal_link:
            diagnostics["calendar_auth_link_hint"] = _safe_url_hint(cal_link)
            if print_url:
                diagnostics["calendar_auth_link"] = cal_link
    except Exception as exc:
        diagnostics["calendar_auth_link_generated"] = False
        diagnostics["calendar_auth_link_error"] = f"{type(exc).__name__}: {exc}"

    # Optional: list auth-config providers for quick sanity checks (no IDs).
    try:
        from composio import Composio

        api_key = os.getenv("COMPOSIO_API_KEY")
        if api_key:
            c = Composio(api_key=api_key)
            res = c.auth_configs.list()
            items = getattr(res, "items", None) or getattr(res, "data", None) or []
            providers = []
            for cfg in items:
                if hasattr(cfg, "model_dump"):
                    cfg = cfg.model_dump()
                providers.append(str(cfg.get("provider") or cfg.get("name") or "")[:64])
            diagnostics["auth_configs_count"] = len(items)
            diagnostics["auth_config_providers"] = providers
    except Exception as exc:
        diagnostics["auth_configs_error"] = f"{type(exc).__name__}: {exc}"

    return diagnostics


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe Composio auth-link healthcheck (no secrets).")
    parser.add_argument("--user-id", default="composio-healthcheck", help="Entity user id to use for link generation.")
    parser.add_argument(
        "--print-url",
        action="store_true",
        help="Include the full redirect URL in output (sensitive).",
    )
    args = parser.parse_args()

    diagnostics = asyncio.run(_run(user_id=args.user_id, print_url=bool(args.print_url)))
    print(json.dumps(diagnostics, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
