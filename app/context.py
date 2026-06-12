"""Thread-local context for request-scoped data.

This module provides async-safe context variables for propagating request context
(user_id, chat_guid, job_type) through the call stack to the LLM client for
usage tracking without requiring explicit parameter passing.

Usage:
    # At request entry point (orchestrator, job worker):
    set_llm_context(user_id=str(user['id']), chat_guid=webhook.chat_guid)
    try:
        # ... handle request, LLM calls will automatically pick up context ...
    finally:
        clear_llm_context()

    # In LLM client (automatic):
    ctx = get_llm_context()
    await tracker.log_usage(..., user_id=ctx.get("user_id"), ...)
"""

from contextvars import ContextVar
from typing import Optional, Dict, Any

# Context variables for LLM usage tracking
# These are async-safe and isolated per-task/coroutine
_current_user_id: ContextVar[Optional[str]] = ContextVar("llm_user_id", default=None)
_current_chat_guid: ContextVar[Optional[str]] = ContextVar("llm_chat_guid", default=None)
_current_job_type: ContextVar[Optional[str]] = ContextVar("llm_job_type", default=None)


def set_llm_context(
    user_id: Optional[str] = None,
    chat_guid: Optional[str] = None,
    job_type: Optional[str] = None,
) -> None:
    """
    Set the current context for LLM usage tracking.

    Call this at the entry point of request handling (orchestrator.handle_message,
    job worker functions) to associate all subsequent LLM calls with the user/chat/job.

    Args:
        user_id: UUID string of the user making the request
        chat_guid: iMessage chat GUID for group chat context
        job_type: Background job identifier (e.g., "profile_synthesis", "daily_email")
    """
    if user_id is not None:
        _current_user_id.set(str(user_id) if user_id else None)
    if chat_guid is not None:
        _current_chat_guid.set(chat_guid if chat_guid else None)
    if job_type is not None:
        _current_job_type.set(job_type if job_type else None)


def get_llm_context() -> Dict[str, Any]:
    """
    Get the current LLM context.

    Returns:
        Dictionary with user_id, chat_guid, and job_type (may be None)
    """
    return {
        "user_id": _current_user_id.get(),
        "chat_guid": _current_chat_guid.get(),
        "job_type": _current_job_type.get(),
    }


def clear_llm_context() -> None:
    """
    Clear all context variables.

    Call this in a finally block after request handling completes to ensure
    context doesn't leak between requests.
    """
    _current_user_id.set(None)
    _current_chat_guid.set(None)
    _current_job_type.set(None)
