from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import List, Tuple

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _worker_commands() -> List[Tuple[str, List[str]]]:
    python = sys.executable
    return [
        (
            "groupchat-summary",
            [python, "-m", "app.groupchat.summary.worker", "--loop", "--interval-seconds=5"],
        ),
        (
            "groupchat-followup",
            [python, "-m", "app.groupchat.followup.worker", "--loop", "--interval-seconds=10"],
        ),
        (
            "daily-email",
            [python, "-m", "app.proactive.email.worker", "--loop", "--interval-seconds=300"],
        ),
        (
            "proactive-outreach",
            [python, "-m", "app.proactive.outreach.worker", "--loop", "--interval-seconds=300"],
        ),
    ]


async def _start_workers() -> List[asyncio.subprocess.Process]:
    procs: List[asyncio.subprocess.Process] = []
    for name, cmd in _worker_commands():
        logger.info("[WORKERS] starting %s: %s", name, " ".join(cmd))
        proc = await asyncio.create_subprocess_exec(*cmd)
        proc._worker_name = name  # type: ignore[attr-defined]
        procs.append(proc)
    return procs


async def _stop_workers(procs: List[asyncio.subprocess.Process]) -> None:
    for proc in procs:
        name = getattr(proc, "_worker_name", "worker")
        if proc.returncode is None:
            logger.info("[WORKERS] stopping %s", name)
            proc.terminate()
    await asyncio.gather(*(proc.wait() for proc in procs), return_exceptions=True)


async def _run() -> int:
    procs = await _start_workers()

    stop_event = asyncio.Event()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.warning("[WORKERS] received signal %s", sig.name)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())

    async def _watch(proc: asyncio.subprocess.Process) -> Tuple[str, int]:
        code = await proc.wait()
        name = getattr(proc, "_worker_name", "worker")
        return name, int(code or 0)

    watch_tasks = [asyncio.create_task(_watch(proc)) for proc in procs]

    done, _ = await asyncio.wait(
        watch_tasks + [asyncio.create_task(stop_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if stop_event.is_set():
        await _stop_workers(procs)
        return 0

    # A worker exited unexpectedly.
    for task in done:
        if task in watch_tasks:
            name, code = task.result()
            logger.error("[WORKERS] %s exited with code %s", name, code)
            await _stop_workers(procs)
            return code or 1

    await _stop_workers(procs)
    return 0


def main() -> int:
    _configure_logging()
    return int(asyncio.run(_run()) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
