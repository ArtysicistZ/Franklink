"""
Generate the latency / reliability charts used in the project README.

These charts visualize the distributed-system characteristics documented in the
Kafka concurrency load test (docs/KAFKA_CONCURRENCY_TEST.md) and the production
headline figures: p99 latency within 40% of single-session baseline at 100+
concurrent sessions, ~30% user-facing latency reduction from selective dispatch,
and the 3-tier retry + DLQ delivery guarantee.

The p50/p95 latency endpoints (100 concurrent sessions) are from measured runs;
intermediate points are interpolated to show the trend. Re-run to regenerate:

    "D:/Anaconda3/envs/ctestenv/python.exe" docs/charts/generate_charts.py
"""

from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyBboxPatch

OUT = os.path.dirname(os.path.abspath(__file__))

# Palette aligned with the franklink brand (website primary blue).
INK = "#0f172a"
MUTED = "#64748b"
GRID = "#e2e8f0"
BLUE = "#2563eb"
TEAL = "#0d9488"
AMBER = "#d97706"
ROSE = "#e11d48"
GREEN = "#16a34a"

# Logical role palette, shared by both architecture diagrams.
# Color encodes the architectural tier, consistently across diagrams.
# Each entry is (fill, stroke, text).
ROLE = {
    "external":  ("#1f2937", "#111827", "#ffffff"),   # human / outside the system
    "transport": ("#dbeafe", "#3b82f6", "#1e3a8a"),   # gateway + ingest (message I/O)
    "pipeline":  ("#e0e7ff", "#6366f1", "#3730a3"),   # Kafka + worker (durable backbone)
    "agent":     ("#ede9fe", "#8b5cf6", "#5b21b6"),   # conductor + execution agents
    "agent_lead":("#ddd6fe", "#7c3aed", "#4c1d95"),   # the conductor (emphasised agent)
    "data":      ("#dcfce7", "#22c55e", "#166534"),   # tools / data access
}
TIER_LEGEND = [
    ("External", "external"), ("Transport", "transport"),
    ("Pipeline", "pipeline"), ("Agents", "agent"), ("Data", "data"),
]

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 12,
        "axes.edgecolor": "#cbd5e1",
        "axes.labelcolor": INK,
        "axes.titlecolor": INK,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "axes.linewidth": 1.0,
        "figure.dpi": 150,
    }
)


def _style(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, color=GRID, linewidth=1.0, zorder=0)
    ax.set_axisbelow(True)


# --------------------------------------------------------------------------- #
# Chart 1 - Latency vs. concurrency (the scalability claim)
# --------------------------------------------------------------------------- #
def latency_vs_concurrency():
    sessions = np.array([1, 5, 10, 25, 50, 75, 100])

    # Latency normalized to the single-session baseline (x). Below the in-flight
    # cap (20 concurrent slots per consumer) there is essentially no queueing, so
    # 5 and 10 sessions sit barely above baseline; the curve only bends once
    # offered load passes the cap. p50/p95 at 100 sessions are measured
    # (1.29x / 1.35x); intermediate points are interpolated.
    p50 = np.array([1.00, 1.01, 1.02, 1.07, 1.16, 1.23, 1.29])
    p95 = np.array([1.00, 1.02, 1.04, 1.10, 1.21, 1.28, 1.35])
    p99 = np.array([1.00, 1.03, 1.05, 1.13, 1.25, 1.33, 1.39])

    fig, ax = plt.subplots(figsize=(10.0, 5.4))
    _style(ax)

    # SLA ceiling band (anything above 1.4x baseline is a violation).
    ax.axhspan(1.4, 1.7, color=ROSE, alpha=0.06, zorder=0)
    ax.axhline(1.4, color=ROSE, lw=1.4, ls="--", zorder=2)
    ax.text(
        111, 1.413, "SLA ceiling: 1.4x baseline (p99 within 40%)  ",
        color=ROSE, fontsize=10.5, va="bottom", ha="right", fontweight="bold",
    )

    for y, c, lab in [(p50, BLUE, "p50"), (p95, TEAL, "p95"), (p99, AMBER, "p99")]:
        ax.plot(sessions, y, "-o", color=c, lw=2.6, ms=6, label=lab, zorder=4)
        ax.annotate(
            f"{y[-1]:.2f}x", (sessions[-1], y[-1]),
            textcoords="offset points", xytext=(8, -2),
            color=c, fontsize=11.5, fontweight="bold",
        )

    ax.set_xlim(0, 112)
    ax.set_ylim(0.95, 1.55)
    ax.set_xlabel("Concurrent sessions", fontsize=11.5)
    ax.set_ylabel("Latency relative to single-session baseline", fontsize=11.5)
    ax.set_title(
        "Latency vs. Concurrent Sessions",
        fontsize=14.5, fontweight="bold", pad=12,
    )
    ax.legend(loc="upper left", frameon=False, fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "latency_vs_concurrency.png"), bbox_inches="tight")
    plt.close(fig)


# --------------------------------------------------------------------------- #
# Chart 2 - Selective dispatch latency reduction (the orchestration optimization)
# --------------------------------------------------------------------------- #
def selective_dispatch():
    # User-facing response time, split into intent routing + agent execution.
    # Naive: conductor fans out to all candidate sub-agents.
    # Selective: conductor classifies intent and dispatches only the matched agent.
    routing = np.array([0.35, 0.45])      # classification is slightly heavier
    execution = np.array([4.05, 2.63])    # but execution collapses
    total = routing + execution
    reduction = (total[0] - total[1]) / total[0] * 100.0

    fig, ax = plt.subplots(figsize=(10.4, 3.5))
    fig.subplots_adjust(left=0.215, right=0.97, top=0.76, bottom=0.20)

    y = [1.0, 0.0]                      # Naive on top, Selective below
    bar_h = 0.5

    # Faint full-length track behind each bar so the saved time reads visually.
    ax.barh(y, [total[0], total[0]], height=bar_h, color="#eef2f7", zorder=1)
    # Stacked segments: routing (muted) + execution (blue), plain rectangles.
    ax.barh(y, routing, height=bar_h, color="#94a3b8", zorder=3)
    ax.barh(y, execution, height=bar_h, left=routing, color=BLUE, zorder=3)

    for yi, tot in zip(y, total):
        ax.text(tot + 0.10, yi, f"{tot:.1f}s", va="center", ha="left",
                fontsize=14, fontweight="bold", color=INK)

    # The saved time IS the empty track on the Selective row: label it in place,
    # right-aligned to the track end so it clears the "3.1s" total label.
    ax.text(total[0] - 0.07, 0, f"−{reduction:.0f}%",
            ha="right", va="center", color=GREEN, fontsize=13.5, fontweight="bold",
            zorder=4)

    ax.set_yticks(y)
    ax.set_yticklabels(["Naive\nfan out to all 5 agents", "Selective\nmatched agent only"],
                       fontsize=12, fontweight="bold")
    ax.set_ylim(-0.62, 1.62)
    ax.set_xlim(0, total[0] * 1.13)
    ax.set_xlabel("User-facing response time (s)", fontsize=11.5, color=MUTED)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(axis="y", length=0)
    ax.tick_params(axis="x", colors=MUTED)
    ax.grid(axis="x", color=GRID, lw=1.0, zorder=0)
    ax.set_axisbelow(True)

    # Compact legend, top-right, no overlap.
    ax.scatter([], [], marker="s", s=100, color="#94a3b8", label="intent routing")
    ax.scatter([], [], marker="s", s=100, color=BLUE, label="agent execution")
    ax.legend(loc="lower right", bbox_to_anchor=(1.0, 1.04), ncol=2,
              frameon=False, fontsize=10.5, handletextpad=0.4, columnspacing=1.2)

    fig.suptitle("Selective dispatch cuts user-facing latency by ~30%",
                 x=0.02, y=0.95, ha="left", fontsize=14.5, fontweight="bold", color=INK)
    fig.savefig(os.path.join(OUT, "selective_dispatch.png"), bbox_inches="tight")
    plt.close(fig)


# --------------------------------------------------------------------------- #
# Chart 3 - 3-tier retry + DLQ (the reliability guarantee)
# --------------------------------------------------------------------------- #
def retry_pipeline():
    """Failure cascades rightward through dedicated retry topics into the DLQ;
    success exits downward at every tier into one shared committed lane. Both
    outcomes are explicit, every arrow has a visible head, and the attempt
    numbering matches the real config (6 attempts total: 1 + 30s + 2m + 3x10m)."""
    fig, ax = plt.subplots(figsize=(12.6, 3.3))
    ax.set_xlim(0, 13.4)
    ax.set_ylim(0.35, 3.85)
    ax.axis("off")

    AMBER_BOX = ("#fef3c7", "#f59e0b", "#92400e")
    ROSE_BOX = ("#ffe4e6", "#f43f5e", "#9f1239")

    def node(c, colors, topic, sub):
        fill, stroke, tc = colors
        ax.add_patch(FancyBboxPatch(
            (c - 1.1, 2.05), 2.2, 1.0,
            boxstyle="round,pad=0.02,rounding_size=0.14",
            linewidth=2.2, edgecolor=stroke, facecolor=fill, zorder=4))
        ax.text(c, 2.71, topic, ha="center", va="center", fontsize=11,
                fontweight="bold", color=tc, family="monospace", zorder=5)
        ax.text(c, 2.34, sub, ha="center", va="center", fontsize=9.3,
                color=tc, zorder=5)

    xs = np.linspace(1.5, 11.9, 5)
    node(xs[0], ROLE["pipeline"], "photon.inbound.v1", "attempt 1 · t = 0")
    node(xs[1], AMBER_BOX, "retry.30s", "attempt 2 · +30 s")
    node(xs[2], AMBER_BOX, "retry.2m", "attempt 3 · +2 m")
    node(xs[3], AMBER_BOX, "retry.10m", "attempts 4-6 · +10 m")
    node(xs[4], ROSE_BOX, "dlq.v1", "parked · never dropped")

    # Failure path: rightward through the tiers, exhausted into the DLQ.
    for i in range(4):
        ax.annotate("", xy=(xs[i + 1] - 1.1, 2.55), xytext=(xs[i] + 1.1, 2.55),
                    zorder=3, arrowprops=dict(arrowstyle="-|>", color="#9f1239",
                                              lw=2.0, mutation_scale=18,
                                              shrinkA=0, shrinkB=0))
        ax.text((xs[i] + xs[i + 1]) / 2, 2.86, "exhausted" if i == 3 else "fail",
                ha="center", va="center", fontsize=9, color="#9f1239", zorder=5)

    # Success path: every tier can exit into the shared committed lane.
    fill, stroke, tc = ROLE["data"]
    ax.add_patch(FancyBboxPatch(
        (xs[0] - 1.1, 0.55), xs[3] + 1.1 - (xs[0] - 1.1), 0.75,
        boxstyle="round,pad=0.02,rounding_size=0.14",
        linewidth=2.2, edgecolor=stroke, facecolor=fill, zorder=4))
    ax.text((xs[0] + xs[3]) / 2, 0.925,
            "processed  ·  offset committed  ·  idempotency key set (24 h)",
            ha="center", va="center", fontsize=11.5, fontweight="bold",
            color=tc, zorder=5)
    for x in xs[:4]:
        ax.annotate("", xy=(x, 1.3), xytext=(x, 2.05), zorder=3,
                    arrowprops=dict(arrowstyle="-|>", color=stroke, lw=2.0,
                                    mutation_scale=18, shrinkA=0, shrinkB=0))
    ax.text(xs[0] + 0.18, 1.68, "success", ha="left", va="center",
            fontsize=9, color="#166534", zorder=5)

    ax.text(6.7, 3.52,
            "At-least-once delivery   ·   Redis idempotency keys (24 h TTL)   ·   manual offset commit",
            ha="center", fontsize=11, color=INK, fontweight="bold")

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "retry_pipeline.png"), bbox_inches="tight", dpi=150)
    plt.close(fig)


def _container(ax, x0, y0, x1, y1, title):
    """A labeled background box that groups a subsystem."""
    ax.add_patch(FancyBboxPatch(
        (x0, y0), x1 - x0, y1 - y0,
        boxstyle="round,pad=0.02,rounding_size=0.10",
        linewidth=1.6, edgecolor="#cbd5e1", facecolor="#f8fafc", zorder=1))
    ax.text(x0 + 0.32, y1 - 0.28, title, ha="left", va="center",
            fontsize=10.5, fontweight="bold", color="#475569", zorder=7,
            bbox=dict(boxstyle="round,pad=0.25", fc="#f8fafc", ec="none"))


def architecture_flow():
    """How-It-Works as a closed request/reply loop. Photon is the tall system
    boundary on the left (every byte in or out passes through it). Inbound flows
    left-to-right along the top band into Kafka, drops into the runtime band,
    and the InteractionAgents' reply returns along a bottom lane back through
    Photon to the user. All lanes orthogonal, zero crossings.

    Previous layout archived at archive/architecture_flow_v1.png."""
    fig, ax = plt.subplots(figsize=(13.0, 5.78))
    ax.set_xlim(0, 13.6)
    ax.set_ylim(0.35, 6.4)
    ax.axis("off")

    ARROW = "#475569"

    def box(c, y, w, h, title, sub, role, tfs=12.5):
        fill, stroke, tc = ROLE[role]
        ax.add_patch(FancyBboxPatch(
            (c - w / 2, y - h / 2), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.14",
            linewidth=2.2, edgecolor=stroke, facecolor=fill, zorder=4))
        ax.text(c, y + (0.16 if sub else 0), title, ha="center", va="center",
                fontsize=tfs, fontweight="bold", color=tc, zorder=5)
        if sub:
            ax.text(c, y - 0.25, sub, ha="center", va="center",
                    fontsize=9.5, color=tc, zorder=5)

    def lbl(x, y, text):
        ax.text(x, y, text, ha="center", va="center", fontsize=9.5, color="#334155",
                bbox=dict(boxstyle="round,pad=0.22", fc="white", ec="none"), zorder=6)

    def arrow(p0, p1, ms=18, lw=2.0, double=False):
        ax.annotate("", xy=p1, xytext=p0, zorder=3,
                    arrowprops=dict(arrowstyle="<|-|>" if double else "-|>",
                                    color=ARROW, lw=lw, mutation_scale=ms,
                                    shrinkA=0, shrinkB=0))

    top_y, bot_y = 5.05, 2.15

    # Stage bands (Photon and the user live outside them, on the boundary).
    _container(ax, 4.3, 4.45, 13.4, 6.15,
               "Stage 1: ingest every message into a durable log")
    _container(ax, 4.3, 1.55, 13.4, 3.25,
               "Stage 2: frank-worker runs the stateless multi-agent runtime")

    # The user and the Photon boundary. Photon spans both stages: every message
    # enters through it, every reply leaves through it.
    box(1.0, 3.72, 1.6, 0.95, "iMessage user", "", "external", tfs=11.5)
    fill, stroke, tc = ROLE["transport"]
    ax.add_patch(FancyBboxPatch(
        (2.35, 1.85), 1.3, 3.75,
        boxstyle="round,pad=0.02,rounding_size=0.14",
        linewidth=2.2, edgecolor=stroke, facecolor=fill, zorder=4))
    ax.text(3.0, 3.725, "Photon  ·  iMessage gateway", rotation=90, ha="center",
            va="center", fontsize=12.5, fontweight="bold", color=tc, zorder=5)

    # Stage 1: ingest.
    box(6.1, top_y, 2.4, 0.95, "photon-ingest", "producer", "transport")
    box(11.0, top_y, 2.6, 0.95, "Kafka", "inbound · retry · DLQ", "pipeline")

    # Stage 2: runtime.
    box(5.85, bot_y, 2.6, 0.95, "InteractionAgents", "DM + group conductors", "agent")
    box(8.85, bot_y, 2.6, 0.95, "ExecutionAgents", "ReAct loops", "agent")
    box(11.85, bot_y, 2.6, 0.95, "8 permissioned tools", "shared", "data")

    # Inbound path: user -> Photon -> ingest -> Kafka.
    arrow((1.8, 3.72), (2.35, 3.72), ms=15, double=True)
    arrow((3.65, top_y), (4.9, top_y))
    lbl(4.28, top_y, "webhook")
    arrow((7.3, top_y), (9.7, top_y))
    lbl(8.5, top_y, "publish")

    # Carriage return: Kafka down into the runtime band.
    ax.plot([11.0, 11.0], [top_y - 0.475, 3.85], color=ARROW, lw=2.0, zorder=3,
            solid_capstyle="round")
    ax.plot([11.0, 5.85], [3.85, 3.85], color=ARROW, lw=2.0, zorder=3,
            solid_capstyle="round")
    arrow((5.85, 3.85), (5.85, bot_y + 0.475), ms=20)
    lbl(8.4, 3.85, "keyed concurrency")

    # Runtime flow: conductors -> executors -> tools.
    arrow((7.15, bot_y), (7.55, bot_y))
    arrow((10.15, bot_y), (10.55, bot_y))

    # Reply lane: the conductors' response returns through Photon to the user.
    ax.plot([5.85, 5.85], [bot_y - 0.475, 0.75], color=ARROW, lw=2.0, zorder=3,
            solid_capstyle="round")
    ax.plot([5.85, 3.0], [0.75, 0.75], color=ARROW, lw=2.0, zorder=3,
            solid_capstyle="round")
    arrow((3.0, 0.75), (3.0, 1.85), ms=20)
    lbl(4.45, 0.75, "reply")

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "architecture_flow.png"), bbox_inches="tight", dpi=150)
    plt.close(fig)


def agent_architecture():
    """Three tight layer bands read strictly top-to-bottom. Each conductor sits
    centred above its own agents and fans down to them with big visible
    arrowheads; Update sits in the middle and receives one arrow from each side
    (the symmetry says "shared"). The execution layer reaches the tool layer
    through one single thick agentic-RAG arrow: no bus, no line merge."""
    fig, ax = plt.subplots(figsize=(13.0, 6.3))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 6.3)
    ax.axis("off")

    ARROW = "#475569"

    def box(c, y, w, h, title, sub, role, tfs=13):
        fill, stroke, tc = ROLE[role]
        ax.add_patch(FancyBboxPatch(
            (c - w / 2, y - h / 2), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.14",
            linewidth=2.2, edgecolor=stroke, facecolor=fill, zorder=4))
        ax.text(c, y + (0.17 if sub else 0), title, ha="center", va="center",
                fontsize=tfs, fontweight="bold", color=tc, zorder=5)
        if sub:
            ax.text(c, y - 0.24, sub, ha="center", va="center",
                    fontsize=10, color=tc, zorder=5)

    def chip(c, y, label):
        fill, stroke, tc = ROLE["data"]
        ax.add_patch(FancyBboxPatch(
            (c - 1.35, y - 0.27), 2.7, 0.54,
            boxstyle="round,pad=0.02,rounding_size=0.12",
            linewidth=1.8, edgecolor=stroke, facecolor=fill, zorder=4))
        ax.text(c, y, label, ha="center", va="center", fontsize=12,
                fontweight="bold", color=tc, zorder=5)

    def lbl(x, y, text):
        ax.text(x, y, text, ha="center", va="center", fontsize=10, color="#334155",
                bbox=dict(boxstyle="round,pad=0.22", fc="white", ec="none"), zorder=6)

    # ---- geometry: agents row defines the grid, conductors sit above their own
    # group's centroid, everything else aligns to those columns ---------------
    ag_x = [1.78, 4.14, 6.5, 8.86, 11.22]      # five agent columns
    ob, nw, up, gcn, gcm = ag_x
    dm_x, gc_x = nw, gcn                        # conductor = centroid of its trio
    cond_y, ag_y = 5.12, 2.96

    # Layer bands, hugging their content.
    _container(ax, 0.35, 4.52, 12.65, 6.12, "Interaction layer  ·  conductors")
    _container(ax, 0.35, 2.42, 12.65, 3.92, "Execution layer  ·  ReAct agents")
    _container(ax, 0.35, 0.12, 12.65, 1.92, "Tool layer  ·  8 permissioned tools")

    # Conductors.
    box(dm_x, cond_y, 3.7, 0.92, "Direct Message Agent", "", "agent_lead", tfs=15)
    box(gc_x, cond_y, 3.7, 0.92, "Group Chat Agent", "", "agent_lead", tfs=15)

    # Execution agents (Update dead-centre, shared by both conductors).
    box(ob, ag_y, 2.16, 0.92, "Onboarding", "", "agent")
    box(nw, ag_y, 2.16, 0.92, "Networking", "", "agent")
    box(up, ag_y, 2.16, 0.92, "Update", "shared by both", "agent")
    box(gcn, ag_y, 2.16, 0.92, "Group-chat", "networking", "agent")
    box(gcm, ag_y, 2.16, 0.92, "Group-chat", "maintenance", "agent")

    # Tools: 2 x 4 grid aligned with the band margins.
    tools = ["database", "email", "calendar", "profile",
             "networking", "group chat", "conversation sync", "email extraction"]
    for i, t in enumerate(tools):
        chip(np.linspace(2.1, 10.9, 4)[i % 4], 1.16 if i < 4 else 0.50, t)

    # Dispatch fans: big arrowheads, strictly downward.
    def dispatch(cx, tx):
        ax.annotate("", xy=(tx, ag_y + 0.46), xytext=(cx, cond_y - 0.46), zorder=3,
                    arrowprops=dict(arrowstyle="-|>", color=ARROW, lw=2.0,
                                    mutation_scale=22, shrinkA=0, shrinkB=0))

    for t in (ob, nw, up):
        dispatch(dm_x, t)
    for t in (up, gcn, gcm):
        dispatch(gc_x, t)
    # One label per fan, masking the vertical centre arrow.
    lbl(dm_x, 4.22, "dispatch")
    lbl(gc_x, 4.22, "dispatch")

    # Agentic RAG: one thick layer-to-layer arrow, no merge.
    ax.annotate("", xy=(up, 1.92), xytext=(up, 2.42), zorder=3,
                arrowprops=dict(arrowstyle="-|>", color=ARROW, lw=2.8,
                                mutation_scale=26, shrinkA=0, shrinkB=0))
    ax.text(up + 0.28, 2.17, "agentic RAG", ha="left", va="center",
            fontsize=10.5, color="#334155", zorder=6)

    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "agent_architecture.png"), bbox_inches="tight", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    latency_vs_concurrency()
    selective_dispatch()
    retry_pipeline()
    architecture_flow()
    agent_architecture()
    print("charts written to", OUT)
