#!/usr/bin/env python3
from __future__ import annotations

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
#     "numpy",
#     "plotly",
#     "pandas",
# ]
# ///
"""
Plot benchmark results from Fast CASPaxos simulation runs.

Reads benchmark-latency.csv files from the benchmark output directory and produces:
  - Latency vs proposal index (time-series)
  - Throughput (proposals/second, sliding window)
  - Latency CDF comparison

Comparative mode (--comparative) additionally generates:
  - Latency time-series with confidence bands (multi-run aggregation)
  - Contention spectrum charts
  - Failover timeline charts
  - RTT distribution comparisons
  - Scenario summary dashboards
  - Topology x mode heatmaps
  - Per-proposer latency traces

Usage:
    uv run scripts/plot_benchmarks.py --input artifacts/benchmark
    uv run scripts/plot_benchmarks.py --input artifacts/benchmark --format html
    uv run scripts/plot_benchmarks.py --input artifacts/benchmark --comparative
"""

import argparse
import csv
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

# ---------------------------------------------------------------------------
# Publication-quality color palette
# ---------------------------------------------------------------------------

MODE_COLORS = {
    "fast": "#2196F3",        # Blue
    "leader": "#FF9800",      # Orange
    "classic": "#9E9E9E",     # Gray
    "fast+leader": "#4CAF50", # Green
}

MODE_LABELS = {
    "fast": "Fast Paxos (Leaderless)",
    "leader": "Distinguished Leader",
    "classic": "Classic Paxos",
    "fast+leader": "Fast + Leader",
}

FAULT_WINDOW_COLOR = "#FFCDD2"   # Light red for fault shading
CONFIDENCE_ALPHA = 0.2           # Transparency for confidence bands

# Fallback colors when mode is not in MODE_COLORS
_EXTRA_COLORS = [
    "#E91E63", "#9C27B0", "#00BCD4", "#795548", "#607D8B",
    "#3F51B5", "#009688", "#FF5722", "#CDDC39", "#8BC34A",
]


def _color_for_mode(mode: str, idx: int = 0) -> str:
    """Return a color for a protocol mode, falling back to extras."""
    return MODE_COLORS.get(mode, _EXTRA_COLORS[idx % len(_EXTRA_COLORS)])


def _label_for_mode(mode: str) -> str:
    """Return a human-friendly label for a protocol mode."""
    return MODE_LABELS.get(mode, mode)


# ---------------------------------------------------------------------------
# Matplotlib global styling
# ---------------------------------------------------------------------------

def configure_matplotlib_style():
    """Set publication-quality defaults for matplotlib."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.size": 11,
        "axes.titlesize": 14,
        "axes.titleweight": "bold",
        "axes.labelsize": 12,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "figure.dpi": 150,
        "savefig.dpi": 200,
        "savefig.bbox": "tight",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.2,
        "lines.linewidth": 1.5,
    })


# ---------------------------------------------------------------------------
# CSV discovery and loading
# ---------------------------------------------------------------------------

def find_csv_files(input_dir: str) -> list[Path]:
    """Recursively find all benchmark-latency.csv files."""
    results = []
    for root, _, files in os.walk(input_dir):
        for f in files:
            if f == "benchmark-latency.csv":
                results.append(Path(root) / f)
    return sorted(results)


def load_csv(path: Path, round_id: str = "") -> list[dict]:
    """Load a CSV file into a list of dicts with typed values."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "request_id": int(row["request_id"]),
                "proposer": row["proposer"],
                "kind": row["kind"],
                "sent_ms": float(row["sent_ms"]),
                "completed_ms": float(row["completed_ms"]),
                "latency_ms": float(row["latency_ms"]),
                "round_trips": int(row["round_trips"]),
                "mode": row.get("mode", ""),
                "topology": row.get("topology", ""),
                "fault_label": row.get("fault_label", ""),
                "failover_count": int(row.get("failover_count", "0")),
                "_round_id": round_id,
            })
    return rows


def group_by(rows: list[dict], key: str) -> dict[str, list[dict]]:
    """Group rows by a key field."""
    groups = defaultdict(list)
    for row in rows:
        groups[row[key]].append(row)
    return dict(groups)


# ---------------------------------------------------------------------------
# Throughput helpers
# ---------------------------------------------------------------------------

def sliding_window_throughput(rows: list[dict], window_ms: float = 100.0) -> tuple[list[float], list[float]]:
    """Compute throughput (proposals/sec) over a sliding window."""
    if not rows:
        return [], []

    sorted_rows = sorted(rows, key=lambda r: r["completed_ms"])
    times = []
    throughputs = []

    for i, row in enumerate(sorted_rows):
        t = row["completed_ms"]
        window_start = t - window_ms
        count = sum(1 for r in sorted_rows if window_start <= r["completed_ms"] <= t)
        throughput = (count / window_ms) * 1000.0  # proposals per second
        times.append(t)
        throughputs.append(throughput)

    return times, throughputs


def _smooth_series(arr: np.ndarray, window: int) -> np.ndarray:
    """Apply centred rolling-mean smoothing, handling edges gracefully."""
    if len(arr) <= window:
        return arr
    import pandas as pd
    return pd.Series(arr).rolling(window, min_periods=1, center=True).mean().values


# ---------------------------------------------------------------------------
# Multi-run aggregation
# ---------------------------------------------------------------------------

def aggregate_multi_run(all_rows: list[dict], smoothing_window: int = 20) -> dict:
    """
    Aggregate samples across multiple rounds of the same scenario configuration.

    Groups by (mode, topology, fault_label), aligns by proposal index within
    each group, and computes statistics.

    Returns dict keyed by (mode, topology, fault_label) -> {
        'proposal_index': [...],
        'latency_median': [...],
        'latency_p25': [...],
        'latency_p75': [...],
        'latency_p5': [...],
        'latency_p95': [...],
        'round_trips_median': [...],
        'throughput_times': [...],
        'throughput_median': [...],
        'throughput_p25': [...],
        'throughput_p75': [...],
        'raw_latencies': [...],  # for CDF
    }
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in all_rows:
        key = (row["mode"], row["topology"], row["fault_label"])
        groups[key].append(row)

    result = {}
    for key, rows in groups.items():
        # --- Identify rounds ---
        # Prefer explicit _round_id when available
        has_round_id = any(r.get("_round_id") for r in rows)
        if has_round_id:
            round_dict: dict[str, list[dict]] = defaultdict(list)
            for r in rows:
                round_dict[r["_round_id"]].append(r)
            round_groups = list(round_dict.values())
        else:
            # Detect rounds by sent_ms resets
            sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
            round_groups = []
            current: list[dict] = [sorted_rows[0]]
            for i in range(1, len(sorted_rows)):
                if sorted_rows[i]["sent_ms"] < current[-1]["sent_ms"] - 100:
                    round_groups.append(current)
                    current = [sorted_rows[i]]
                else:
                    current.append(sorted_rows[i])
            round_groups.append(current)

        # Sort each round internally and assign proposal index
        for rg in round_groups:
            rg.sort(key=lambda r: r["sent_ms"])

        # Align across rounds using the minimum completed count
        min_len = min(len(rg) for rg in round_groups) if round_groups else 0
        if min_len == 0:
            continue

        proposal_indices = np.arange(min_len)

        # Collect per-index vectors
        lat_matrix = np.array(
            [[rg[i]["latency_ms"] for rg in round_groups] for i in range(min_len)]
        )
        rt_matrix = np.array(
            [[rg[i]["round_trips"] for rg in round_groups] for i in range(min_len)]
        )

        # Percentiles per index (axis=1 is across rounds)
        lat_median = np.median(lat_matrix, axis=1)
        lat_p25 = np.percentile(lat_matrix, 25, axis=1)
        lat_p75 = np.percentile(lat_matrix, 75, axis=1)
        lat_p5 = np.percentile(lat_matrix, 5, axis=1)
        lat_p95 = np.percentile(lat_matrix, 95, axis=1)
        rt_median = np.median(rt_matrix, axis=1)

        # Smooth after percentile computation
        lat_median = _smooth_series(lat_median, smoothing_window)
        lat_p25 = _smooth_series(lat_p25, smoothing_window)
        lat_p75 = _smooth_series(lat_p75, smoothing_window)
        lat_p5 = _smooth_series(lat_p5, smoothing_window)
        lat_p95 = _smooth_series(lat_p95, smoothing_window)
        rt_median = _smooth_series(rt_median, smoothing_window)

        # Throughput: per-round, then aggregate on a common time grid
        tp_times_all: list[list[float]] = []
        tp_values_all: list[list[float]] = []
        for rg in round_groups:
            ts, tps = sliding_window_throughput(rg, window_ms=200.0)
            if ts:
                tp_times_all.append(ts)
                tp_values_all.append(tps)

        if tp_times_all:
            all_times = sorted({t for ts in tp_times_all for t in ts})
            tp_matrix_np = np.array([
                np.interp(all_times, ts, tps)
                for ts, tps in zip(tp_times_all, tp_values_all)
            ])
            tp_median = np.median(tp_matrix_np, axis=0)
            tp_p25 = np.percentile(tp_matrix_np, 25, axis=0)
            tp_p75 = np.percentile(tp_matrix_np, 75, axis=0)
        else:
            all_times = []
            tp_median = np.array([])
            tp_p25 = np.array([])
            tp_p75 = np.array([])

        result[key] = {
            "proposal_index": proposal_indices,
            "latency_median": lat_median,
            "latency_p25": lat_p25,
            "latency_p75": lat_p75,
            "latency_p5": lat_p5,
            "latency_p95": lat_p95,
            "round_trips_median": rt_median,
            "throughput_times": np.array(all_times),
            "throughput_median": tp_median,
            "throughput_p25": tp_p25,
            "throughput_p75": tp_p75,
            "raw_latencies": [r["latency_ms"] for r in rows],
        }

    return result


# ---------------------------------------------------------------------------
# Directory structure discovery (comparative mode)
# ---------------------------------------------------------------------------

def discover_comparative_structure(root_dir: str) -> dict[str, list[Path]]:
    """Walk the comparative output directory and group CSV files by scenario name."""
    scenarios: dict[str, list[Path]] = defaultdict(list)
    root = Path(root_dir)
    for csv_file in find_csv_files(root_dir):
        try:
            rel = csv_file.relative_to(root)
        except ValueError:
            continue
        scenario_name = rel.parts[0] if rel.parts else "unknown"
        scenarios[scenario_name].append(csv_file)
    return dict(scenarios)


# ---------------------------------------------------------------------------
# 6a  Latency time-series with confidence bands
# ---------------------------------------------------------------------------

def plot_latency_timeseries_aggregated(
    aggregated_data: dict, output_dir: str, scenario_name: str,
) -> None:
    """Latency time-series with confidence bands (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    # Determine if we need to qualify labels with topology/fault
    modes_seen = set()
    topologies_seen = set()
    faults_seen = set()
    for (mode, topo, fault) in aggregated_data:
        modes_seen.add(mode)
        topologies_seen.add(topo)
        faults_seen.add(fault)
    multi_topo = len(topologies_seen) > 1
    multi_fault = len(faults_seen - {""}) > 1

    fig, ax = plt.subplots(figsize=(14, 6))
    legend_seen: set[str] = set()
    for idx, (key, data) in enumerate(sorted(aggregated_data.items())):
        mode, topology, fault = key
        color = _color_for_mode(mode, idx)
        suffix_parts = []
        if multi_topo and topology:
            suffix_parts.append(topology)
        if multi_fault and fault and fault != "none":
            suffix_parts.append(fault)
        label = _label_for_mode(mode)
        if suffix_parts:
            label += f" ({', '.join(suffix_parts)})"
        show_legend = label not in legend_seen
        legend_seen.add(label)

        x = data["proposal_index"]
        ax.plot(x, data["latency_median"], label=label if show_legend else "_nolegend_", color=color, linewidth=1.5)
        ax.fill_between(
            x, data["latency_p25"], data["latency_p75"],
            color=color, alpha=CONFIDENCE_ALPHA, label="_nolegend_",
        )
        ax.fill_between(
            x, data["latency_p5"], data["latency_p95"],
            color=color, alpha=CONFIDENCE_ALPHA * 0.5, label="_nolegend_",
        )

    ax.set_xlabel("Proposal Index", fontsize=12)
    ax.set_ylabel("Latency (ms)", fontsize=12)
    ax.set_title(f"Commit Latency \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    ax.legend(fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"latency-{scenario_name}.png"), dpi=200)
    plt.close(fig)


def plot_latency_timeseries_aggregated_plotly(
    aggregated_data: dict, output_dir: str, scenario_name: str,
) -> None:
    """Latency time-series with confidence bands (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    # Determine if we need to qualify labels with topology/fault
    topologies_seen = set()
    faults_seen = set()
    for (mode, topo, fault) in aggregated_data:
        topologies_seen.add(topo)
        faults_seen.add(fault)
    multi_topo = len(topologies_seen) > 1
    multi_fault = len(faults_seen - {""}) > 1

    fig = go.Figure()
    legend_seen: set[str] = set()
    for idx, (key, data) in enumerate(sorted(aggregated_data.items())):
        mode, topology, fault = key
        color = _color_for_mode(mode, idx)
        suffix_parts = []
        if multi_topo and topology:
            suffix_parts.append(topology)
        if multi_fault and fault and fault != "none":
            suffix_parts.append(fault)
        label = _label_for_mode(mode)
        if suffix_parts:
            label += f" ({', '.join(suffix_parts)})"
        show_legend = label not in legend_seen
        legend_seen.add(label)
        x = data["proposal_index"].tolist()

        # P5-P95 band
        fig.add_trace(go.Scatter(
            x=x + x[::-1],
            y=data["latency_p5"].tolist() + data["latency_p95"].tolist()[::-1],
            fill="toself", fillcolor=color, opacity=CONFIDENCE_ALPHA * 0.5,
            line=dict(width=0), showlegend=False, name=f"{label} P5-P95",
        ))
        # P25-P75 band
        fig.add_trace(go.Scatter(
            x=x + x[::-1],
            y=data["latency_p25"].tolist() + data["latency_p75"].tolist()[::-1],
            fill="toself", fillcolor=color, opacity=CONFIDENCE_ALPHA,
            line=dict(width=0), showlegend=False, name=f"{label} IQR",
        ))
        # Median line
        fig.add_trace(go.Scatter(
            x=x, y=data["latency_median"].tolist(),
            mode="lines", name=label, showlegend=show_legend,
            line=dict(color=color, width=2),
        ))

    fig.update_layout(
        title=f"Commit Latency \u2014 {scenario_name}",
        xaxis_title="Proposal Index", yaxis_title="Latency (ms)",
        height=500, template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, f"latency-{scenario_name}.html"))


# ---------------------------------------------------------------------------
# 6b  Contention spectrum chart
# ---------------------------------------------------------------------------

def plot_contention_spectrum(all_rows: list[dict], output_dir: str) -> None:
    """
    Contention spectrum (matplotlib).

    X-axis: number of proposers (contention level).
    Y-axis: median latency.
    Lines: one per mode with error bars (IQR).
    """
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    # Group by (mode, topology, fault_label) and count unique proposers
    groups: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    key_groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in all_rows:
        k = (row["mode"], row["topology"], row["fault_label"])
        key_groups[k].append(row)

    for (mode, _topo, _fault), rows in key_groups.items():
        n_proposers = len({r["proposer"] for r in rows})
        lats = [r["latency_ms"] for r in rows]
        groups[mode][n_proposers].extend(lats)

    fig, ax = plt.subplots(figsize=(10, 6))
    for idx, (mode, prop_data) in enumerate(sorted(groups.items())):
        xs = sorted(prop_data.keys())
        medians = [float(np.median(prop_data[x])) for x in xs]
        p25s = [float(np.percentile(prop_data[x], 25)) for x in xs]
        p75s = [float(np.percentile(prop_data[x], 75)) for x in xs]
        color = _color_for_mode(mode, idx)
        label = _label_for_mode(mode)

        yerr_low = [m - p for m, p in zip(medians, p25s)]
        yerr_high = [p - m for m, p in zip(medians, p75s)]
        ax.errorbar(
            xs, medians, yerr=[yerr_low, yerr_high],
            label=label, color=color, marker="o", capsize=4, linewidth=1.5,
        )

    ax.set_xlabel("Number of Proposers (Contention)")
    ax.set_ylabel("Median Latency (ms)")
    ax.set_title("Contention Spectrum", fontsize=14, fontweight="bold")
    ax.legend(framealpha=0.9)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "contention-spectrum.png"), dpi=200)
    plt.close(fig)


def plot_contention_spectrum_plotly(all_rows: list[dict], output_dir: str) -> None:
    """Contention spectrum (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    groups: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    key_groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in all_rows:
        k = (row["mode"], row["topology"], row["fault_label"])
        key_groups[k].append(row)

    for (mode, _topo, _fault), rows in key_groups.items():
        n_proposers = len({r["proposer"] for r in rows})
        groups[mode][n_proposers].extend([r["latency_ms"] for r in rows])

    fig = go.Figure()
    for idx, (mode, prop_data) in enumerate(sorted(groups.items())):
        xs = sorted(prop_data.keys())
        medians = [float(np.median(prop_data[x])) for x in xs]
        p25s = [float(np.percentile(prop_data[x], 25)) for x in xs]
        p75s = [float(np.percentile(prop_data[x], 75)) for x in xs]
        color = _color_for_mode(mode, idx)
        fig.add_trace(go.Scatter(
            x=xs, y=medians, mode="lines+markers",
            name=_label_for_mode(mode),
            line=dict(color=color, width=2),
            error_y=dict(
                type="data", symmetric=False,
                array=[p - m for m, p in zip(medians, p75s)],
                arrayminus=[m - p for m, p in zip(medians, p25s)],
            ),
        ))

    fig.update_layout(
        title="Contention Spectrum",
        xaxis_title="Number of Proposers", yaxis_title="Median Latency (ms)",
        height=500, template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, "contention-spectrum.html"))


# ---------------------------------------------------------------------------
# 6c  Failover timeline chart
# ---------------------------------------------------------------------------

def plot_failover_timeline(
    all_rows: list[dict],
    output_dir: str,
    scenario_name: str,
    fault_start_ms: float | None = None,
    fault_end_ms: float | None = None,
) -> None:
    """Failover timeline (matplotlib). Points coloured by proposer."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    # Infer fault window from failover events if not provided
    failover_rows = [r for r in all_rows if r.get("failover_count", 0) > 0]
    if fault_start_ms is None and failover_rows:
        fault_start_ms = min(r["sent_ms"] for r in failover_rows)
    if fault_end_ms is None and failover_rows:
        fault_end_ms = max(r["completed_ms"] for r in failover_rows)

    by_proposer = group_by(all_rows, "proposer")

    fig, ax = plt.subplots(figsize=(14, 6))

    # Fault window shading
    if fault_start_ms is not None and fault_end_ms is not None:
        ax.axvspan(fault_start_ms, fault_end_ms, color=FAULT_WINDOW_COLOR,
                   alpha=0.5, label="Fault Window")

    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS
    for idx, (proposer, rows) in enumerate(sorted(by_proposer.items())):
        color = colors[idx % len(colors)]
        sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
        xs = [r["sent_ms"] for r in sorted_rows]
        ys = [r["latency_ms"] for r in sorted_rows]
        ax.scatter(xs, ys, s=6, alpha=0.6, color=color, label=proposer)

        # Mark failover events
        fo = [r for r in sorted_rows if r.get("failover_count", 0) > 0]
        if fo:
            ax.scatter(
                [r["sent_ms"] for r in fo],
                [r["latency_ms"] for r in fo],
                s=40, marker="x", color="red", zorder=5,
                label="_nolegend_",
            )

    ax.set_xlabel("Simulated Time (ms)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"Failover Timeline \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    ax.legend(fontsize=9, framealpha=0.9, ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"failover-timeline-{scenario_name}.png"), dpi=200)
    plt.close(fig)


def plot_failover_timeline_plotly(
    all_rows: list[dict],
    output_dir: str,
    scenario_name: str,
    fault_start_ms: float | None = None,
    fault_end_ms: float | None = None,
) -> None:
    """Failover timeline (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    failover_rows = [r for r in all_rows if r.get("failover_count", 0) > 0]
    if fault_start_ms is None and failover_rows:
        fault_start_ms = min(r["sent_ms"] for r in failover_rows)
    if fault_end_ms is None and failover_rows:
        fault_end_ms = max(r["completed_ms"] for r in failover_rows)

    by_proposer = group_by(all_rows, "proposer")
    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    fig = go.Figure()
    if fault_start_ms is not None and fault_end_ms is not None:
        fig.add_vrect(
            x0=fault_start_ms, x1=fault_end_ms,
            fillcolor=FAULT_WINDOW_COLOR, opacity=0.4,
            line_width=0, annotation_text="Fault Window",
        )

    for idx, (proposer, rows) in enumerate(sorted(by_proposer.items())):
        color = colors[idx % len(colors)]
        sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
        fig.add_trace(go.Scatter(
            x=[r["sent_ms"] for r in sorted_rows],
            y=[r["latency_ms"] for r in sorted_rows],
            mode="markers", marker=dict(size=3, color=color, opacity=0.6),
            name=proposer,
        ))

    fig.update_layout(
        title=f"Failover Timeline \u2014 {scenario_name}",
        xaxis_title="Simulated Time (ms)", yaxis_title="Latency (ms)",
        height=500, template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, f"failover-timeline-{scenario_name}.html"))


# ---------------------------------------------------------------------------
# 6d  RTT distribution comparison
# ---------------------------------------------------------------------------

def plot_rtt_distribution(
    all_rows: list[dict], output_dir: str, scenario_name: str,
) -> None:
    """Grouped bar chart of RTT buckets per mode (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    by_mode = group_by(all_rows, "mode")
    buckets = ["0", "1", "2", "3", "4+"]

    def bucket_label(rt: int) -> str:
        return str(rt) if rt <= 3 else "4+"

    fig, ax = plt.subplots(figsize=(10, 6))
    bar_width = 0.8 / max(len(by_mode), 1)
    x_base = np.arange(len(buckets))

    for idx, (mode, rows) in enumerate(sorted(by_mode.items())):
        counts: dict[str, int] = {b: 0 for b in buckets}
        for r in rows:
            counts[bucket_label(r["round_trips"])] += 1
        total = max(sum(counts.values()), 1)
        pcts = [100.0 * counts[b] / total for b in buckets]
        color = _color_for_mode(mode, idx)
        ax.bar(
            x_base + idx * bar_width, pcts, bar_width,
            label=_label_for_mode(mode), color=color,
        )

    ax.set_xticks(x_base + bar_width * (len(by_mode) - 1) / 2)
    ax.set_xticklabels(buckets)
    ax.set_xlabel("Round Trips")
    ax.set_ylabel("% of Proposals")
    ax.set_title(f"RTT Distribution \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    ax.legend(framealpha=0.9)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"rtt-distribution-{scenario_name}.png"), dpi=200)
    plt.close(fig)


def plot_rtt_distribution_plotly(
    all_rows: list[dict], output_dir: str, scenario_name: str,
) -> None:
    """RTT distribution grouped bar chart (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    by_mode = group_by(all_rows, "mode")
    buckets = ["0", "1", "2", "3", "4+"]

    def bucket_label(rt: int) -> str:
        return str(rt) if rt <= 3 else "4+"

    fig = go.Figure()
    for idx, (mode, rows) in enumerate(sorted(by_mode.items())):
        counts: dict[str, int] = {b: 0 for b in buckets}
        for r in rows:
            counts[bucket_label(r["round_trips"])] += 1
        total = max(sum(counts.values()), 1)
        pcts = [100.0 * counts[b] / total for b in buckets]
        fig.add_trace(go.Bar(
            x=buckets, y=pcts, name=_label_for_mode(mode),
            marker_color=_color_for_mode(mode, idx),
        ))

    fig.update_layout(
        barmode="group",
        title=f"RTT Distribution \u2014 {scenario_name}",
        xaxis_title="Round Trips", yaxis_title="% of Proposals",
        height=500, template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, f"rtt-distribution-{scenario_name}.html"))


# ---------------------------------------------------------------------------
# 6e  Scenario summary dashboard
# ---------------------------------------------------------------------------

def plot_summary_dashboard(
    scenario_results: dict[str, list[dict]], output_dir: str,
) -> None:
    """Grid of small-multiple panels comparing modes per scenario (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    n = len(scenario_results)
    if n == 0:
        return
    cols = min(4, n)
    rows_count = math.ceil(n / cols)

    fig, axes = plt.subplots(
        rows_count, cols,
        figsize=(5 * cols, 4 * rows_count),
        squeeze=False,
    )

    for ax_idx, (scenario, rows) in enumerate(sorted(scenario_results.items())):
        r, c = divmod(ax_idx, cols)
        ax = axes[r][c]
        by_mode = group_by(rows, "mode")
        modes = sorted(by_mode.keys())
        medians = [float(np.median([x["latency_ms"] for x in by_mode[m]])) for m in modes]
        p25s = [float(np.percentile([x["latency_ms"] for x in by_mode[m]], 25)) for m in modes]
        p75s = [float(np.percentile([x["latency_ms"] for x in by_mode[m]], 75)) for m in modes]
        colors_list = [_color_for_mode(m, i) for i, m in enumerate(modes)]

        x = np.arange(len(modes))
        yerr_low = [m - p for m, p in zip(medians, p25s)]
        yerr_high = [p - m for m, p in zip(medians, p75s)]
        ax.bar(x, medians, color=colors_list, yerr=[yerr_low, yerr_high], capsize=3)
        ax.set_xticks(x)
        ax.set_xticklabels([_label_for_mode(m).split("(")[0].strip() for m in modes],
                           fontsize=7, rotation=30, ha="right")
        ax.set_title(scenario, fontsize=9)
        ax.set_ylabel("Median Lat. (ms)", fontsize=8)

    # Hide unused axes
    for ax_idx in range(n, rows_count * cols):
        r, c = divmod(ax_idx, cols)
        axes[r][c].set_visible(False)

    fig.suptitle("Scenario Summary Dashboard", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(os.path.join(output_dir, "summary-dashboard.png"), dpi=200)
    plt.close(fig)


def plot_summary_dashboard_plotly(
    scenario_results: dict[str, list[dict]], output_dir: str,
) -> None:
    """Scenario summary dashboard (plotly)."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return

    n = len(scenario_results)
    if n == 0:
        return
    cols = min(4, n)
    rows_count = math.ceil(n / cols)

    titles = sorted(scenario_results.keys())
    fig = make_subplots(rows=rows_count, cols=cols, subplot_titles=titles)

    for ax_idx, scenario in enumerate(titles):
        rows = scenario_results[scenario]
        r, c = divmod(ax_idx, cols)
        by_mode = group_by(rows, "mode")
        for m_idx, (mode, mrows) in enumerate(sorted(by_mode.items())):
            lats = [x["latency_ms"] for x in mrows]
            fig.add_trace(
                go.Bar(
                    x=[_label_for_mode(mode)],
                    y=[float(np.median(lats))],
                    marker_color=_color_for_mode(mode, m_idx),
                    name=_label_for_mode(mode),
                    showlegend=(ax_idx == 0),
                    legendgroup=mode,
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=[float(np.percentile(lats, 75) - np.median(lats))],
                        arrayminus=[float(np.median(lats) - np.percentile(lats, 25))],
                    ),
                ),
                row=r + 1, col=c + 1,
            )

    fig.update_layout(
        title="Scenario Summary Dashboard",
        height=350 * rows_count, template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, "summary-dashboard.html"))


# ---------------------------------------------------------------------------
# 6f  Topology x Mode heatmap
# ---------------------------------------------------------------------------

def plot_topology_mode_heatmap(all_rows: list[dict], output_dir: str) -> None:
    """Heatmap of median latency: X=topology, Y=mode (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    # Build matrix
    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in all_rows:
        groups[(r["mode"], r["topology"])].append(r["latency_ms"])

    modes = sorted({k[0] for k in groups})
    topos = sorted({k[1] for k in groups})

    if not modes or not topos:
        return

    matrix = np.full((len(modes), len(topos)), np.nan)
    for mi, mode in enumerate(modes):
        for ti, topo in enumerate(topos):
            lats = groups.get((mode, topo))
            if lats:
                matrix[mi, ti] = float(np.median(lats))

    fig, ax = plt.subplots(figsize=(max(8, 2 * len(topos)), max(4, 1.5 * len(modes))))
    im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd")

    ax.set_xticks(np.arange(len(topos)))
    ax.set_yticks(np.arange(len(modes)))
    ax.set_xticklabels(topos, rotation=45, ha="right")
    ax.set_yticklabels([_label_for_mode(m) for m in modes])

    # Annotate cells
    for mi in range(len(modes)):
        for ti in range(len(topos)):
            val = matrix[mi, ti]
            if not np.isnan(val):
                ax.text(ti, mi, f"{val:.1f}", ha="center", va="center",
                        color="white" if val > np.nanmedian(matrix) else "black",
                        fontsize=10, fontweight="bold")

    fig.colorbar(im, ax=ax, label="Median Latency (ms)")
    ax.set_title("Topology \u00d7 Mode Heatmap", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "topology-mode-heatmap.png"), dpi=200)
    plt.close(fig)


def plot_topology_mode_heatmap_plotly(all_rows: list[dict], output_dir: str) -> None:
    """Topology x Mode heatmap (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    groups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in all_rows:
        groups[(r["mode"], r["topology"])].append(r["latency_ms"])

    modes = sorted({k[0] for k in groups})
    topos = sorted({k[1] for k in groups})
    if not modes or not topos:
        return

    z = []
    annotations = []
    for mi, mode in enumerate(modes):
        row_vals = []
        for ti, topo in enumerate(topos):
            lats = groups.get((mode, topo))
            val = float(np.median(lats)) if lats else float("nan")
            row_vals.append(val)
            if not np.isnan(val):
                annotations.append(f"{val:.1f}")
            else:
                annotations.append("")
        z.append(row_vals)

    fig = go.Figure(data=go.Heatmap(
        z=z, x=topos, y=[_label_for_mode(m) for m in modes],
        colorscale="YlOrRd", text=[[f"{v:.1f}" if not np.isnan(v) else "" for v in row] for row in z],
        texttemplate="%{text}", textfont=dict(size=12),
    ))
    fig.update_layout(
        title="Topology \u00d7 Mode Heatmap",
        xaxis_title="Topology", yaxis_title="Mode",
        height=max(400, 80 * len(modes)), template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, "topology-mode-heatmap.html"))


# ---------------------------------------------------------------------------
# 6g  Per-proposer latency traces
# ---------------------------------------------------------------------------

def plot_per_proposer_traces(
    all_rows: list[dict],
    output_dir: str,
    scenario_name: str,
    fault_start_idx: int | None = None,
    fault_end_idx: int | None = None,
) -> None:
    """Side-by-side panels of per-proposer latency traces (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    by_mode = group_by(all_rows, "mode")
    n_modes = max(len(by_mode), 1)
    fig, axes = plt.subplots(1, n_modes, figsize=(7 * n_modes, 5), squeeze=False)

    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    for mi, (mode, mode_rows) in enumerate(sorted(by_mode.items())):
        ax = axes[0][mi]
        by_proposer = group_by(mode_rows, "proposer")

        # Determine fault window in time domain from failover events
        failover_times = [r["sent_ms"] for r in mode_rows if r.get("failover_count", 0) > 0]
        if failover_times:
            ax.axvspan(min(failover_times), max(failover_times),
                       color=FAULT_WINDOW_COLOR, alpha=0.5, label="Fault Window")

        for pidx, (proposer, rows) in enumerate(sorted(by_proposer.items())):
            sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
            ax.plot(
                [r["sent_ms"] for r in sorted_rows],
                [r["latency_ms"] for r in sorted_rows],
                linewidth=0.8, alpha=0.7,
                color=colors[pidx % len(colors)],
                label=proposer,
            )

        ax.set_xlabel("Simulated Time (ms)")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(_label_for_mode(mode), fontsize=12)
        ax.legend(fontsize=7, framealpha=0.9, ncol=2)

    fig.suptitle(
        f"Per-Proposer Latency Traces \u2014 {scenario_name}",
        fontsize=14, fontweight="bold",
    )
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(
        os.path.join(output_dir, f"per-proposer-traces-{scenario_name}.png"), dpi=200,
    )
    plt.close(fig)


def plot_per_proposer_traces_plotly(
    all_rows: list[dict],
    output_dir: str,
    scenario_name: str,
    fault_start_idx: int | None = None,
    fault_end_idx: int | None = None,
) -> None:
    """Per-proposer latency traces (plotly)."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return

    by_mode = group_by(all_rows, "mode")
    modes = sorted(by_mode.keys())
    n_modes = max(len(modes), 1)

    fig = make_subplots(rows=1, cols=n_modes, subplot_titles=[_label_for_mode(m) for m in modes])
    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    for mi, mode in enumerate(modes):
        mode_rows = by_mode[mode]
        by_proposer = group_by(mode_rows, "proposer")

        failover_times = [r["sent_ms"] for r in mode_rows if r.get("failover_count", 0) > 0]
        if failover_times:
            fig.add_vrect(
                x0=min(failover_times), x1=max(failover_times),
                fillcolor=FAULT_WINDOW_COLOR, opacity=0.4,
                line_width=0, row=1, col=mi + 1,
            )

        for pidx, (proposer, rows) in enumerate(sorted(by_proposer.items())):
            sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
            fig.add_trace(
                go.Scatter(
                    x=[r["sent_ms"] for r in sorted_rows],
                    y=[r["latency_ms"] for r in sorted_rows],
                    mode="lines", name=proposer,
                    line=dict(color=colors[pidx % len(colors)], width=1),
                    showlegend=(mi == 0),
                    legendgroup=proposer,
                ),
                row=1, col=mi + 1,
            )

    fig.update_layout(
        title=f"Per-Proposer Latency Traces \u2014 {scenario_name}",
        height=500, template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, f"per-proposer-traces-{scenario_name}.html"))


# ---------------------------------------------------------------------------
# Original chart functions (preserved for non-comparative mode)
# ---------------------------------------------------------------------------

def plot_matplotlib(all_rows: list[dict], output_dir: str):
    """Generate static PNG charts with matplotlib."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("matplotlib not installed. Install with: pip install matplotlib numpy", file=sys.stderr)
        return

    configure_matplotlib_style()

    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    # Group by topology x fault for faceting
    facet_key = lambda r: f"{r['topology']}_{r['fault_label']}"
    facets = defaultdict(list)
    for row in all_rows:
        facets[facet_key(row)].append(row)

    for facet_name, facet_rows in sorted(facets.items()):
        by_mode = group_by(facet_rows, "mode")

        # 1. Latency time-series
        fig, ax = plt.subplots(figsize=(12, 6))
        for idx, (mode, rows) in enumerate(sorted(by_mode.items())):
            sorted_rows = sorted(rows, key=lambda r: r["completed_ms"])
            x = list(range(len(sorted_rows)))
            y = [r["latency_ms"] for r in sorted_rows]
            ax.plot(x, y, label=_label_for_mode(mode), color=_color_for_mode(mode, idx), alpha=0.8, linewidth=0.8)
        ax.set_xlabel("Proposal Index")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"Commit Latency Over Time \u2014 {facet_name}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(charts_dir, f"latency-timeseries-{facet_name}.png"), dpi=150)
        plt.close(fig)

        # 2. Throughput
        fig, ax = plt.subplots(figsize=(12, 6))
        for idx, (mode, rows) in enumerate(sorted(by_mode.items())):
            times, throughputs = sliding_window_throughput(rows, window_ms=200.0)
            if times:
                ax.plot(times, throughputs, label=_label_for_mode(mode), color=_color_for_mode(mode, idx), alpha=0.8, linewidth=0.8)
        ax.set_xlabel("Simulated Time (ms)")
        ax.set_ylabel("Throughput (proposals/sec)")
        ax.set_title(f"Throughput Over Time \u2014 {facet_name}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(charts_dir, f"throughput-{facet_name}.png"), dpi=150)
        plt.close(fig)

        # 3. Latency CDF
        fig, ax = plt.subplots(figsize=(10, 6))
        for idx, (mode, rows) in enumerate(sorted(by_mode.items())):
            latencies = sorted([r["latency_ms"] for r in rows])
            n = len(latencies)
            cdf = [(i + 1) / n for i in range(n)]
            ax.plot(latencies, cdf, label=_label_for_mode(mode), color=_color_for_mode(mode, idx), linewidth=1.2)
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("CDF")
        ax.set_title(f"Latency CDF \u2014 {facet_name}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(charts_dir, f"latency-cdf-{facet_name}.png"), dpi=150)
        plt.close(fig)

    # 4. Cross-topology comparison (latency box plot per mode)
    by_topo = group_by(all_rows, "topology")
    if len(by_topo) > 1:
        fig, axes = plt.subplots(1, len(by_topo), figsize=(6 * len(by_topo), 6), sharey=False)
        if len(by_topo) == 1:
            axes = [axes]
        for ax, (topo, topo_rows) in zip(axes, sorted(by_topo.items())):
            by_mode = group_by(topo_rows, "mode")
            data = []
            labels = []
            for mode, rows in sorted(by_mode.items()):
                data.append([r["latency_ms"] for r in rows])
                labels.append(mode)
            ax.boxplot(data, labels=labels, showfliers=False)
            ax.set_title(f"Topology: {topo}")
            ax.set_ylabel("Latency (ms)")
            ax.grid(True, alpha=0.3)
        fig.suptitle("Latency Distribution by Topology and Protocol Mode")
        fig.tight_layout()
        fig.savefig(os.path.join(charts_dir, "latency-boxplot-comparison.png"), dpi=150)
        plt.close(fig)

    print(f"matplotlib charts written to {charts_dir}")


def plot_plotly(all_rows: list[dict], output_dir: str):
    """Generate interactive HTML charts with plotly."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        print("plotly not installed. Install with: pip install plotly", file=sys.stderr)
        return

    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    facet_key = lambda r: f"{r['topology']}_{r['fault_label']}"
    facets = defaultdict(list)
    for row in all_rows:
        facets[facet_key(row)].append(row)

    for facet_name, facet_rows in sorted(facets.items()):
        by_mode = group_by(facet_rows, "mode")

        # Combined interactive chart: latency + throughput + CDF
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Commit Latency Over Time",
                "Latency CDF",
                "Throughput Over Time",
                "Latency Distribution",
            ],
        )

        for i, (mode, rows) in enumerate(sorted(by_mode.items())):
            color = _color_for_mode(mode, i)
            label = _label_for_mode(mode)
            sorted_rows = sorted(rows, key=lambda r: r["completed_ms"])

            # Latency time-series
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(sorted_rows))),
                    y=[r["latency_ms"] for r in sorted_rows],
                    mode="lines",
                    name=label,
                    line=dict(color=color, width=1),
                    legendgroup=mode,
                ),
                row=1, col=1,
            )

            # Latency CDF
            latencies = sorted([r["latency_ms"] for r in rows])
            n = len(latencies)
            fig.add_trace(
                go.Scatter(
                    x=latencies,
                    y=[(j + 1) / n for j in range(n)],
                    mode="lines",
                    name=label,
                    line=dict(color=color, width=1.5),
                    legendgroup=mode,
                    showlegend=False,
                ),
                row=1, col=2,
            )

            # Throughput
            times, throughputs = sliding_window_throughput(rows, window_ms=200.0)
            if times:
                fig.add_trace(
                    go.Scatter(
                        x=times,
                        y=throughputs,
                        mode="lines",
                        name=label,
                        line=dict(color=color, width=1),
                        legendgroup=mode,
                        showlegend=False,
                    ),
                    row=2, col=1,
                )

            # Box plot
            fig.add_trace(
                go.Box(
                    y=[r["latency_ms"] for r in rows],
                    name=label,
                    marker_color=color,
                    legendgroup=mode,
                    showlegend=False,
                ),
                row=2, col=2,
            )

        fig.update_layout(
            title_text=f"Benchmark: {facet_name}",
            height=800,
            showlegend=True,
        )
        fig.update_xaxes(title_text="Proposal Index", row=1, col=1)
        fig.update_yaxes(title_text="Latency (ms)", row=1, col=1)
        fig.update_xaxes(title_text="Latency (ms)", row=1, col=2)
        fig.update_yaxes(title_text="CDF", row=1, col=2)
        fig.update_xaxes(title_text="Simulated Time (ms)", row=2, col=1)
        fig.update_yaxes(title_text="Proposals/sec", row=2, col=1)
        fig.update_yaxes(title_text="Latency (ms)", row=2, col=2)

        fig.write_html(os.path.join(charts_dir, f"benchmark-{facet_name}.html"))

    print(f"plotly charts written to {charts_dir}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot Fast CASPaxos benchmark results")
    parser.add_argument("--input", required=True, help="Root directory containing benchmark output")
    parser.add_argument(
        "--format",
        choices=["png", "html", "both"],
        default="both",
        help="Output format: png (matplotlib), html (plotly), or both (default)",
    )
    parser.add_argument(
        "--comparative",
        action="store_true",
        help="Comparative benchmark mode: aggregate across rounds, generate scenario-specific charts",
    )
    parser.add_argument(
        "--smoothing-window",
        type=int,
        default=20,
        help="Rolling window size for smoothing (default: 20)",
    )
    args = parser.parse_args()

    csv_files = find_csv_files(args.input)
    if not csv_files:
        print(f"No benchmark-latency.csv files found in {args.input}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_files)} benchmark CSV file(s)")

    if args.comparative:
        # ---------------------------------------------------------------
        # Comparative mode: organise by scenario directory structure
        # ---------------------------------------------------------------
        scenario_dirs = discover_comparative_structure(args.input)
        charts_dir = os.path.join(args.input, "charts")
        os.makedirs(charts_dir, exist_ok=True)

        all_scenario_rows: dict[str, list[dict]] = {}

        for scenario_name, scenario_csvs in sorted(scenario_dirs.items()):
            rows: list[dict] = []
            for csv_file in scenario_csvs:
                # Derive a round id from the directory name
                round_id = csv_file.parent.name
                rows.extend(load_csv(csv_file, round_id=round_id))

            if not rows:
                continue

            all_scenario_rows[scenario_name] = rows
            print(f"  Scenario {scenario_name}: {len(rows)} samples from {len(scenario_csvs)} CSV(s)")

            # Aggregate across rounds
            aggregated = aggregate_multi_run(rows, smoothing_window=args.smoothing_window)

            # Generate charts (PNG and/or HTML based on --format)
            if args.format in ("png", "both"):
                plot_latency_timeseries_aggregated(aggregated, charts_dir, scenario_name)
                plot_rtt_distribution(rows, charts_dir, scenario_name)

                if "contention" in scenario_name.lower():
                    plot_contention_spectrum(rows, charts_dir)
                if "gray-failure" in scenario_name.lower() or "crash-failover" in scenario_name.lower():
                    plot_per_proposer_traces(rows, charts_dir, scenario_name)
                    plot_failover_timeline(rows, charts_dir, scenario_name)

            if args.format in ("html", "both"):
                plot_latency_timeseries_aggregated_plotly(aggregated, charts_dir, scenario_name)
                plot_rtt_distribution_plotly(rows, charts_dir, scenario_name)

                if "contention" in scenario_name.lower():
                    plot_contention_spectrum_plotly(rows, charts_dir)
                if "gray-failure" in scenario_name.lower() or "crash-failover" in scenario_name.lower():
                    plot_per_proposer_traces_plotly(rows, charts_dir, scenario_name)
                    plot_failover_timeline_plotly(rows, charts_dir, scenario_name)

        # Cross-scenario summary
        if all_scenario_rows:
            if args.format in ("png", "both"):
                plot_summary_dashboard(all_scenario_rows, charts_dir)
            if args.format in ("html", "both"):
                plot_summary_dashboard_plotly(all_scenario_rows, charts_dir)

        # Topology heatmap (from geo-low-contention data)
        geo_key = None
        for k in all_scenario_rows:
            if "geo-low-contention" in k.lower():
                geo_key = k
                break
        if geo_key is not None:
            geo_rows = all_scenario_rows[geo_key]
            if args.format in ("png", "both"):
                plot_topology_mode_heatmap(geo_rows, charts_dir)
            if args.format in ("html", "both"):
                plot_topology_mode_heatmap_plotly(geo_rows, charts_dir)

        print(f"Comparative charts written to {charts_dir}")
    else:
        # ---------------------------------------------------------------
        # Original single-run behaviour (preserved)
        # ---------------------------------------------------------------
        all_rows: list[dict] = []
        for csv_file in csv_files:
            rows = load_csv(csv_file)
            print(f"  {csv_file}: {len(rows)} samples")
            all_rows.extend(rows)

        print(f"Total: {len(all_rows)} samples")

        if args.format in ("png", "both"):
            plot_matplotlib(all_rows, args.input)

        if args.format in ("html", "both"):
            plot_plotly(all_rows, args.input)


if __name__ == "__main__":
    main()
