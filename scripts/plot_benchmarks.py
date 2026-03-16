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
  - Scenario-specific contention spectrum charts
  - Scenario-specific failover timeline charts
  - RTT distribution comparisons
  - Scenario-specific topology x mode heatmaps
  - Combined scenario x mode heatmap across all scenarios
  - Scenario-specific per-proposer latency traces

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

SERIES_COLORS = list(MODE_COLORS.values()) + _EXTRA_COLORS

COMPARATIVE_CONFIG_FIELDS = (
    "mode",
    "topology",
    "fault_label",
    "proposer_count",
    "acceptor_count",
    "agreement_mode",
    "register_init_mode",
    "write_interval_ms",
    "serial_random_proposer",
    "client_timeout_ms",
    "client_max_retries",
    "proposer_timing_source",
    "proposer_expected_rtt_ms",
    "proposer_retry_backoff_start_after_failures",
    "proposer_retry_backoff_base_ms",
    "proposer_retry_backoff_max_ms",
    "proposer_retry_backoff_jitter_ratio",
    "proposer_response_timeout_ms",
)


def _color_for_mode(mode: str, idx: int = 0) -> str:
    """Return a color for a protocol mode, falling back to extras."""
    return MODE_COLORS.get(mode, _EXTRA_COLORS[idx % len(_EXTRA_COLORS)])


def _label_for_mode(mode: str) -> str:
    """Return a human-friendly label for a protocol mode."""
    return MODE_LABELS.get(mode, mode)


def _color_for_series(idx: int) -> str:
    """Return a stable color for a chart series."""
    return SERIES_COLORS[idx % len(SERIES_COLORS)]


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
            def parse_int(name: str, default: int = 0) -> int:
                value = row.get(name)
                if value in (None, ""):
                    return default
                try:
                    return int(value)
                except ValueError:
                    return default

            def parse_bool(name: str) -> bool:
                return str(row.get(name, "")).strip().lower() == "true"

            def parse_float(name: str, default: float = 0.0) -> float:
                raw = str(row.get(name, "")).strip()
                if raw == "":
                    return default
                return float(raw)

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
                "proposer_count": parse_int("proposer_count"),
                "acceptor_count": parse_int("acceptor_count", default=5),
                "agreement_mode": parse_bool("agreement_mode"),
                "register_init_mode": parse_bool("register_init_mode"),
                "write_interval_ms": parse_int("write_interval_ms"),
                "serial_random_proposer": parse_bool("serial_random_proposer"),
                "client_timeout_ms": parse_int("client_timeout_ms"),
                "client_max_retries": parse_int("client_max_retries", default=2),
                "proposer_timing_source": row.get("proposer_timing_source", ""),
                "proposer_expected_rtt_ms": parse_float("proposer_expected_rtt_ms"),
                "proposer_retry_backoff_start_after_failures": parse_int("proposer_retry_backoff_start_after_failures"),
                "proposer_retry_backoff_base_ms": parse_float("proposer_retry_backoff_base_ms"),
                "proposer_retry_backoff_max_ms": parse_float("proposer_retry_backoff_max_ms"),
                "proposer_retry_backoff_jitter_ratio": parse_float("proposer_retry_backoff_jitter_ratio"),
                "proposer_response_timeout_ms": parse_float("proposer_response_timeout_ms"),
                "_round_id": round_id,
            })
    return rows


def group_by(rows: list[dict], key: str) -> dict[str, list[dict]]:
    """Group rows by a key field."""
    groups = defaultdict(list)
    for row in rows:
        groups[row[key]].append(row)
    return dict(groups)


def comparative_config_key(
    row: dict,
    *,
    exclude_fields: set[str] | None = None,
) -> tuple:
    """Build a grouping key for a comparative benchmark configuration."""
    excluded = exclude_fields or set()
    return tuple(
        row.get(field)
        for field in COMPARATIVE_CONFIG_FIELDS
        if field not in excluded
    )


def build_comparative_label_map(
    rows: list[dict],
    *,
    exclude_fields: set[str] | None = None,
) -> dict[tuple, str]:
    """Build human-readable labels for comparative benchmark configurations."""
    excluded = exclude_fields or set()
    unique_configs: dict[tuple, dict] = {}
    for row in rows:
        key = comparative_config_key(row, exclude_fields=excluded)
        unique_configs.setdefault(key, row)

    if not unique_configs:
        return {}

    varying_fields: set[str] = set()
    for field in COMPARATIVE_CONFIG_FIELDS:
        if field == "mode" or field in excluded:
            continue
        values = {config_row.get(field) for config_row in unique_configs.values()}
        if len(values) > 1:
            varying_fields.add(field)

    labels: dict[tuple, str] = {}
    for key, row in unique_configs.items():
        parts = [_label_for_mode(row.get("mode", ""))]

        if "topology" in varying_fields and row.get("topology"):
            parts.append(f"topology={row['topology']}")
        if "fault_label" in varying_fields:
            parts.append(f"fault={row.get('fault_label') or 'none'}")
        if "proposer_count" in varying_fields:
            parts.append(f"proposers={row.get('proposer_count', 0)}")
        if "acceptor_count" in varying_fields:
            parts.append(f"acceptors={row.get('acceptor_count', 0)}")
        if "agreement_mode" in varying_fields:
            parts.append(f"agreement={'true' if row.get('agreement_mode') else 'false'}")
        if "register_init_mode" in varying_fields:
            parts.append(f"register-init={'true' if row.get('register_init_mode') else 'false'}")
        if "write_interval_ms" in varying_fields:
            parts.append(f"interval={row.get('write_interval_ms', 0)}ms")
        if "client_timeout_ms" in varying_fields:
            parts.append(f"timeout={row.get('client_timeout_ms', 0)}ms")
        if "client_max_retries" in varying_fields:
            parts.append(f"retries={row.get('client_max_retries', 0)}")
        if "proposer_timing_source" in varying_fields and row.get("proposer_timing_source"):
            parts.append(f"timing={row.get('proposer_timing_source')}")
        if "proposer_expected_rtt_ms" in varying_fields:
            parts.append(f"rtt={row.get('proposer_expected_rtt_ms', 0):g}ms")
        if "proposer_retry_backoff_start_after_failures" in varying_fields:
            parts.append(f"retry-start={row.get('proposer_retry_backoff_start_after_failures', 0)}")
        if "proposer_retry_backoff_base_ms" in varying_fields:
            parts.append(f"retry-base={row.get('proposer_retry_backoff_base_ms', 0):g}ms")
        if "proposer_retry_backoff_max_ms" in varying_fields:
            parts.append(f"retry-max={row.get('proposer_retry_backoff_max_ms', 0):g}ms")
        if "proposer_retry_backoff_jitter_ratio" in varying_fields:
            parts.append(f"retry-jitter={row.get('proposer_retry_backoff_jitter_ratio', 0):g}")
        if "proposer_response_timeout_ms" in varying_fields:
            parts.append(f"resp-timeout={row.get('proposer_response_timeout_ms', 0):g}ms")

        labels[key] = ", ".join(parts)

    return labels


def _sub_group_key_no_mode(row: dict) -> tuple:
    """Sub-grouping key: all comparative config fields except mode."""
    return tuple(
        row.get(field)
        for field in COMPARATIVE_CONFIG_FIELDS
        if field != "mode"
    )


def _build_sub_group_suffix(
    sub_group_rows: list[dict], all_scenario_rows: list[dict],
) -> str:
    """Build a compact descriptive suffix from non-mode fields that vary within the scenario."""
    varying: list[tuple[str, str]] = []
    sample = sub_group_rows[0]
    for field in COMPARATIVE_CONFIG_FIELDS:
        if field == "mode":
            continue
        values = {r.get(field) for r in all_scenario_rows}
        if len(values) <= 1:
            continue
        val = sample.get(field)
        if field == "topology":
            varying.append(("", str(val)))
        elif field == "proposer_count":
            varying.append(("p", str(val)))
        elif field == "acceptor_count":
            varying.append(("a", str(val)))
        elif field == "agreement_mode":
            varying.append(("agree-", "true" if val else "false"))
        elif field == "register_init_mode":
            varying.append(("reginit-", "true" if val else "false"))
        elif field == "write_interval_ms":
            varying.append(("interval-", f"{val}ms"))
        elif field == "fault_label":
            varying.append(("fault-", str(val or "none")))
        else:
            varying.append((f"{field.replace('_', '-')}-", str(val)))

    if not varying:
        return ""
    return "-".join(f"{prefix}{val}" for prefix, val in varying)


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

    Groups by the full benchmark configuration, aligns by proposal index within
    each group, and computes statistics.

    Returns dict keyed by the comparative configuration tuple -> {
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
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        key = comparative_config_key(row)
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
            "label": labels.get(key, _label_for_mode(rows[0].get("mode", ""))),
            "mode": rows[0].get("mode", ""),
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

    fig, ax = plt.subplots(figsize=(14, 6))
    for idx, (_key, data) in enumerate(sorted(aggregated_data.items(), key=lambda item: item[1]["label"])):
        mode = data.get("mode", "")
        color = _color_for_mode(mode, idx) if mode else _color_for_series(idx)
        x = data["proposal_index"]
        ax.plot(x, data["latency_median"], label=data["label"], color=color, linewidth=1.5)
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
    ax.set_ylim(bottom=0)
    ax.set_title(f"Commit Latency \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    ax.legend(fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"{scenario_name}-commit-latency.png"), dpi=200)
    plt.close(fig)


def plot_latency_timeseries_aggregated_plotly(
    aggregated_data: dict, output_dir: str, scenario_name: str,
) -> None:
    """Latency time-series with confidence bands (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    fig = go.Figure()
    for idx, (_key, data) in enumerate(sorted(aggregated_data.items(), key=lambda item: item[1]["label"])):
        mode = data.get("mode", "")
        color = _color_for_mode(mode, idx) if mode else _color_for_series(idx)
        label = data["label"]
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
            mode="lines", name=label, showlegend=True,
            line=dict(color=color, width=2),
        ))

    fig.update_layout(
        title=f"Commit Latency \u2014 {scenario_name}",
        xaxis_title="Proposal Index", yaxis_title="Latency (ms)",
        height=500, template="plotly_white",
    )
    fig.update_yaxes(rangemode="tozero")
    fig.write_html(os.path.join(output_dir, f"{scenario_name}-commit-latency.html"))


# ---------------------------------------------------------------------------
# 6b  Contention spectrum chart
# ---------------------------------------------------------------------------

def plot_contention_spectrum(all_rows: list[dict], output_dir: str, scenario_name: str) -> None:
    """
    Contention spectrum (matplotlib).

    X-axis: number of proposers (contention level).
    Y-axis: median latency.
    Lines: one per mode with error bars (IQR).
    """
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    # Group by configuration excluding proposer-count, which becomes the x-axis.
    groups: dict[tuple, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    labels = build_comparative_label_map(all_rows, exclude_fields={"proposer_count"})
    key_groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in all_rows:
        k = comparative_config_key(row, exclude_fields={"proposer_count"})
        key_groups[k].append(row)

    for key, rows in key_groups.items():
        n_proposers = rows[0].get("proposer_count") or len({r["proposer"] for r in rows})
        groups[key][n_proposers].extend([r["latency_ms"] for r in rows])

    fig, ax = plt.subplots(figsize=(10, 6))
    for idx, (key, prop_data) in enumerate(sorted(groups.items(), key=lambda item: labels.get(item[0], ""))):
        xs = sorted(prop_data.keys())
        medians = [float(np.median(prop_data[x])) for x in xs]
        p25s = [float(np.percentile(prop_data[x], 25)) for x in xs]
        p75s = [float(np.percentile(prop_data[x], 75)) for x in xs]
        color = _color_for_series(idx)
        label = labels.get(key, "configuration")

        yerr_low = [m - p for m, p in zip(medians, p25s)]
        yerr_high = [p - m for m, p in zip(medians, p75s)]
        ax.errorbar(
            xs, medians, yerr=[yerr_low, yerr_high],
            label=label, color=color, marker="o", capsize=4, linewidth=1.5,
        )

    ax.set_xlabel("Number of Proposers (Contention)")
    ax.set_ylabel("Median Latency (ms)")
    ax.set_ylim(bottom=0)
    ax.set_title(f"Contention Spectrum — {scenario_name}", fontsize=14, fontweight="bold")
    ax.legend(framealpha=0.9)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"{scenario_name}-contention-spectrum.png"), dpi=200)
    plt.close(fig)


def plot_contention_spectrum_plotly(all_rows: list[dict], output_dir: str, scenario_name: str) -> None:
    """Contention spectrum (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    groups: dict[tuple, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    labels = build_comparative_label_map(all_rows, exclude_fields={"proposer_count"})
    key_groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in all_rows:
        k = comparative_config_key(row, exclude_fields={"proposer_count"})
        key_groups[k].append(row)

    for key, rows in key_groups.items():
        n_proposers = rows[0].get("proposer_count") or len({r["proposer"] for r in rows})
        groups[key][n_proposers].extend([r["latency_ms"] for r in rows])

    fig = go.Figure()
    for idx, (key, prop_data) in enumerate(sorted(groups.items(), key=lambda item: labels.get(item[0], ""))):
        xs = sorted(prop_data.keys())
        medians = [float(np.median(prop_data[x])) for x in xs]
        p25s = [float(np.percentile(prop_data[x], 25)) for x in xs]
        p75s = [float(np.percentile(prop_data[x], 75)) for x in xs]
        color = _color_for_series(idx)
        fig.add_trace(go.Scatter(
            x=xs, y=medians, mode="lines+markers",
            name=labels.get(key, "configuration"),
            line=dict(color=color, width=2),
            error_y=dict(
                type="data", symmetric=False,
                array=[p - m for m, p in zip(medians, p75s)],
                arrayminus=[m - p for m, p in zip(medians, p25s)],
            ),
        ))

    fig.update_layout(
        title=f"Contention Spectrum — {scenario_name}",
        xaxis_title="Number of Proposers", yaxis_title="Median Latency (ms)",
        height=500, template="plotly_white",
    )
    fig.update_yaxes(rangemode="tozero")
    fig.write_html(os.path.join(output_dir, f"{scenario_name}-contention-spectrum.html"))


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
    """Failover timeline (matplotlib), one panel per configuration."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    by_config: dict[tuple, list[dict]] = defaultdict(list)
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        by_config[comparative_config_key(row)].append(row)

    config_items = sorted(by_config.items(), key=lambda item: labels.get(item[0], ""))
    fig, axes = plt.subplots(1, max(len(config_items), 1), figsize=(7 * max(len(config_items), 1), 5), squeeze=False)
    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    for config_idx, (key, config_rows) in enumerate(config_items):
        ax = axes[0][config_idx]
        config_failover_rows = [r for r in config_rows if r.get("failover_count", 0) > 0]
        config_fault_start_ms = fault_start_ms
        config_fault_end_ms = fault_end_ms
        if config_fault_start_ms is None and config_failover_rows:
            config_fault_start_ms = min(r["sent_ms"] for r in config_failover_rows)
        if config_fault_end_ms is None and config_failover_rows:
            config_fault_end_ms = max(r["completed_ms"] for r in config_failover_rows)

        if config_fault_start_ms is not None and config_fault_end_ms is not None:
            ax.axvspan(
                config_fault_start_ms,
                config_fault_end_ms,
                color=FAULT_WINDOW_COLOR,
                alpha=0.5,
                label="Fault Window",
            )

        by_proposer = group_by(config_rows, "proposer")
        for idx, (proposer, rows) in enumerate(sorted(by_proposer.items())):
            color = colors[idx % len(colors)]
            sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
            ax.scatter(
                [r["sent_ms"] for r in sorted_rows],
                [r["latency_ms"] for r in sorted_rows],
                s=6,
                alpha=0.6,
                color=color,
                label=proposer,
            )

            failover_samples = [r for r in sorted_rows if r.get("failover_count", 0) > 0]
            if failover_samples:
                ax.scatter(
                    [r["sent_ms"] for r in failover_samples],
                    [r["latency_ms"] for r in failover_samples],
                    s=40,
                    marker="x",
                    color="red",
                    zorder=5,
                    label="_nolegend_",
                )

        ax.set_xlabel("Simulated Time (ms)")
        ax.set_ylabel("Latency (ms)")
        ax.set_ylim(bottom=0)
        ax.set_title(labels.get(key, "configuration"), fontsize=12)
        ax.legend(fontsize=8, framealpha=0.9, ncol=2)

    fig.suptitle(f"Failover Timeline \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(os.path.join(output_dir, f"{scenario_name}-failover-timeline.png"), dpi=200)
    plt.close(fig)


def plot_failover_timeline_plotly(
    all_rows: list[dict],
    output_dir: str,
    scenario_name: str,
    fault_start_ms: float | None = None,
    fault_end_ms: float | None = None,
) -> None:
    """Failover timeline (plotly), one panel per configuration."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        return

    by_config: dict[tuple, list[dict]] = defaultdict(list)
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        by_config[comparative_config_key(row)].append(row)

    config_items = sorted(by_config.items(), key=lambda item: labels.get(item[0], ""))
    fig = make_subplots(
        rows=1,
        cols=max(len(config_items), 1),
        subplot_titles=[labels.get(key, "configuration") for key, _rows in config_items],
    )
    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    for config_idx, (_key, config_rows) in enumerate(config_items):
        config_failover_rows = [r for r in config_rows if r.get("failover_count", 0) > 0]
        config_fault_start_ms = fault_start_ms
        config_fault_end_ms = fault_end_ms
        if config_fault_start_ms is None and config_failover_rows:
            config_fault_start_ms = min(r["sent_ms"] for r in config_failover_rows)
        if config_fault_end_ms is None and config_failover_rows:
            config_fault_end_ms = max(r["completed_ms"] for r in config_failover_rows)

        if config_fault_start_ms is not None and config_fault_end_ms is not None:
            fig.add_vrect(
                x0=config_fault_start_ms,
                x1=config_fault_end_ms,
                fillcolor=FAULT_WINDOW_COLOR,
                opacity=0.4,
                line_width=0,
                row=1,
                col=config_idx + 1,
            )

        by_proposer = group_by(config_rows, "proposer")
        for idx, (proposer, rows) in enumerate(sorted(by_proposer.items())):
            color = colors[idx % len(colors)]
            sorted_rows = sorted(rows, key=lambda r: r["sent_ms"])
            fig.add_trace(go.Scatter(
                x=[r["sent_ms"] for r in sorted_rows],
                y=[r["latency_ms"] for r in sorted_rows],
                mode="markers",
                marker=dict(size=3, color=color, opacity=0.6),
                name=proposer,
                showlegend=(config_idx == 0),
                legendgroup=proposer,
            ), row=1, col=config_idx + 1)

    fig.update_layout(
        title=f"Failover Timeline \u2014 {scenario_name}",
        height=500, template="plotly_white",
    )
    fig.update_yaxes(rangemode="tozero")
    fig.write_html(os.path.join(output_dir, f"{scenario_name}-failover-timeline.html"))


# ---------------------------------------------------------------------------
# 6d  RTT distribution comparison
# ---------------------------------------------------------------------------

def plot_rtt_distribution(
    all_rows: list[dict], output_dir: str, scenario_name: str,
) -> None:
    """Grouped bar chart of RTT buckets per configuration (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    by_config: dict[tuple, list[dict]] = defaultdict(list)
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        by_config[comparative_config_key(row)].append(row)
    buckets = ["0", "1", "2", "3", "4+"]

    def bucket_label(rt: int) -> str:
        return str(rt) if rt <= 3 else "4+"

    fig, ax = plt.subplots(figsize=(10, 6))
    bar_width = 0.8 / max(len(by_config), 1)
    x_base = np.arange(len(buckets))

    for idx, (key, rows) in enumerate(sorted(by_config.items(), key=lambda item: labels.get(item[0], ""))):
        counts: dict[str, int] = {b: 0 for b in buckets}
        for r in rows:
            counts[bucket_label(r["round_trips"])] += 1
        total = max(sum(counts.values()), 1)
        pcts = [100.0 * counts[b] / total for b in buckets]
        mode = rows[0].get("mode", "")
        color = _color_for_mode(mode, idx) if mode else _color_for_series(idx)
        ax.bar(
            x_base + idx * bar_width, pcts, bar_width,
            label=labels.get(key, "configuration"), color=color,
        )

    ax.set_xticks(x_base + bar_width * (len(by_config) - 1) / 2)
    ax.set_xticklabels(buckets)
    ax.set_xlabel("Round Trips")
    ax.set_ylabel("% of Proposals")
    ax.set_title(f"RTT Distribution \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    ax.legend(framealpha=0.9)
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"{scenario_name}-rtt-histogram.png"), dpi=200)
    plt.close(fig)


def plot_rtt_distribution_plotly(
    all_rows: list[dict], output_dir: str, scenario_name: str,
) -> None:
    """RTT distribution grouped bar chart (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    by_config: dict[tuple, list[dict]] = defaultdict(list)
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        by_config[comparative_config_key(row)].append(row)
    buckets = ["0", "1", "2", "3", "4+"]

    def bucket_label(rt: int) -> str:
        return str(rt) if rt <= 3 else "4+"

    fig = go.Figure()
    for idx, (key, rows) in enumerate(sorted(by_config.items(), key=lambda item: labels.get(item[0], ""))):
        counts: dict[str, int] = {b: 0 for b in buckets}
        for r in rows:
            counts[bucket_label(r["round_trips"])] += 1
        total = max(sum(counts.values()), 1)
        pcts = [100.0 * counts[b] / total for b in buckets]
        mode = rows[0].get("mode", "")
        color = _color_for_mode(mode, idx) if mode else _color_for_series(idx)
        fig.add_trace(go.Bar(
            x=buckets, y=pcts, name=labels.get(key, "configuration"),
            marker_color=color,
        ))

    fig.update_layout(
        barmode="group",
        title=f"RTT Distribution \u2014 {scenario_name}",
        xaxis_title="Round Trips", yaxis_title="% of Proposals",
        height=500, template="plotly_white",
    )
    fig.update_yaxes(rangemode="tozero")
    fig.write_html(os.path.join(output_dir, f"{scenario_name}-rtt-histogram.html"))


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
        ax.set_ylim(bottom=0)

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
    fig.update_yaxes(rangemode="tozero")
    fig.write_html(os.path.join(output_dir, "summary-dashboard.html"))


# ---------------------------------------------------------------------------
# 6f  Topology x Mode heatmap
# ---------------------------------------------------------------------------

def plot_topology_mode_heatmap(all_rows: list[dict], output_dir: str, scenario_name: str) -> None:
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
    ax.set_title(f"Topology \u00d7 Mode Heatmap \u2014 {scenario_name}", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, f"{scenario_name}-topology-mode-heatmap.png"), dpi=200)
    plt.close(fig)


def plot_topology_mode_heatmap_plotly(all_rows: list[dict], output_dir: str, scenario_name: str) -> None:
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
        title=f"Topology \u00d7 Mode Heatmap \u2014 {scenario_name}",
        xaxis_title="Topology", yaxis_title="Mode",
        height=max(400, 80 * len(modes)), template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, f"{scenario_name}-topology-mode-heatmap.html"))


# ---------------------------------------------------------------------------
# 6f′ Combined Scenario x Mode heatmap (all scenarios in one chart)
# ---------------------------------------------------------------------------

def plot_scenario_mode_heatmap(
    all_scenario_rows: dict[str, list[dict]], output_dir: str,
) -> None:
    """Heatmap of median latency: X=mode, Y=scenario (matplotlib)."""
    configure_matplotlib_style()
    import matplotlib.pyplot as plt

    modes_ordered = ["classic", "fast", "leader"]
    mode_labels = [_label_for_mode(m) for m in modes_ordered]

    # Build per-(scenario, mode) median latencies
    scenario_ids: list[str] = []
    scenario_labels: list[str] = []
    for sid in _SCENARIO_ORDER:
        matching = [k for k in all_scenario_rows if k.split("-", 1)[0].upper() == sid]
        if matching:
            scenario_ids.append(sid)
            scenario_labels.append(_SCENARIO_DISPLAY_NAMES.get(sid, sid))

    if not scenario_ids:
        return

    matrix = np.full((len(scenario_ids), len(modes_ordered)), np.nan)
    for si, sid in enumerate(scenario_ids):
        rows = []
        for k, v in all_scenario_rows.items():
            if k.split("-", 1)[0].upper() == sid:
                rows.extend(v)
        for mi, mode in enumerate(modes_ordered):
            lats = [r["latency_ms"] for r in rows if r["mode"] == mode]
            if lats:
                matrix[si, mi] = float(np.median(lats))

    fig, ax = plt.subplots(figsize=(max(6, 2 * len(modes_ordered)), max(5, 0.8 * len(scenario_ids))))
    im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd")

    ax.set_xticks(np.arange(len(modes_ordered)))
    ax.set_yticks(np.arange(len(scenario_ids)))
    ax.set_xticklabels(mode_labels, fontsize=11)
    ax.set_yticklabels(scenario_labels, fontsize=11)

    for si in range(len(scenario_ids)):
        for mi in range(len(modes_ordered)):
            val = matrix[si, mi]
            if not np.isnan(val):
                ax.text(mi, si, f"{val:.1f}", ha="center", va="center",
                        color="white" if val > np.nanmedian(matrix) else "black",
                        fontsize=10, fontweight="bold")

    fig.colorbar(im, ax=ax, label="Median Latency (ms)")
    ax.set_title("Scenario × Mode Heatmap", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(output_dir, "scenario-mode-heatmap.png"), dpi=200)
    plt.close(fig)


def plot_scenario_mode_heatmap_plotly(
    all_scenario_rows: dict[str, list[dict]], output_dir: str,
) -> None:
    """Scenario x Mode heatmap (plotly)."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        return

    modes_ordered = ["classic", "fast", "leader"]
    mode_labels = [_label_for_mode(m) for m in modes_ordered]

    scenario_ids: list[str] = []
    scenario_labels: list[str] = []
    for sid in _SCENARIO_ORDER:
        matching = [k for k in all_scenario_rows if k.split("-", 1)[0].upper() == sid]
        if matching:
            scenario_ids.append(sid)
            scenario_labels.append(_SCENARIO_DISPLAY_NAMES.get(sid, sid))

    if not scenario_ids:
        return

    z: list[list[float]] = []
    for si, sid in enumerate(scenario_ids):
        rows = []
        for k, v in all_scenario_rows.items():
            if k.split("-", 1)[0].upper() == sid:
                rows.extend(v)
        row_vals: list[float] = []
        for mode in modes_ordered:
            lats = [r["latency_ms"] for r in rows if r["mode"] == mode]
            row_vals.append(float(np.median(lats)) if lats else float("nan"))
        z.append(row_vals)

    text = [[f"{v:.1f}" if not np.isnan(v) else "" for v in row] for row in z]

    fig = go.Figure(data=go.Heatmap(
        z=z, x=mode_labels, y=scenario_labels,
        colorscale="YlOrRd", text=text,
        texttemplate="%{text}", textfont=dict(size=12),
    ))
    fig.update_layout(
        title="Scenario × Mode Heatmap",
        xaxis_title="Mode", yaxis_title="Scenario",
        height=max(400, 60 * len(scenario_ids)), template="plotly_white",
    )
    fig.write_html(os.path.join(output_dir, "scenario-mode-heatmap.html"))


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

    by_config: dict[tuple, list[dict]] = defaultdict(list)
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        by_config[comparative_config_key(row)].append(row)

    config_items = sorted(by_config.items(), key=lambda item: labels.get(item[0], ""))
    n_configs = max(len(config_items), 1)
    fig, axes = plt.subplots(1, n_configs, figsize=(7 * n_configs, 5), squeeze=False)

    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    for mi, (key, mode_rows) in enumerate(config_items):
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
        ax.set_ylim(bottom=0)
        ax.set_title(labels.get(key, "configuration"), fontsize=12)
        ax.legend(fontsize=7, framealpha=0.9, ncol=2)

    fig.suptitle(
        f"Per-Proposer Latency Traces \u2014 {scenario_name}",
        fontsize=14, fontweight="bold",
    )
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(
        os.path.join(output_dir, f"{scenario_name}-per-proposer-traces.png"), dpi=200,
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

    by_config: dict[tuple, list[dict]] = defaultdict(list)
    labels = build_comparative_label_map(all_rows)
    for row in all_rows:
        by_config[comparative_config_key(row)].append(row)

    config_items = sorted(by_config.items(), key=lambda item: labels.get(item[0], ""))
    n_modes = max(len(config_items), 1)

    fig = make_subplots(
        rows=1,
        cols=n_modes,
        subplot_titles=[labels.get(key, "configuration") for key, _rows in config_items],
    )
    colors = list(MODE_COLORS.values()) + _EXTRA_COLORS

    for mi, (_key, mode_rows) in enumerate(config_items):
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
    fig.update_yaxes(rangemode="tozero")
    fig.write_html(os.path.join(output_dir, f"{scenario_name}-per-proposer-traces.html"))


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
        ax.set_ylim(bottom=0)
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
        ax.set_ylim(bottom=0)
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
            ax.set_ylim(bottom=0)
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
        fig.update_yaxes(title_text="Latency (ms)", rangemode="tozero", row=1, col=1)
        fig.update_xaxes(title_text="Latency (ms)", row=1, col=2)
        fig.update_yaxes(title_text="CDF", row=1, col=2)
        fig.update_xaxes(title_text="Simulated Time (ms)", row=2, col=1)
        fig.update_yaxes(title_text="Proposals/sec", rangemode="tozero", row=2, col=1)
        fig.update_yaxes(title_text="Latency (ms)", rangemode="tozero", row=2, col=2)

        fig.write_html(os.path.join(charts_dir, f"benchmark-{facet_name}.html"))

    print(f"plotly charts written to {charts_dir}")


# ---------------------------------------------------------------------------
# RESULTS.md generation (comparative mode)
# ---------------------------------------------------------------------------

_TOPOLOGY_SECTION = r"""## Topologies

The current comparative suite defined in `run_comparative_benchmarks.ps1` uses
only two latency profiles: `lan` and `geo5region`. If local chart artifacts do
not match these descriptions, regenerate the comparative output from the current
benchmark suite.

### LAN

All nodes are co-located in one datacenter. Each proposer ordinal `N` is
collocated with client `N` and acceptor `N` on the same machine (~0 ms latency).
Non-collocated same-region links use `base-delay-ms=1` and `max-jitter-ms=1`.

```mermaid
graph LR
  subgraph Datacenter
    subgraph Machine_1
      C1([C1])
      P1([P1])
      A1([A1])
    end
    subgraph Machine_2
      C2([C2])
      P2([P2])
      A2([A2])
    end
    subgraph Machine_3
      C3([C3])
      P3([P3])
      A3([A3])
    end
    A4([A4])
    A5([A5])
    C1 ---|~0 ms| P1
    C2 ---|~0 ms| P2
    C3 ---|~0 ms| P3
    P1 ---|~0 ms| A1
    P1 ---|1-2 ms| A2
    P1 ---|1-2 ms| A3
    P1 ---|1-2 ms| A4
    P1 ---|1-2 ms| A5
    P2 ---|1-2 ms| A1
    P2 ---|~0 ms| A2
    P2 ---|1-2 ms| A3
    P2 ---|1-2 ms| A4
    P2 ---|1-2 ms| A5
    P3 ---|1-2 ms| A1
    P3 ---|1-2 ms| A2
    P3 ---|~0 ms| A3
    P3 ---|1-2 ms| A4
    P3 ---|1-2 ms| A5
  end
```

| Link | Configured latency |
|------|--------------------|
| Collocated client `N` <-> proposer `N` | ~0 ms (1 µs) |
| Collocated proposer `N` <-> acceptor `N` | ~0 ms (1 µs) |
| Other same-region proposer <-> acceptor links | 1 ms base + up to 1 ms jitter |

**Quorum impact (5 acceptors):**
Classic and leader need 3 / 5 responses; fast needs 4 / 5. Because every node is
local, the difference is mostly 1 RTT versus 2 RTT, not geography.

---

### Geo 5-Region

The geo suite uses `geo5region`: East US 2, France Central, Southeast Asia,
Brazil South, and Australia Southeast. Cross-region latencies are configured
from Azure P50 RTT medians, which span 85-332 ms for this region set.
Each proposer ordinal `N` is collocated with client `N` and acceptor `N`
on the same machine (~0 ms latency).

```mermaid
graph TB
  subgraph East_US_2
    C1([C1])
    P1([P1])
    A1([A1])
  end
  subgraph France_Central
    C2([C2])
    P2([P2])
    A2([A2])
  end
  subgraph Southeast_Asia
    C3([C3])
    P3([P3])
    A3([A3])
  end
  subgraph Brazil_South
    A4([A4])
  end
  subgraph Australia_Southeast
    A5([A5])
    CX(["CX (S1 client)"])
  end
  C1 ---|~0 ms| P1
  C2 ---|~0 ms| P2
  C3 ---|~0 ms| P3
  P1 ---|~0 ms| A1
  P1 ---|~43 ms| A2
  P1 ---|~115 ms| A3
  P1 ---|~59 ms| A4
  P1 ---|~103 ms| A5
  P2 ---|~43 ms| A1
  P2 ---|~0 ms| A2
  P2 ---|~75 ms| A3
  P2 ---|~96 ms| A4
  P2 ---|~119 ms| A5
  P3 ---|~115 ms| A1
  P3 ---|~75 ms| A2
  P3 ---|~0 ms| A3
  P3 ---|~167 ms| A4
  P3 ---|~45 ms| A5
  CX -. "~45 ms" .-> P3
  CX -. "~103 ms" .-> P1
```

| Region pair | Median RTT |
|-------------|------------|
| East US 2 <-> France Central | 85 ms |
| East US 2 <-> Southeast Asia | 228 ms |
| East US 2 <-> Brazil South | 116 ms |
| East US 2 <-> Australia Southeast | 204 ms |
| France Central <-> Southeast Asia | 148 ms |
| France Central <-> Brazil South | 190 ms |
| France Central <-> Australia Southeast | 236 ms |
| Southeast Asia <-> Brazil South | 332 ms |
| Southeast Asia <-> Australia Southeast | 88 ms |
| Brazil South <-> Australia Southeast | 311 ms |
| Collocated client `N` <-> proposer `N` | ~0 ms (1 µs) |
| Collocated proposer `N` <-> acceptor `N` | ~0 ms (1 µs) |
| Other intra-region links | 1 ms base + up to 1 ms jitter |

The comparative suite never uses more proposers than acceptors, so proposer
ordinal `N` is always aligned with acceptor ordinal `N`. In the 3-proposer geo
layout used by S1, proposers occupy East US 2, France Central, and Southeast
Asia, while acceptors occupy all five regions. Scenario S1 places the explicit
benchmark client at ordinal 5 in Australia Southeast, a region with no proposer.
Its nearest proposer is P3 in Southeast Asia (88 ms RTT). The distinguished
leader is set to P2 in France Central via `leader-ordinal=2`, so leader mode
pays a 236 ms client hop instead of the 88 ms fast route.
"""

_SCENARIO_DESCRIPTIONS: dict[str, str] = {
    "S1": r"""## S1 - Geo-Distributed, No Contention

**What this tests:** One explicit benchmark client drives a single request stream
from topology ordinal 5 (Australia Southeast), a region with no proposer, while
the three proposers are spread across the first three geo regions. There is no
proposer contention, so the dominant cost is client routing plus the commit RTT.

**Setup:** 3 proposers, 5 acceptors, 1000 proposals on `geo5region` with
`client-ordinal=5`, `leader-ordinal=2`, and no faults.

**Client routing:** The explicit client is in Australia Southeast, which has no
proposer. Fast and classic route to the nearest healthy proposer, which is
proposer 3 in Southeast Asia (88 ms RTT). Leader mode designates proposer 2 in
France Central as the primary (`leader-ordinal=2`), so it pays a 236 ms
client-to-primary hop versus the 88 ms a non-leader client would pay.

| Parameter | Value |
|-----------|-------|
| Proposers | 3 |
| Acceptors | 5 |
| Proposals | 1 000 |
| Topology | Geo 5-Region |
| Explicit client | Ordinal 5 (Australia Southeast) |
| Leader ordinal | 2 (France Central) |
| Modes | Classic, Fast, Leader |
| Faults | None |

**What to look for:** Fast commits in a single proposer-to-acceptor RTT from the
nearest proposer, while leader absorbs both the non-local client hop and its own
quorum distances. Proposer learning keeps later requests from starting cold even
though the workload itself is contention-free.
""",

    "S2": r"""## S2 - Agreement Consensus

**What this tests:** A LAN agreement workload with five proposers so each
proposer can share a site with a different acceptor. Geo-aware clients and
proposer learning converge on the same next value so fast rounds avoid conflict.

**Setup:** 5 proposers, 5 acceptors, 1000 proposals on `lan` with
`agreement-mode=true`.

**Client routing:**
- Fast fans each logical request to all healthy proposers, classic targets the
  nearest healthy proposer, and leader targets the current primary and updates
  after failover.
- After commits, proposers broadcast learned state so later requests can reuse
  the accepted ballot, accepted value, and prepared ballot.

| Parameter | Value |
|-----------|-------|
| Proposers | 5 |
| Acceptors | 5 |
| Proposals | 1 000 |
| Topology | LAN |
| Agreement mode | true |
| Modes | Classic, Fast, Leader |
| Faults | None |

**What to look for:** With one proposer per acceptor site, agreement routing
and learned-state propagation should keep fast-round conflicts low while the
mode comparison isolates the remaining protocol overheads.
""",

    "S3": r"""## S3 - Register Initialization Burst

**What this tests:** Many independent first writes to previously uninitialized
registers. The simulation reinitializes all proposer and acceptor state before
each proposal, so every write behaves as if it targets a brand-new register.

**Setup:** 1 proposer, 5 acceptors, 200 proposals on `lan` with
`register-init-mode=true`.

**Client routing:** The default benchmark workload drives one synthetic request
stream per proposer, so the comparison isolates first-write protocol mechanics
instead of geo routing.

| Parameter | Value |
|-----------|-------|
| Proposers | 1 |
| Acceptors | 5 |
| Proposals | 200 |
| Topology | LAN |
| Register-init mode | true |
| Modes | Classic, Fast, Leader |
| Faults | None |

**What to look for:** Fast commit's implicit ballot gives first writes a 1-RTT
path instead of the 2-RTT prepare-plus-accept required by Classic and Leader
modes. On LAN the absolute gap is small, but the RTT histogram should clearly
show the missing prepare phase for Fast commits.
""",

    "S4": r"""## S4 - Infrequent Writes

**What this tests:** Serial random-proposer proposals across 3 proposers with 1 s
gaps between writes. After each commit, the committed value and prepared ballot
are broadcast to all peer proposers. The 1 s interval ensures these learn
messages arrive before the next proposal starts.

**Setup:** 3 proposers, 5 acceptors, 100 proposals on `lan` with
`serial-random-proposer=true` and `write-interval-ms=1000`. All three modes
(classic, fast, leader) are included. Each proposer has a collocated client
(zero-latency link between client _i_ and proposer _i_).

**Client routing:** Proposals select a collocated client uniformly at random.
For fast and classic, each client sends to its collocated proposer. For leader,
every client sends to the distinguished leader (P1), so roughly 1/3 of requests are
collocated and 2/3 traverse a standard LAN hop.

| Parameter | Value |
|-----------|-------|
| Proposers | 3 |
| Acceptors | 5 |
| Proposals | 100 |
| Topology | LAN |
| Write interval | 1000 ms |
| Serial random-proposer | Yes (collocated clients) |
| Modes | Classic, Fast, Leader |
| Faults | None |

**What to look for:**

- **Fast ≈ 1 acceptor RTT.** Each proposer reuses the learned prepared fast
  ballot from the previous commit. The fast ballot is shared (proposer-id = 0),
  so any proposer can skip prepare. Because every client sends to its collocated
  proposer (≈ 0 ms hop), end-to-end latency is uniformly ~1 acceptor RTT with a
  tight spread.
- **Classic ≈ 2 acceptor RTTs.** Classic mode does not piggyback a prepared
  ballot, so every proposal must prepare before accepting. Like fast, each client
  sends to its own collocated proposer, so the spread is tight.
- **Leader: wide bimodal spread, average slightly under 2 acceptor RTTs.** All
  proposals route to P1 (the distinguished leader), who reuses its piggybacked
  classic ballot and commits in 1 protocol RTT—the same as fast. However, the
  end-to-end latency measured from client to client includes the client ↔ leader
  network hop. Because each proposer's collocated client has a near-zero link
  (1 μs) to its _own_ proposer but uses the standard LAN delay (~1 ms base +
  0–1 ms jitter) for any other node, the commit latency splits into two bands:
  - **~1/3 of proposals** (Client 1, collocated with leader): ≈ 0 ms forwarding
    → total ≈ 1 acceptor RTT.
  - **~2/3 of proposals** (Client 2 or 3, remote from leader): ≈ 1 LAN
    round-trip of forwarding overhead → total ≈ 2 acceptor RTTs.

  This bimodal split produces the wide spread visible in the commit-latency
  chart. The weighted average is roughly 1/3 × 1 + 2/3 × 2 ≈ 5/3 acceptor RTTs,
  which is why leader sits between fast (1 RTT) and classic (2 RTTs).
""",

    "S5": r"""## S5 - High-Throughput Sustained Stream

**What this tests:** A single proposer emits a long write stream with no idle
time. This is the best case for distinguished leader mode because prepared-ballot
reuse is continuous and there is no multi-proposer contention.

**Setup:** 1 proposer, 5 acceptors, 2000 proposals on `lan`.

**Client routing:** The default workload becomes one synthetic request stream on
a single proposer, so the chart focuses on steady-state protocol overhead rather
than routing decisions.

| Parameter | Value |
|-----------|-------|
| Proposers | 1 |
| Acceptors | 5 |
| Proposals | 2 000 |
| Topology | LAN |
| Modes | Classic, Fast, Leader |
| Faults | None |

**What to look for:** Fast still saves an RTT per commit, but leader should
recover some of that gap by amortizing its prepare over a sustained stream.
""",

    "S6": r"""## S6 - Contention Spectrum

**What this tests:** A proposer-count sweep (1, 2, 3, 5) showing where fast's
leaderless 1-RTT path stops winning and leader's centralized coordination starts
paying off. The sweep stops at 5 so every proposer stays aligned with a
different acceptor.

**Setup:** 1, 2, 3, and 5 proposers, 5 acceptors, 500 proposals on `lan`.
All three protocol modes are included.

**Client routing:** The default workload keeps one synthetic request stream per
proposer, so every point on the sweep raises or lowers contention by changing the
number of active writers targeting the same register.

| Parameter | Value |
|-----------|-------|
| Proposers | 1, 2, 3, 5 |
| Acceptors | 5 |
| Proposals | 500 |
| Topology | LAN |
| Modes | Classic, Fast, Leader |
| Faults | None |

**What to look for:** Low contention favors fast's 1-RTT path. As contention
rises, retries and conflict recovery should create a crossover where leader wins.
""",

    "S7": r"""## S7 - Leader Gray Failure

**What this tests:** A soft failure on proposer 1. During the middle of the run,
all traffic to and from proposer 1 gets an extra 200 ms delay plus 50 ms jitter.

**Setup:** 3 proposers, 5 acceptors, 600 proposals on `lan` with
`fault-schedule=slow-link:proposer:1:at:200:until:400:delay:200:jitter:50`.
Only fast and leader are included.

**Client routing:** The default synthetic workload keeps one request stream per
proposer. In leader mode, proposer 1 is also the distinguished primary, so all
cluster traffic depends on the degraded node.

| Parameter | Value |
|-----------|-------|
| Proposers | 3 |
| Acceptors | 5 |
| Proposals | 600 |
| Topology | LAN |
| Fault | Slow-link on proposer 1 during proposals 200-400 |
| Modes | Fast, Leader only |

**What to look for:** Leader mode should degrade cluster-wide because every
proposal still routes through proposer 1. In fast mode, only proposer 1's own
traffic slows down while the other proposers continue normally.
""",

    "S8": r"""## S8 - Leader Crash Failover

**What this tests:** A hard leader outage followed by client failover. Proposer 5
(the designated leader) crashes at proposal 30 and stays down until proposal 60.
A single explicit client at proposer 1 routes locally for classic/fast and through
the leader for leader mode.

**Setup:** 5 proposers, 5 acceptors, 90 proposals on `lan` with
`fault-schedule=crash:proposer:5:at:30:until:60`, `client-ordinal=1`,
`leader-ordinal=5`, and `client-timeout-ms=20`. All three modes are included.

**Client routing:**
- Classic/Fast: client routes to nearest proposer (proposer 1, collocated),
  completely unaffected by the leader crash.
- Leader: client routes to proposer 5, crash forces a 20 ms timeout followed
  by failover to a different healthy proposer.

| Parameter | Value |
|-----------|-------|
| Proposers | 5 |
| Acceptors | 5 |
| Proposals | 90 |
| Topology | LAN |
| Client | Ordinal 1 (collocated with proposer 1) |
| Leader | Proposer 5 |
| Client timeout | 20 ms |
| Fault | Crash proposer 5 during proposals 30-60 |
| Modes | Classic, Fast, Leader |

**What to look for:** Classic and fast should show flat, unaffected latency
throughout the run. Leader mode should show a short availability stutter
during the crash window, with recovery after failover.
""",
}

_PROTOCOL_MODES_SECTION = r"""## Protocol Modes

| Mode | RTT per commit | Quorum | Prepare phase | Key trade-off |
|------|---------------|--------|---------------|---------------|
| **Fast Paxos (Leaderless)** | 1 | 4 / 5 | None (implicit ballot) | Lowest latency, but larger quorum and vulnerable to conflicts |
| **Distinguished Leader** | 1 (amortised) | 3 / 5 | Once per ballot (amortised) | Smaller quorum + amortised prepare, but leader is a single point of failure |
| **Classic Paxos** | 2 | 3 / 5 | Every commit | Simplest, but always pays 2 RTT |

- **Fast Paxos** skips the prepare phase entirely by using an implicit ballot
  number that all acceptors recognise. Commits in a single RTT if ≥ 4/5
  acceptors agree. Falls back to 2 RTT on conflict.
- **Distinguished Leader** designates proposer 1 as leader. The leader issues
  one prepare, then piggybacks subsequent accepts on the prepared ballot.
  Achieves 1 RTT per commit (after the initial prepare) with only a 3/5 quorum,
  but all traffic funnels through the leader.
- **Classic Paxos** issues prepare + accept on every commit. Always 2 RTT,
  3/5 quorum. The baseline.
"""

_SCENARIO_SUMMARY_SECTION = r"""## Current Comparative Suite

| Scenario | Topology | Main sweep | Focus |
|----------|----------|------------|-------|
| S1 | Geo 5-Region | Explicit client at ordinal 5 (Australia SE) | Non-primary client routing with no contention |
| S2 | LAN | Proposers `5`, `agreement-mode=true` | Agreement routing plus proposer learning |
| S3 | LAN | Proposers `1/3` | First-write latency with `register-init-mode=true` |
| S4 | LAN | `serial-random-proposer=true`, 1 s interval | Learn-propagated ballot reuse across proposers |
| S5 | LAN | Single proposer stream | Sustained throughput with no contention |
| S6 | LAN | Proposers `1/2/3/5` | Contention crossover between fast and leader |
| S7 | LAN | Slow-link fault on proposer 1 | Gray-failure isolation vs leader bottleneck |
| S8 | LAN | Crash fault on proposer 5 | Timeout-driven leader failover |
"""

_RESULTS_INTRO_SECTION = r"""_This document describes the current comparative benchmark suite defined in `run_comparative_benchmarks.ps1`. If charts are missing for a scenario, rerun `.\run_comparative_benchmarks.ps1` and then `uv run scripts\plot_benchmarks.py --input artifacts\comparative --comparative` to refresh the artifacts._
"""

_SCENARIO_ORDER = ("S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8")

_SCENARIO_DISPLAY_NAMES: dict[str, str] = {
    "S1": "Geo-Distributed",
    "S2": "Same Value",
    "S3": "Initialization",
    "S4": "Infrequent Writes",
    "S5": "High-Throughput",
    "S6": "High Contention",
    "S7": "Gray Failure",
    "S8": "Failover",
}

def build_scenario_mode_table(
    all_scenario_rows: dict[str, list[dict]],
) -> str:
    """Build a markdown table of median latency per scenario × mode with best/worst icons."""
    modes_ordered = ["classic", "fast", "leader"]
    mode_headers = [_label_for_mode(m) for m in modes_ordered]

    header = "| Scenario | " + " | ".join(mode_headers) + " |"
    separator = "|" + "|".join(["---"] * (1 + len(modes_ordered))) + "|"

    rows: list[str] = []
    for sid in _SCENARIO_ORDER:
        matching_rows: list[dict] = []
        for k, v in all_scenario_rows.items():
            if k.split("-", 1)[0].upper() == sid:
                matching_rows.extend(v)

        display_name = _SCENARIO_DISPLAY_NAMES.get(sid, sid)

        # Compute per-mode medians
        medians: dict[str, float | None] = {}
        for mode in modes_ordered:
            lats = [r["latency_ms"] for r in matching_rows if r["mode"] == mode]
            medians[mode] = float(np.median(lats)) if lats else None

        present = {m: v for m, v in medians.items() if v is not None}
        best_mode = min(present, key=lambda m: present[m]) if present else None
        worst_mode = max(present, key=lambda m: present[m]) if present else None
        # Only mark best/worst when there are at least 2 distinct modes to compare
        if best_mode == worst_mode:
            best_mode = worst_mode = None

        cells: list[str] = []
        for mode in modes_ordered:
            val = medians[mode]
            if val is None:
                cells.append("—")
            else:
                formatted = f"{val:.1f} ms"
                if mode == best_mode:
                    formatted = f"🟢 {formatted}"
                elif mode == worst_mode:
                    formatted = f"🔴 {formatted}"
                cells.append(formatted)

        rows.append(f"| {sid} — {display_name} | " + " | ".join(cells) + " |")

    return "\n".join([header, separator] + rows) + "\n"


def generate_results_markdown(
    root_dir: str,
    charts_dir: str,
    generated_charts: list[str],
    all_scenario_rows: dict[str, list[dict]] | None = None,
) -> None:
    """Write RESULTS.md to root_dir with embedded chart images from charts_dir."""
    # Determine relative path from root_dir to charts_dir
    charts_rel = os.path.relpath(charts_dir, root_dir).replace("\\", "/")

    # Group charts by scenario ID; separate out cross-scenario charts
    from collections import OrderedDict
    scenario_charts: dict[str, list[str]] = OrderedDict()
    cross_scenario_charts: list[str] = []
    for fname in sorted(generated_charts):
        sid = fname.split("-", 1)[0]  # e.g. "S1"
        if sid.upper() in {s for s in _SCENARIO_ORDER}:
            scenario_charts.setdefault(sid, []).append(fname)
        else:
            cross_scenario_charts.append(fname)

    lines: list[str] = []
    lines.append("# Fast CASPaxos — Comparative Benchmark Results\n")
    lines.append(_RESULTS_INTRO_SECTION)
    lines.append(_PROTOCOL_MODES_SECTION)
    lines.append(_SCENARIO_SUMMARY_SECTION)
    lines.append(_TOPOLOGY_SECTION)

    # Cross-scenario section: markdown table + any remaining cross-scenario charts
    heatmap_files = {"scenario-mode-heatmap.png", "scenario-mode-heatmap.html"}
    non_heatmap_cross = [f for f in cross_scenario_charts if f not in heatmap_files]

    if all_scenario_rows or non_heatmap_cross:
        lines.append("## Scenario Overview\n")
        if all_scenario_rows:
            lines.append(build_scenario_mode_table(all_scenario_rows))
            lines.append("")
        for fname in non_heatmap_cross:
            alt = fname.rsplit(".", 1)[0].replace("-", " ").title()
            lines.append(f"![{alt}]({charts_rel}/{fname})\n")
        lines.append("")

    ordered_sids = list(dict.fromkeys([*_SCENARIO_ORDER, *scenario_charts.keys()]))

    # Per-scenario sections
    for sid in ordered_sids:
        charts = scenario_charts.get(sid, [])
        desc = _SCENARIO_DESCRIPTIONS.get(sid)
        if desc:
            lines.append(desc)
        else:
            lines.append(f"## {sid}\n")

        if not charts:
            lines.append("_Charts are not available for this scenario in the current artifact snapshot. Re-run the comparative suite to regenerate them._\n")
            lines.append("---\n")
            continue

        # Group charts by sub-group prefix (everything before the chart-type suffix)
        sub_groups: dict[str, list[str]] = OrderedDict()
        for fname in charts:
            # Strip extension
            stem = fname.rsplit(".", 1)[0]
            # Known chart-type suffixes
            for suffix in (
                "commit-latency", "rtt-histogram", "contention-spectrum",
                "failover-timeline", "per-proposer-traces", "topology-mode-heatmap",
            ):
                if stem.endswith(suffix):
                    prefix = stem[: -len(suffix)].rstrip("-")
                    break
            else:
                prefix = stem
            sub_groups.setdefault(prefix, []).append(fname)

        # Sort sub-groups: those with a sub-label first (alphabetically), scenario-level last
        sorted_prefixes = sorted(sub_groups.keys(), key=lambda p: ("" if p != sid else "~", p))

        for prefix in sorted_prefixes:
            group_charts = sub_groups[prefix]
            # Extract sub-group label (portion after scenario ID)
            sub_label = prefix[len(sid):].lstrip("-")
            if sub_label:
                lines.append(f"### {sub_label}\n")

            for fname in group_charts:
                alt = fname.rsplit(".", 1)[0].replace("-", " ").title()
                lines.append(f"![{alt}]({charts_rel}/{fname})\n")

        lines.append("---\n")

    md_path = os.path.join(root_dir, "RESULTS.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"RESULTS.md written to {md_path}")


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
        # Comparative mode: organise by scenario directory structure,
        # then split each scenario into per-configuration charts with
        # exactly 3 series (fast / leader / classic).
        # ---------------------------------------------------------------
        scenario_dirs = discover_comparative_structure(args.input)
        charts_dir = os.path.join(args.input, "charts")
        os.makedirs(charts_dir, exist_ok=True)

        all_scenario_rows: dict[str, list[dict]] = {}
        generated_charts: list[str] = []

        for scenario_name, scenario_csvs in sorted(scenario_dirs.items()):
            rows: list[dict] = []
            for csv_file in scenario_csvs:
                round_id = csv_file.parent.name
                rows.extend(load_csv(csv_file, round_id=round_id))

            if not rows:
                continue

            all_scenario_rows[scenario_name] = rows
            print(f"  Scenario {scenario_name}: {len(rows)} samples from {len(scenario_csvs)} CSV(s)")

            # Extract scenario ID (e.g., "S1" from "S1-geo-no-contention")
            scenario_id = scenario_name.split("-", 1)[0]

            # Sub-group by all config fields except mode
            sub_groups: dict[tuple, list[dict]] = defaultdict(list)
            for row in rows:
                sub_groups[_sub_group_key_no_mode(row)].append(row)

            for sg_key, sg_rows in sorted(sub_groups.items()):
                suffix = _build_sub_group_suffix(sg_rows, rows)
                chart_prefix = f"{scenario_id}-{suffix}" if suffix else scenario_id

                aggregated = aggregate_multi_run(sg_rows, smoothing_window=args.smoothing_window)

                if args.format in ("png", "both"):
                    plot_latency_timeseries_aggregated(aggregated, charts_dir, chart_prefix)
                    generated_charts.append(f"{chart_prefix}-commit-latency.png")
                    plot_rtt_distribution(sg_rows, charts_dir, chart_prefix)
                    generated_charts.append(f"{chart_prefix}-rtt-histogram.png")

                    if "gray-failure" in scenario_name.lower() or "crash-failover" in scenario_name.lower():
                        plot_per_proposer_traces(sg_rows, charts_dir, chart_prefix)
                        generated_charts.append(f"{chart_prefix}-per-proposer-traces.png")
                        plot_failover_timeline(sg_rows, charts_dir, chart_prefix)
                        generated_charts.append(f"{chart_prefix}-failover-timeline.png")

                if args.format in ("html", "both"):
                    plot_latency_timeseries_aggregated_plotly(aggregated, charts_dir, chart_prefix)
                    plot_rtt_distribution_plotly(sg_rows, charts_dir, chart_prefix)

                    if "gray-failure" in scenario_name.lower() or "crash-failover" in scenario_name.lower():
                        plot_per_proposer_traces_plotly(sg_rows, charts_dir, chart_prefix)
                        plot_failover_timeline_plotly(sg_rows, charts_dir, chart_prefix)

            # Scenario-level charts (aggregate across sub-groups)
            if "contention-spectrum" in scenario_name.lower():
                if args.format in ("png", "both"):
                    plot_contention_spectrum(rows, charts_dir, scenario_id)
                    generated_charts.append(f"{scenario_id}-contention-spectrum.png")
                if args.format in ("html", "both"):
                    plot_contention_spectrum_plotly(rows, charts_dir, scenario_id)

        generate_results_markdown(args.input, charts_dir, generated_charts, all_scenario_rows)
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
