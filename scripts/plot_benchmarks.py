#!/usr/bin/env python3
"""
Plot benchmark results from Fast CASPaxos simulation runs.

Reads benchmark-latency.csv files from the benchmark output directory and produces:
  - Latency vs proposal index (time-series)
  - Throughput (proposals/second, sliding window)
  - Latency CDF comparison

Usage:
    python scripts/plot_benchmarks.py --input artifacts/benchmark
    python scripts/plot_benchmarks.py --input artifacts/benchmark --format html
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path


def find_csv_files(input_dir: str) -> list[Path]:
    """Recursively find all benchmark-latency.csv files."""
    results = []
    for root, _, files in os.walk(input_dir):
        for f in files:
            if f == "benchmark-latency.csv":
                results.append(Path(root) / f)
    return sorted(results)


def load_csv(path: Path) -> list[dict]:
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
            })
    return rows


def group_by(rows: list[dict], key: str) -> dict[str, list[dict]]:
    """Group rows by a key field."""
    groups = defaultdict(list)
    for row in rows:
        groups[row[key]].append(row)
    return dict(groups)


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

    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)

    # Group by topology × fault for faceting
    facet_key = lambda r: f"{r['topology']}_{r['fault_label']}"
    facets = defaultdict(list)
    for row in all_rows:
        facets[facet_key(row)].append(row)

    for facet_name, facet_rows in sorted(facets.items()):
        by_mode = group_by(facet_rows, "mode")

        # 1. Latency time-series
        fig, ax = plt.subplots(figsize=(12, 6))
        for mode, rows in sorted(by_mode.items()):
            sorted_rows = sorted(rows, key=lambda r: r["completed_ms"])
            x = list(range(len(sorted_rows)))
            y = [r["latency_ms"] for r in sorted_rows]
            ax.plot(x, y, label=mode, alpha=0.8, linewidth=0.8)
        ax.set_xlabel("Proposal Index")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"Commit Latency Over Time — {facet_name}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(charts_dir, f"latency-timeseries-{facet_name}.png"), dpi=150)
        plt.close(fig)

        # 2. Throughput
        fig, ax = plt.subplots(figsize=(12, 6))
        for mode, rows in sorted(by_mode.items()):
            times, throughputs = sliding_window_throughput(rows, window_ms=200.0)
            if times:
                ax.plot(times, throughputs, label=mode, alpha=0.8, linewidth=0.8)
        ax.set_xlabel("Simulated Time (ms)")
        ax.set_ylabel("Throughput (proposals/sec)")
        ax.set_title(f"Throughput Over Time — {facet_name}")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(charts_dir, f"throughput-{facet_name}.png"), dpi=150)
        plt.close(fig)

        # 3. Latency CDF
        fig, ax = plt.subplots(figsize=(10, 6))
        for mode, rows in sorted(by_mode.items()):
            latencies = sorted([r["latency_ms"] for r in rows])
            n = len(latencies)
            cdf = [(i + 1) / n for i in range(n)]
            ax.plot(latencies, cdf, label=mode, linewidth=1.2)
        ax.set_xlabel("Latency (ms)")
        ax.set_ylabel("CDF")
        ax.set_title(f"Latency CDF — {facet_name}")
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

        colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]

        for i, (mode, rows) in enumerate(sorted(by_mode.items())):
            color = colors[i % len(colors)]
            sorted_rows = sorted(rows, key=lambda r: r["completed_ms"])

            # Latency time-series
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(sorted_rows))),
                    y=[r["latency_ms"] for r in sorted_rows],
                    mode="lines",
                    name=mode,
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
                    name=mode,
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
                        name=mode,
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
                    name=mode,
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


def main():
    parser = argparse.ArgumentParser(description="Plot Fast CASPaxos benchmark results")
    parser.add_argument("--input", required=True, help="Root directory containing benchmark output")
    parser.add_argument(
        "--format",
        choices=["png", "html", "both"],
        default="both",
        help="Output format: png (matplotlib), html (plotly), or both (default)",
    )
    args = parser.parse_args()

    csv_files = find_csv_files(args.input)
    if not csv_files:
        print(f"No benchmark-latency.csv files found in {args.input}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(csv_files)} benchmark CSV file(s)")

    all_rows = []
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
