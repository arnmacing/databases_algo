#!/usr/bin/env python3
"""
Sequential JMH runner with system metrics collection.

What it does:
1) Builds benchmarks jar (optional).
2) Lists all benchmarks from JMH.
3) Runs benchmarks one by one.
4) Samples CPU/RAM/threads/IO for Java process tree.
5) Saves CSV files and plots (score + resources).
6) Generates Markdown report with per-benchmark Notes blocks.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import psutil
import plotly.graph_objects as go
from plotly.subplots import make_subplots

BENCHMARK_RE = re.compile(r"^bench\.[A-Za-z0-9_.$]+\.[A-Za-z0-9_.$]+$")
X_PARAM_PRIORITY = ["n", "docs", "baseDocs", "bucketCapacity", "threshold", "wordsPerDoc", "seed"]


def parse_args() -> argparse.Namespace:
    default_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Run all JMH benchmarks sequentially with CPU/RAM sampling and generate report."
    )
    parser.add_argument("--project-root", type=Path, default=default_root, help="Repo root path.")
    parser.add_argument("--module", default="hw01", help="Maven module name (default: hw01).")
    parser.add_argument(
        "--jar",
        type=Path,
        default=Path("hw01/target/benchmarks.jar"),
        help="Path to JMH fat jar relative to project root.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("hw01/target/jmh-system-report"),
        help="Output directory relative to project root.",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.5,
        help="Sampling interval in seconds (default: 0.5).",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Do not run 'mvn -q -pl <module> -DskipTests package'.",
    )
    parser.add_argument(
        "--include",
        default=".*",
        help="Regex filter for benchmark full names (default: all).",
    )
    parser.add_argument(
        "--jmh-arg",
        action="append",
        default=[],
        help="Optional JMH override arg (repeatable). Use --jmh-arg=-wi style for dashed values.",
    )
    parser.add_argument(
        "--jmh-args",
        default="",
        help='Optional JMH override args as one string, e.g. "--jmh-args \'-wi 5 -i 10 -f 2\'".',
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="List discovered benchmarks and exit.",
    )
    parser.add_argument(
        "--rebuild-only",
        action="store_true",
        help="Rebuild plots/report from existing out-dir JSON/CSV without running JMH.",
    )
    return parser.parse_args()


def run(cmd: List[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)


def ensure_built(project_root: Path, module: str) -> None:
    cmd = ["mvn", "-q", "-pl", module, "-DskipTests", "package"]
    print("$", " ".join(shlex.quote(c) for c in cmd))
    subprocess.run(cmd, cwd=project_root, check=True)


def list_benchmarks(jar_path: Path, cwd: Path) -> List[str]:
    out = run(["java", "-jar", str(jar_path), "-l"], cwd=cwd).stdout.splitlines()
    benchmarks = [line.strip() for line in out if BENCHMARK_RE.match(line.strip())]
    return sorted(set(benchmarks))


def benchmark_slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def benchmark_class(name: str) -> str:
    parts = name.split(".")
    return parts[-2] if len(parts) >= 2 else name


def benchmark_method(name: str) -> str:
    return name.split(".")[-1]


def operation_kind(method: str) -> str:
    m = method.lower()
    if "build" in m:
        return "build"
    if any(x in m for x in ("put", "add", "insert", "update")):
        return "write"
    if any(x in m for x in ("remove", "delete")):
        return "delete"
    if any(x in m for x in ("get", "contains", "query", "near", "candidate")):
        return "read/query"
    return "other"


def sample_process_tree(proc: psutil.Process, elapsed_s: float) -> Dict[str, Any]:
    processes: List[psutil.Process] = []
    try:
        processes.append(proc)
        processes.extend(proc.children(recursive=True))
    except psutil.Error:
        pass

    total_cpu = 0.0
    total_rss = 0
    total_vms = 0
    total_threads = 0
    total_read = 0
    total_write = 0

    try:
        system_cpu_percent = float(psutil.cpu_percent(interval=None))
    except psutil.Error:
        system_cpu_percent = float("nan")
    try:
        system_memory_percent = float(psutil.virtual_memory().percent)
    except psutil.Error:
        system_memory_percent = float("nan")

    for p in processes:
        try:
            total_cpu += p.cpu_percent(interval=None)
        except psutil.Error:
            pass
        try:
            mem = p.memory_info()
            total_rss += int(mem.rss)
            total_vms += int(mem.vms)
        except psutil.Error:
            pass
        try:
            total_threads += int(p.num_threads())
        except psutil.Error:
            pass
        try:
            io = p.io_counters()
            total_read += int(io.read_bytes)
            total_write += int(io.write_bytes)
        except (psutil.Error, AttributeError):
            pass

    return {
        "elapsed_s": elapsed_s,
        "cpu_percent": total_cpu,
        "rss_mb": total_rss / (1024 * 1024),
        "vms_mb": total_vms / (1024 * 1024),
        "threads": total_threads,
        "read_mb": total_read / (1024 * 1024),
        "write_mb": total_write / (1024 * 1024),
        "system_cpu_percent": system_cpu_percent,
        "system_memory_percent": system_memory_percent,
    }


def run_benchmark_with_sampling(
    cmd: List[str],
    cwd: Path,
    sample_interval: float,
    log_path: Path,
) -> Tuple[int, float, pd.DataFrame]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, text=True)
        ps_proc = psutil.Process(proc.pid)
        psutil.cpu_percent(interval=None)
        try:
            ps_proc.cpu_percent(interval=None)
        except psutil.Error:
            pass

        start = time.time()
        rows: List[Dict[str, Any]] = []

        while True:
            rc = proc.poll()
            elapsed = time.time() - start
            rows.append(sample_process_tree(ps_proc, elapsed))
            if rc is not None:
                break
            time.sleep(sample_interval)

        duration = time.time() - start
        return proc.returncode, duration, pd.DataFrame(rows)


def flatten_jmh_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for r in records:
        pm = r.get("primaryMetric") or {}
        params = r.get("params") or {}

        row: Dict[str, Any] = {
            "benchmark": r.get("benchmark"),
            "mode": r.get("mode"),
            "score": pm.get("score"),
            "scoreError": pm.get("scoreError"),
            "scoreUnit": pm.get("scoreUnit"),
        }
        row.update(params)
        rows.append(row)
    return pd.DataFrame(rows)


def choose_x_param(df: pd.DataFrame) -> Optional[str]:
    param_cols = [c for c in df.columns if c not in {"benchmark", "mode", "score", "scoreError", "scoreUnit"}]
    varying = [c for c in param_cols if df[c].astype(str).nunique(dropna=True) > 1]
    if not varying:
        return None
    for p in X_PARAM_PRIORITY:
        if p in varying:
            return p
    return varying[0]


def write_figure(fig: go.Figure, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.write_image(str(out_path), format="png", scale=2)
        return out_path
    except Exception:
        html_path = out_path.with_suffix(".html")
        fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)
        return html_path


def build_score_plot(df: pd.DataFrame, benchmark: str, out_path: Path) -> Path:
    x_param = choose_x_param(df)
    plot_df = df.copy()
    plot_df["score"] = pd.to_numeric(plot_df["score"], errors="coerce")
    plot_df["scoreError"] = pd.to_numeric(plot_df["scoreError"], errors="coerce")
    plot_df = plot_df.dropna(subset=["score"])

    if x_param is None:
        plot_df["case"] = range(1, len(plot_df) + 1)
        x_param = "case"

    modes = sorted(str(m) for m in plot_df["mode"].dropna().unique().tolist())
    if not modes:
        modes = ["unknown"]
        plot_df["mode"] = "unknown"

    cols = 1 if len(modes) == 1 else 2
    rows = math.ceil(len(modes) / cols)
    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=[f"mode={m}" for m in modes],
        vertical_spacing=0.12 if rows > 1 else 0.06,
        horizontal_spacing=0.08,
    )

    for idx, mode in enumerate(modes):
        row = (idx // cols) + 1
        col = (idx % cols) + 1
        mode_df = plot_df[plot_df["mode"].astype(str) == mode].copy()

        group_cols: List[str] = []
        for c in mode_df.columns:
            if c in {"benchmark", "mode", "score", "scoreError", "scoreUnit", x_param}:
                continue
            if mode_df[c].astype(str).nunique(dropna=True) > 1:
                group_cols.append(c)

        if group_cols:
            grouped: Iterable[Tuple[Any, pd.DataFrame]] = mode_df.groupby(group_cols, dropna=False, sort=True)
        else:
            grouped = [("all", mode_df)]

        for key, part in grouped:
            if not isinstance(key, tuple):
                key = (key,)
            name = "all" if group_cols == [] else ", ".join(f"{g}={v}" for g, v in zip(group_cols, key))
            part = part.sort_values(by=x_param)
            fig.add_trace(
                go.Scatter(
                    x=part[x_param],
                    y=part["score"],
                    mode="lines+markers",
                    name=f"{mode}: {name}",
                    legendgroup=f"{mode}:{name}",
                    showlegend=True,
                    error_y=dict(type="data", array=part["scoreError"].fillna(0).tolist(), visible=True),
                    text=[
                        ", ".join(
                            f"{c}={part.iloc[i][c]}"
                            for c in part.columns
                            if c not in {"score", "scoreError"}
                        )
                        for i in range(len(part))
                    ],
                    hovertemplate="%{text}<br>score=%{y}<extra></extra>",
                ),
                row=row,
                col=col,
            )

        units = sorted(str(u) for u in mode_df["scoreUnit"].dropna().unique().tolist())
        y_title = "score" if not units else f"score ({', '.join(units)})"
        fig.update_xaxes(title_text=x_param, row=row, col=col)
        fig.update_yaxes(title_text=y_title, row=row, col=col)

    fig.update_layout(
        title=f"{benchmark}: score by mode (collage)",
        legend_title="series",
        template="plotly_white",
    )
    return write_figure(fig, out_path)


def build_resource_plot(metrics_df: pd.DataFrame, benchmark: str, out_path: Path) -> Path:
    plot_df = metrics_df.copy()
    for col in (
        "elapsed_s",
        "cpu_percent",
        "rss_mb",
        "threads",
        "read_mb",
        "write_mb",
        "system_cpu_percent",
    ):
        if col not in plot_df.columns:
            plot_df[col] = 0.0

    dt_s = plot_df["elapsed_s"].diff().replace(0, pd.NA)
    plot_df["read_mb_s"] = (plot_df["read_mb"].diff() / dt_s).fillna(0)
    plot_df["write_mb_s"] = (plot_df["write_mb"].diff() / dt_s).fillna(0)

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=("CPU % (process and system)", "RSS MB", "Threads", "Disk IO MB/s"),
    )

    x = plot_df["elapsed_s"]
    fig.add_trace(go.Scatter(x=x, y=plot_df["cpu_percent"], mode="lines", name="process CPU %"), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=plot_df["system_cpu_percent"], mode="lines", name="system CPU %"), row=1, col=1)
    fig.add_trace(go.Scatter(x=x, y=plot_df["rss_mb"], mode="lines", name="RSS MB"), row=2, col=1)
    fig.add_trace(go.Scatter(x=x, y=plot_df["threads"], mode="lines", name="Threads"), row=3, col=1)
    fig.add_trace(go.Scatter(x=x, y=plot_df["read_mb_s"], mode="lines", name="read MB/s"), row=4, col=1)
    fig.add_trace(go.Scatter(x=x, y=plot_df["write_mb_s"], mode="lines", name="write MB/s"), row=4, col=1)

    fig.update_xaxes(title_text="elapsed, s", row=4, col=1)
    fig.update_layout(title=f"{benchmark}: system resources", template="plotly_white", showlegend=True)
    return write_figure(fig, out_path)


def stats_or_nan(series: pd.Series, fn: str) -> float:
    if series.empty:
        return float("nan")
    if fn == "mean":
        return float(series.mean())
    if fn == "max":
        return float(series.max())
    if fn == "p95":
        return float(series.quantile(0.95))
    raise ValueError(fn)


def get_numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def fmt_value(v: Any) -> str:
    if pd.isna(v):
        return "-"
    if isinstance(v, (float, int)):
        return f"{float(v):.3f}"
    return str(v)


def build_class_overview_plot(class_df: pd.DataFrame, class_name: str, out_path: Path) -> Path:
    plot_df = class_df.copy()
    plot_df = plot_df.sort_values(["operation_kind", "method"]).reset_index(drop=True)
    plot_df["label"] = [f"{m}\n({k})" for m, k in zip(plot_df["method"], plot_df["operation_kind"])]

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        specs=[[{"secondary_y": True}], [{"secondary_y": True}]],
        subplot_titles=("Duration and CPU", "Memory and Threads"),
    )

    fig.add_trace(
        go.Bar(x=plot_df["label"], y=plot_df["duration_s"], name="duration, s"),
        row=1,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=plot_df["label"], y=plot_df["p95_cpu_percent"], mode="lines+markers", name="p95 CPU %"),
        row=1,
        col=1,
        secondary_y=True,
    )
    fig.add_trace(
        go.Bar(x=plot_df["label"], y=plot_df["peak_rss_mb"], name="peak RSS, MB"),
        row=2,
        col=1,
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=plot_df["label"], y=plot_df["avg_threads"], mode="lines+markers", name="avg threads"),
        row=2,
        col=1,
        secondary_y=True,
    )

    fig.update_yaxes(title_text="seconds", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="CPU %", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="MB", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="threads", row=2, col=1, secondary_y=True)
    fig.update_xaxes(title_text="operation", row=2, col=1)
    fig.update_layout(title=f"{class_name}: operations overview", template="plotly_white", barmode="group")
    return write_figure(fig, out_path)


def generate_report(
    out_dir: Path,
    operation_summary: pd.DataFrame,
    bench_sections: Dict[str, List[Dict[str, Any]]],
    class_overview_plots: Dict[str, Path],
    cmdline: str,
) -> Path:
    report_path = out_dir / "report.md"
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines: List[str] = []
    lines.append("# JMH System Report")
    lines.append("")
    lines.append(f"- Generated: `{now}`")
    lines.append(f"- Command: `{cmdline}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Benchmarks executed: **{len(operation_summary)}**")
    lines.append("- Summary CSV: `operation_summary.csv`")
    lines.append("- Flattened JMH CSV: `jmh_results_flat.csv`")
    lines.append("")

    for class_name in sorted(bench_sections):
        lines.append(f"## {class_name}")
        lines.append("")
        overview_plot = class_overview_plots.get(class_name)
        if overview_plot is not None:
            lines.append(f"![overview]({overview_plot.as_posix()})")
            lines.append("")

        class_rows = operation_summary[operation_summary["class"] == class_name].copy()
        if not class_rows.empty:
            class_rows = class_rows.sort_values(["operation_kind", "method"])
            lines.append("| operation | kind | duration_s | p95_cpu_% | peak_rss_mb | score_median |")
            lines.append("|---|---:|---:|---:|---:|---:|")
            for _, row in class_rows.iterrows():
                lines.append(
                    "| "
                    f"{row['method']} | {row['operation_kind']} | {fmt_value(row['duration_s'])} | "
                    f"{fmt_value(row['p95_cpu_percent'])} | {fmt_value(row['peak_rss_mb'])} | "
                    f"{fmt_value(row['score_median'])} |"
                )
            lines.append("")

        for section in bench_sections[class_name]:
            bench = section["benchmark"]
            rows = section["rows"]
            score_plot = section["score_plot"]
            resource_plot = section["resource_plot"]
            jmh_csv = section["jmh_csv"]
            metrics_csv = section["metrics_csv"]

            lines.append(f"### {benchmark_method(bench)}")
            lines.append("")
            lines.append(f"- Benchmark: `{bench}`")
            lines.append(f"- JMH data: `{jmh_csv.as_posix()}`")
            lines.append(f"- Resource data: `{metrics_csv.as_posix()}`")
            lines.append(f"- Operation type: `{operation_kind(benchmark_method(bench))}`")

            params: Dict[str, List[str]] = {}
            for row in rows:
                for k, v in row.items():
                    if k in {"benchmark", "mode", "score", "scoreError", "scoreUnit"}:
                        continue
                    params.setdefault(k, [])
                    sv = str(v)
                    if sv not in params[k]:
                        params[k].append(sv)

            if params:
                lines.append("- Parameters:")
                for k in sorted(params):
                    lines.append(f"  - `{k}`: {', '.join(params[k])}")
            lines.append("")

            lines.append(f"![score]({score_plot.as_posix()})")
            lines.append("")
            lines.append(f"![resources]({resource_plot.as_posix()})")
            lines.append("")
            lines.append("#### Notes")
            lines.append("```text")
            lines.append("- ")
            lines.append("```")
            lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def rebuild_from_existing(out_dir: Path, include_re: re.Pattern[str]) -> None:
    json_dir = out_dir / "json"
    csv_dir = out_dir / "csv"
    plots_dir = out_dir / "plots"
    for d in (json_dir, csv_dir, plots_dir):
        d.mkdir(parents=True, exist_ok=True)

    json_files = sorted(json_dir.glob("*.json"))
    if not json_files:
        raise SystemExit(f"No JSON files found in: {json_dir}")

    all_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {}
    class_overview_plots: Dict[str, Path] = {}

    for json_path in json_files:
        records = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(records, list) or not records:
            continue
        bench = str(records[0].get("benchmark") or "")
        if not bench or not include_re.search(bench):
            continue

        slug = benchmark_slug(bench)
        metrics_csv = csv_dir / f"{slug}__resources.csv"
        jmh_csv = csv_dir / f"{slug}__jmh.csv"
        score_plot_path = plots_dir / f"{slug}__score.png"
        resource_plot_path = plots_dir / f"{slug}__resources.png"

        flat_df = flatten_jmh_records(records)
        flat_df.to_csv(jmh_csv, index=False)
        score_plot_actual = build_score_plot(flat_df, bench, score_plot_path)

        if metrics_csv.exists():
            metrics_df = pd.read_csv(metrics_csv)
        else:
            metrics_df = pd.DataFrame(
                columns=[
                    "elapsed_s",
                    "cpu_percent",
                    "rss_mb",
                    "vms_mb",
                    "threads",
                    "read_mb",
                    "write_mb",
                    "system_cpu_percent",
                    "system_memory_percent",
                ]
            )
        resource_plot_actual = build_resource_plot(metrics_df, bench, resource_plot_path)

        all_rows.extend(records)
        section = {
            "benchmark": bench,
            "rows": flat_df.to_dict(orient="records"),
            "score_plot": score_plot_actual.relative_to(out_dir),
            "resource_plot": resource_plot_actual.relative_to(out_dir),
            "jmh_csv": jmh_csv.relative_to(out_dir),
            "metrics_csv": metrics_csv.relative_to(out_dir),
        }
        sections.setdefault(benchmark_class(bench), []).append(section)

        elapsed_s = get_numeric_series(metrics_df, "elapsed_s")
        cpu = get_numeric_series(metrics_df, "cpu_percent")
        sys_cpu = get_numeric_series(metrics_df, "system_cpu_percent")
        rss = get_numeric_series(metrics_df, "rss_mb")
        sys_mem = get_numeric_series(metrics_df, "system_memory_percent")
        threads = get_numeric_series(metrics_df, "threads")
        read_mb = get_numeric_series(metrics_df, "read_mb")
        write_mb = get_numeric_series(metrics_df, "write_mb")
        summary_rows.append(
            {
                "benchmark": bench,
                "class": benchmark_class(bench),
                "method": benchmark_method(bench),
                "operation_kind": operation_kind(benchmark_method(bench)),
                "duration_s": float(elapsed_s.max()) if not elapsed_s.empty and pd.notna(elapsed_s.max()) else float("nan"),
                "avg_cpu_percent": stats_or_nan(cpu, "mean"),
                "p95_cpu_percent": stats_or_nan(cpu, "p95"),
                "avg_system_cpu_percent": stats_or_nan(sys_cpu, "mean"),
                "p95_system_cpu_percent": stats_or_nan(sys_cpu, "p95"),
                "peak_rss_mb": stats_or_nan(rss, "max"),
                "peak_system_memory_percent": stats_or_nan(sys_mem, "max"),
                "avg_threads": stats_or_nan(threads, "mean"),
                "max_read_mb": stats_or_nan(read_mb, "max"),
                "max_write_mb": stats_or_nan(write_mb, "max"),
                "jmh_cases": len(flat_df),
                "score_min": pd.to_numeric(flat_df["score"], errors="coerce").min(),
                "score_median": pd.to_numeric(flat_df["score"], errors="coerce").median(),
                "score_max": pd.to_numeric(flat_df["score"], errors="coerce").max(),
                "score_units": ", ".join(sorted(set(str(u) for u in flat_df["scoreUnit"].dropna().tolist()))),
            }
        )

    if not summary_rows:
        raise SystemExit("No matching benchmarks found in existing JSON files.")

    flat_all = flatten_jmh_records(all_rows)
    flat_all.to_csv(out_dir / "jmh_results_flat.csv", index=False)
    summary_df = pd.DataFrame(summary_rows).sort_values(["class", "method"]).reset_index(drop=True)
    summary_df.to_csv(out_dir / "operation_summary.csv", index=False)

    for class_name, class_df in summary_df.groupby("class", sort=True):
        overview_path = plots_dir / f"{benchmark_slug(class_name)}__overview.png"
        overview_actual = build_class_overview_plot(class_df, class_name, overview_path)
        class_overview_plots[class_name] = overview_actual.relative_to(out_dir)

    cmdline = " ".join(shlex.quote(x) for x in sys.argv)
    report_path = generate_report(out_dir, summary_df, sections, class_overview_plots, cmdline)
    print("Rebuilt from existing data.")
    print("OK:", (out_dir / "operation_summary.csv"))
    print("OK:", (out_dir / "jmh_results_flat.csv"))
    print("OK:", report_path)


def main() -> None:
    args = parse_args()
    project_root = args.project_root.resolve()
    module_dir = (project_root / args.module).resolve()
    jar_path = (project_root / args.jar).resolve()
    out_dir = (project_root / args.out_dir).resolve()
    include_re = re.compile(args.include)

    if args.rebuild_only:
        rebuild_from_existing(out_dir, include_re)
        return

    json_dir = out_dir / "json"
    logs_dir = out_dir / "logs"
    csv_dir = out_dir / "csv"
    plots_dir = out_dir / "plots"
    for d in (json_dir, logs_dir, csv_dir, plots_dir):
        d.mkdir(parents=True, exist_ok=True)

    if not args.skip_build:
        ensure_built(project_root, args.module)

    if not jar_path.exists():
        raise SystemExit(f"JMH jar not found: {jar_path}")

    benchmarks = list_benchmarks(jar_path, cwd=module_dir)
    benchmarks = [b for b in benchmarks if include_re.search(b)]

    if not benchmarks:
        raise SystemExit("No benchmarks matched.")

    print("Discovered benchmarks:")
    for b in benchmarks:
        print(" -", b)

    if args.list_only:
        return

    all_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {}
    class_overview_plots: Dict[str, Path] = {}
    extra_jmh_args = list(args.jmh_arg)
    if args.jmh_args.strip():
        extra_jmh_args.extend(shlex.split(args.jmh_args))
    if extra_jmh_args:
        print("Using JMH CLI overrides:", " ".join(extra_jmh_args))
    else:
        print("Using JMH defaults from benchmark annotations (@Warmup/@Measurement/@Fork).")

    for i, bench in enumerate(benchmarks, start=1):
        slug = benchmark_slug(bench)
        json_path = json_dir / f"{slug}.json"
        log_path = logs_dir / f"{slug}.log"
        metrics_csv = csv_dir / f"{slug}__resources.csv"
        jmh_csv = csv_dir / f"{slug}__jmh.csv"
        score_plot_path = plots_dir / f"{slug}__score.png"
        resource_plot_path = plots_dir / f"{slug}__resources.png"

        cmd = ["java", "-jar", str(jar_path), bench, "-foe", "true", "-rf", "json", "-rff", str(json_path)]
        cmd.extend(extra_jmh_args)

        print(f"[{i}/{len(benchmarks)}] Running {bench}")
        print("$", " ".join(shlex.quote(c) for c in cmd))
        rc, duration_s, metrics_df = run_benchmark_with_sampling(
            cmd=cmd,
            cwd=module_dir,
            sample_interval=args.sample_interval,
            log_path=log_path,
        )
        metrics_df.to_csv(metrics_csv, index=False)

        if rc != 0:
            raise SystemExit(f"Benchmark failed ({bench}). See log: {log_path}")
        if not json_path.exists():
            raise SystemExit(f"JMH output missing: {json_path}")

        records = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise SystemExit(f"Unexpected JMH JSON format: {json_path}")

        flat_df = flatten_jmh_records(records)
        flat_df.to_csv(jmh_csv, index=False)

        score_plot_actual = build_score_plot(flat_df, bench, score_plot_path)
        resource_plot_actual = build_resource_plot(metrics_df, bench, resource_plot_path)

        all_rows.extend(records)
        section = {
            "benchmark": bench,
            "rows": flat_df.to_dict(orient="records"),
            "score_plot": score_plot_actual.relative_to(out_dir),
            "resource_plot": resource_plot_actual.relative_to(out_dir),
            "jmh_csv": jmh_csv.relative_to(out_dir),
            "metrics_csv": metrics_csv.relative_to(out_dir),
        }
        sections.setdefault(benchmark_class(bench), []).append(section)

        summary_rows.append(
            {
                "benchmark": bench,
                "class": benchmark_class(bench),
                "method": benchmark_method(bench),
                "operation_kind": operation_kind(benchmark_method(bench)),
                "duration_s": duration_s,
                "avg_cpu_percent": stats_or_nan(metrics_df["cpu_percent"], "mean"),
                "p95_cpu_percent": stats_or_nan(metrics_df["cpu_percent"], "p95"),
                "avg_system_cpu_percent": stats_or_nan(metrics_df["system_cpu_percent"], "mean"),
                "p95_system_cpu_percent": stats_or_nan(metrics_df["system_cpu_percent"], "p95"),
                "peak_rss_mb": stats_or_nan(metrics_df["rss_mb"], "max"),
                "peak_system_memory_percent": stats_or_nan(metrics_df["system_memory_percent"], "max"),
                "avg_threads": stats_or_nan(metrics_df["threads"], "mean"),
                "max_read_mb": stats_or_nan(metrics_df["read_mb"], "max"),
                "max_write_mb": stats_or_nan(metrics_df["write_mb"], "max"),
                "jmh_cases": len(flat_df),
                "score_min": pd.to_numeric(flat_df["score"], errors="coerce").min(),
                "score_median": pd.to_numeric(flat_df["score"], errors="coerce").median(),
                "score_max": pd.to_numeric(flat_df["score"], errors="coerce").max(),
                "score_units": ", ".join(sorted(set(str(u) for u in flat_df["scoreUnit"].dropna().tolist()))),
            }
        )

    flat_all = flatten_jmh_records(all_rows)
    flat_all.to_csv(out_dir / "jmh_results_flat.csv", index=False)

    summary_df = pd.DataFrame(summary_rows).sort_values(["class", "method"]).reset_index(drop=True)
    summary_df.to_csv(out_dir / "operation_summary.csv", index=False)

    for class_name, class_df in summary_df.groupby("class", sort=True):
        overview_path = plots_dir / f"{benchmark_slug(class_name)}__overview.png"
        overview_actual = build_class_overview_plot(class_df, class_name, overview_path)
        class_overview_plots[class_name] = overview_actual.relative_to(out_dir)

    cmdline = " ".join(shlex.quote(x) for x in sys.argv)
    report_path = generate_report(out_dir, summary_df, sections, class_overview_plots, cmdline)

    print("OK:", (out_dir / "operation_summary.csv"))
    print("OK:", (out_dir / "jmh_results_flat.csv"))
    print("OK:", report_path)


if __name__ == "__main__":
    main()
