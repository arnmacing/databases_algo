#!/usr/bin/env python3
"""
Sequential JMH runner with async-profiler integration.

What it does:
1) Builds benchmarks jar (optional).
2) Lists all benchmarks from JMH.
3) Runs benchmarks one by one.
4) Profiles each benchmark with async-profiler (CPU + allocations by default).
5) Saves JMH CSV files and score plots.
6) Generates Markdown report with per-benchmark JMH tables and profiler artifacts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative
from plotly.subplots import make_subplots

BENCHMARK_RE = re.compile(r"^bench\.[A-Za-z0-9_.$]+\.[A-Za-z0-9_.$]+$")
X_PARAM_PRIORITY = ["n", "docs", "baseDocs", "bucketCapacity", "threshold", "wordsPerDoc", "seed"]
JFR_SUMMARY_EVENT_RE = re.compile(r"^\s+([A-Za-z0-9_.]+)\s+([0-9]+)\s+[0-9]+")
JFR_SUMMARY_DURATION_RE = re.compile(r"^\s*Duration:\s+(.+?)\s*$")
PROFILE_MODE_LABEL = {
    "Throughput": "thrpt",
    "AverageTime": "avgt",
    "SampleTime": "sample",
    "SingleShotTime": "ss",
}
PROFILE_MODE_ORDER = {
    "Throughput": 0,
    "AverageTime": 1,
    "SampleTime": 2,
    "SingleShotTime": 3,
}

_JFR_SUMMARY_CACHE: Dict[str, Dict[str, Any]] = {}
_JFR_CPULOAD_CACHE: Dict[str, List[Tuple[float, float, float]]] = {}


def parse_args() -> argparse.Namespace:
    default_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Run all JMH benchmarks sequentially with async-profiler and generate report."
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
        "--profiles-subdir",
        default="profiles",
        help="Subdirectory in out-dir for async-profiler artifacts (default: profiles).",
    )
    parser.add_argument(
        "--async-lib",
        type=Path,
        default=None,
        help="Path to libasyncProfiler.dylib/.so. If omitted, script tries common Homebrew locations.",
    )
    parser.add_argument(
        "--async-events",
        default="cpu,alloc",
        help="Comma-separated async-profiler events (default: cpu,alloc).",
    )
    parser.add_argument(
        "--async-output",
        default="jfr,collapsed,flamegraph",
        help="async-profiler output kinds (default: jfr,collapsed,flamegraph).",
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


def detect_default_async_lib() -> Optional[Path]:
    env = os.environ.get("ASYNC_PROFILER_LIB", "").strip()
    if env:
        p = Path(env).expanduser().resolve()
        if p.exists():
            return p

    candidates = [
        Path("/opt/homebrew/opt/async-profiler/lib/libasyncProfiler.dylib"),
        Path("/usr/local/opt/async-profiler/lib/libasyncProfiler.dylib"),
        Path("/opt/homebrew/lib/libasyncProfiler.dylib"),
        Path("/usr/local/lib/libasyncProfiler.dylib"),
        Path("/opt/homebrew/opt/async-profiler/lib/libasyncProfiler.so"),
        Path("/usr/local/opt/async-profiler/lib/libasyncProfiler.so"),
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def resolve_async_lib(explicit: Optional[Path]) -> Path:
    if explicit is not None:
        p = explicit.expanduser().resolve()
        if p.exists():
            return p
        raise SystemExit(f"async-profiler library not found: {p}")

    guessed = detect_default_async_lib()
    if guessed is not None:
        return guessed

    raise SystemExit(
        "async-profiler library not found. "
        "Install it (e.g. `brew install async-profiler`) and pass --async-lib <path-to-libasyncProfiler.dylib>."
    )


def parse_events(raw_events: str) -> List[str]:
    events = [e.strip() for e in raw_events.split(",") if e.strip()]
    if not events:
        raise SystemExit("--async-events is empty.")
    return events


def build_async_profiler_arg(
    async_lib: Path,
    profile_dir_rel: str,
    event: str,
    output: str,
) -> List[str]:
    lib_path = async_lib.as_posix()
    cfg = f"libPath={lib_path};event={event};output={output};dir={profile_dir_rel}"
    return ["-prof", f"async:{cfg}"]


def run_benchmark(cmd: List[str], cwd: Path, log_path: Path) -> Tuple[int, float]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.run(cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, text=True)
    duration_s = time.time() - start
    return proc.returncode, duration_s


def flatten_jmh_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for r in records:
        params = r.get("params") or {}

        pm = r.get("primaryMetric") or {}
        primary_row: Dict[str, Any] = {
            "benchmark": r.get("benchmark"),
            "metric": "primary",
            "mode": r.get("mode"),
            "score": pm.get("score"),
            "scoreError": pm.get("scoreError"),
            "scoreUnit": pm.get("scoreUnit"),
        }
        primary_row.update(params)
        rows.append(primary_row)

        secondary = r.get("secondaryMetrics") or {}
        if isinstance(secondary, dict):
            for metric_name, metric_obj in secondary.items():
                if not isinstance(metric_obj, dict):
                    continue
                if metric_obj.get("score") is None:
                    continue
                row: Dict[str, Any] = {
                    "benchmark": r.get("benchmark"),
                    "metric": str(metric_name),
                    "mode": r.get("mode"),
                    "score": metric_obj.get("score"),
                    "scoreError": metric_obj.get("scoreError"),
                    "scoreUnit": metric_obj.get("scoreUnit"),
                }
                row.update(params)
                rows.append(row)
    return pd.DataFrame(rows)


def primary_metrics_df(df: pd.DataFrame) -> pd.DataFrame:
    if "metric" not in df.columns:
        return df.copy()
    return df[df["metric"].astype(str) == "primary"].copy()


def choose_x_param(df: pd.DataFrame) -> Optional[str]:
    param_cols = [
        c
        for c in df.columns
        if c not in {"benchmark", "metric", "mode", "score", "scoreError", "scoreUnit"}
    ]
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
    short_benchmark = f"{benchmark_class(benchmark)}.{benchmark_method(benchmark)}"
    plot_df = primary_metrics_df(df)
    if plot_df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No primary JMH metrics", showarrow=False, x=0.5, y=0.5)
        fig.update_layout(title=short_benchmark, template="plotly_white")
        return write_figure(fig, out_path)

    x_param = choose_x_param(plot_df)
    plot_df = plot_df.copy()
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

    cols = 1
    rows = len(modes)

    fixed_cols = {"benchmark", "metric", "mode", "score", "scoreError", "scoreUnit", x_param}
    group_cols = [
        c
        for c in plot_df.columns
        if c not in fixed_cols and plot_df[c].astype(str).nunique(dropna=True) > 1
    ]

    def value_sort_key(v: Any) -> Tuple[int, float, str]:
        s = str(v)
        try:
            return (0, float(s), s)
        except ValueError:
            return (1, 0.0, s)

    def key_sort_key(key: Tuple[Any, ...]) -> Tuple[Any, ...]:
        return tuple(value_sort_key(v) for v in key)

    if group_cols:
        series_keys = [
            tuple(row)
            for row in plot_df[group_cols].drop_duplicates().itertuples(index=False, name=None)
        ]
        series_keys = sorted(series_keys, key=key_sort_key)
    else:
        series_keys = [("all",)]

    palette = qualitative.Plotly + qualitative.D3 + qualitative.Set2 + qualitative.Dark24
    color_map: Dict[Tuple[Any, ...], str] = {
        key: palette[i % len(palette)] for i, key in enumerate(series_keys)
    }
    shown_legend_keys: set[Tuple[Any, ...]] = set()

    fig = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=None,
        vertical_spacing=0.08 if rows > 1 else 0.06,
    )

    for idx, mode in enumerate(modes):
        row = (idx // cols) + 1
        col = (idx % cols) + 1
        mode_df = plot_df[plot_df["mode"].astype(str) == mode].copy()

        if group_cols:
            grouped: Iterable[Tuple[Any, pd.DataFrame]] = mode_df.groupby(group_cols, dropna=False, sort=True)
        else:
            grouped = [("all", mode_df)]

        for key, part in grouped:
            if not isinstance(key, tuple):
                key = (key,)
            series_key = key if group_cols else ("all",)
            if group_cols:
                if len(group_cols) == 1:
                    name = str(series_key[0])
                else:
                    name = " | ".join(str(v) for v in series_key)
            else:
                name = "all"
            part = part.copy()
            x_numeric = pd.to_numeric(part[x_param], errors="coerce")
            if x_numeric.notna().all():
                part["_x_numeric"] = x_numeric
                part = part.sort_values(by="_x_numeric")
            else:
                part = part.sort_values(by=x_param)
            fig.add_trace(
                go.Scatter(
                    x=part[x_param],
                    y=part["score"],
                    mode="lines+markers",
                    name=name,
                    legendgroup="|".join(str(v) for v in series_key),
                    showlegend=series_key not in shown_legend_keys,
                    line=dict(color=color_map.get(series_key)),
                    marker=dict(color=color_map.get(series_key)),
                    error_y=dict(type="data", array=part["scoreError"].fillna(0).tolist(), visible=True),
                    hovertemplate=f"{x_param}=%{{x}}<br>score=%{{y}}<extra></extra>",
                ),
                row=row,
                col=col,
            )
            shown_legend_keys.add(series_key)

        units = sorted(str(u) for u in mode_df["scoreUnit"].dropna().unique().tolist())
        y_title = "score" if not units else f"score ({', '.join(units)})"
        fig.update_xaxes(title_text=x_param, row=row, col=col)
        fig.update_yaxes(title_text=y_title, row=row, col=col)

    legend_title = "series"
    if len(group_cols) == 1:
        legend_title = group_cols[0]
    elif len(group_cols) > 1:
        legend_title = ", ".join(group_cols)
    show_legend = len(series_keys) > 1

    fig.update_layout(
        title=short_benchmark,
        legend_title=legend_title,
        template="plotly_white",
        width=1100,
        height=max(520, 420 * rows),
        showlegend=show_legend,
    )
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
    plot_df = class_df.copy().sort_values(["operation_kind", "method"]).reset_index(drop=True)
    plot_df["label"] = [f"{m}\n({k})" for m, k in zip(plot_df["method"], plot_df["operation_kind"])]

    duration = pd.to_numeric(plot_df.get("duration_s", pd.Series(dtype=float)), errors="coerce")
    median_score = pd.to_numeric(plot_df.get("score_median", pd.Series(dtype=float)), errors="coerce")

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        subplot_titles=("Duration, seconds", "Median JMH score"),
    )

    fig.add_trace(go.Bar(x=plot_df["label"], y=duration, name="duration, s"), row=1, col=1)
    fig.add_trace(go.Bar(x=plot_df["label"], y=median_score, name="score median"), row=2, col=1)

    fig.update_yaxes(title_text="seconds", row=1, col=1)
    fig.update_yaxes(title_text="score", row=2, col=1)
    fig.update_xaxes(title_text="operation", tickangle=-25, automargin=True, row=2, col=1)
    fig.update_layout(
        title=f"{class_name}: operations overview",
        template="plotly_white",
        showlegend=False,
        margin=dict(l=90, r=60, t=120, b=190),
        height=720,
    )
    return write_figure(fig, out_path)


def collect_profile_files(out_dir: Path, profiles_root: Path, slug: str) -> List[Path]:
    bench_dir = profiles_root / slug
    if not bench_dir.exists():
        return []
    files = sorted(p for p in bench_dir.rglob("*") if p.is_file())
    return [p.relative_to(out_dir) for p in files]


def parse_duration_seconds(raw: str) -> float:
    raw = raw.strip()
    if not raw:
        return 0.0

    hms = re.fullmatch(r"(\d+):(\d+):(\d+(?:\.\d+)?)", raw)
    if hms:
        h = int(hms.group(1))
        m = int(hms.group(2))
        s = float(hms.group(3))
        return h * 3600.0 + m * 60.0 + s

    total = 0.0
    for value, unit in re.findall(r"(\d+(?:\.\d+)?)\s*(ms|us|ns|h|m|s)", raw):
        v = float(value)
        if unit == "h":
            total += v * 3600.0
        elif unit == "m":
            total += v * 60.0
        elif unit == "s":
            total += v
        elif unit == "ms":
            total += v / 1000.0
        elif unit == "us":
            total += v / 1_000_000.0
        elif unit == "ns":
            total += v / 1_000_000_000.0
    return total


def parse_jfr_summary(jfr_path: Path) -> Dict[str, Any]:
    key = str(jfr_path.resolve())
    cached = _JFR_SUMMARY_CACHE.get(key)
    if cached is not None:
        return cached

    counts: Dict[str, int] = {}
    duration_s = 0.0
    try:
        out = run(["jfr", "summary", str(jfr_path)], cwd=jfr_path.parent).stdout
    except Exception:
        result = {"counts": counts, "duration_s": duration_s}
        _JFR_SUMMARY_CACHE[key] = result
        return result

    for line in out.splitlines():
        d = JFR_SUMMARY_DURATION_RE.match(line)
        if d:
            duration_s = parse_duration_seconds(d.group(1))
            continue
        m = JFR_SUMMARY_EVENT_RE.match(line)
        if not m:
            continue
        counts[m.group(1)] = int(m.group(2))

    result = {"counts": counts, "duration_s": duration_s}
    _JFR_SUMMARY_CACHE[key] = result
    return result


def parse_jfr_cpu_load(jfr_path: Path) -> List[Tuple[float, float, float]]:
    key = str(jfr_path.resolve())
    cached = _JFR_CPULOAD_CACHE.get(key)
    if cached is not None:
        return cached

    values: List[Tuple[float, float, float]] = []
    try:
        out = run(["jfr", "print", "--events", "jdk.CPULoad", "--json", str(jfr_path)], cwd=jfr_path.parent).stdout
        obj = json.loads(out)
        events = (((obj or {}).get("recording") or {}).get("events")) or []
        for ev in events:
            v = (ev or {}).get("values") or {}
            user = float(v.get("jvmUser", 0.0))
            system = float(v.get("jvmSystem", 0.0))
            machine = float(v.get("machineTotal", 0.0))
            values.append((user * 100.0, system * 100.0, machine * 100.0))
    except Exception:
        values = []

    _JFR_CPULOAD_CACHE[key] = values
    return values


def mean_or_nan(values: List[float]) -> float:
    if not values:
        return float("nan")
    return float(sum(values) / len(values))


def p95_or_nan(values: List[float]) -> float:
    if not values:
        return float("nan")
    sorted_v = sorted(values)
    idx = max(0, min(len(sorted_v) - 1, math.ceil(0.95 * len(sorted_v)) - 1))
    return float(sorted_v[idx])


def order_param_keys(keys: List[str]) -> List[str]:
    priority = [
        "bucketCapacity",
        "n",
        "docs",
        "baseDocs",
        "threshold",
        "wordsPerDoc",
        "seed",
    ]
    rank = {k: i for i, k in enumerate(priority)}
    return sorted(keys, key=lambda k: (rank.get(k, 10_000), k))


def parse_profile_case(bench: str, rel: Path) -> Tuple[str, Dict[str, str]]:
    trial = rel.parts[-2] if len(rel.parts) >= 2 else ""
    prefix = f"{bench}-"
    tail = trial[len(prefix):] if trial.startswith(prefix) else trial
    if not tail:
        return "unknown", {}

    tokens = tail.split("-")
    mode = tokens[0] if tokens else "unknown"
    params: Dict[str, str] = {}
    rest = tokens[1:]
    i = 0
    while i + 1 < len(rest):
        params[rest[i]] = rest[i + 1]
        i += 2
    return mode, params


def init_profile_case(mode: str, params: Dict[str, str]) -> Dict[str, Any]:
    return {
        "mode": mode,
        "params": dict(params),
        "cpu_profile_duration_s": 0.0,
        "cpu_execution_samples": 0.0,
        "cpu_load_events": 0.0,
        "alloc_profile_duration_s": 0.0,
        "alloc_tlab_events": 0.0,
        "alloc_outside_tlab_events": 0.0,
        "_jvm_user_values": [],
        "_jvm_system_values": [],
        "_machine_total_values": [],
    }


def summarize_profile_stats(out_dir: Path, profile_files: List[Path], bench: str) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "cpu_profile_duration_s": 0.0,
        "cpu_execution_samples": 0.0,
        "cpu_load_events": 0.0,
        "alloc_profile_duration_s": 0.0,
        "alloc_tlab_events": 0.0,
        "alloc_outside_tlab_events": 0.0,
        "by_case": [],
        "param_keys": [],
    }
    jvm_user_values: List[float] = []
    jvm_system_values: List[float] = []
    machine_total_values: List[float] = []
    cases: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Dict[str, Any]] = {}
    param_keys_set: set[str] = set()

    for rel in profile_files:
        p = out_dir / rel
        if p.suffix.lower() != ".jfr":
            continue

        summary = parse_jfr_summary(p)
        counts = summary.get("counts", {})
        duration_s = float(summary.get("duration_s", 0.0))
        name = p.name
        mode, params = parse_profile_case(bench, rel)
        for k in params:
            param_keys_set.add(k)
        case_key = (mode, tuple(sorted(params.items())))
        case = cases.setdefault(case_key, init_profile_case(mode, params))

        if name.startswith("jfr-cpu"):
            stats["cpu_profile_duration_s"] += duration_s
            stats["cpu_execution_samples"] += float(counts.get("jdk.ExecutionSample", 0))
            stats["cpu_load_events"] += float(counts.get("jdk.CPULoad", 0))
            case["cpu_profile_duration_s"] += duration_s
            case["cpu_execution_samples"] += float(counts.get("jdk.ExecutionSample", 0))
            case["cpu_load_events"] += float(counts.get("jdk.CPULoad", 0))
            for u, s, m in parse_jfr_cpu_load(p):
                jvm_user_values.append(u)
                jvm_system_values.append(s)
                machine_total_values.append(m)
                case["_jvm_user_values"].append(u)
                case["_jvm_system_values"].append(s)
                case["_machine_total_values"].append(m)
        elif name.startswith("jfr-alloc"):
            stats["alloc_profile_duration_s"] += duration_s
            stats["alloc_tlab_events"] += float(counts.get("jdk.ObjectAllocationInNewTLAB", 0))
            stats["alloc_outside_tlab_events"] += float(counts.get("jdk.ObjectAllocationOutsideTLAB", 0))
            case["alloc_profile_duration_s"] += duration_s
            case["alloc_tlab_events"] += float(counts.get("jdk.ObjectAllocationInNewTLAB", 0))
            case["alloc_outside_tlab_events"] += float(counts.get("jdk.ObjectAllocationOutsideTLAB", 0))

    stats["cpu_jvm_user_avg_pct"] = mean_or_nan(jvm_user_values)
    stats["cpu_jvm_user_p95_pct"] = p95_or_nan(jvm_user_values)
    stats["cpu_jvm_system_avg_pct"] = mean_or_nan(jvm_system_values)
    stats["cpu_jvm_system_p95_pct"] = p95_or_nan(jvm_system_values)
    stats["cpu_machine_total_avg_pct"] = mean_or_nan(machine_total_values)
    stats["cpu_machine_total_p95_pct"] = p95_or_nan(machine_total_values)
    stats["alloc_events_total"] = stats["alloc_tlab_events"] + stats["alloc_outside_tlab_events"]
    cpu_duration = stats["cpu_profile_duration_s"]
    alloc_duration = stats["alloc_profile_duration_s"]
    stats["cpu_samples_per_sec"] = (
        stats["cpu_execution_samples"] / cpu_duration if cpu_duration > 0 else float("nan")
    )
    stats["alloc_events_per_sec"] = (
        stats["alloc_events_total"] / alloc_duration if alloc_duration > 0 else float("nan")
    )
    stats["alloc_tlab_share_pct"] = (
        (100.0 * stats["alloc_tlab_events"] / stats["alloc_events_total"])
        if stats["alloc_events_total"] > 0
        else float("nan")
    )

    ordered_param_keys = order_param_keys(list(param_keys_set))
    finalized_cases: List[Dict[str, Any]] = []
    for case in cases.values():
        c: Dict[str, Any] = {k: v for k, v in case.items() if not k.startswith("_")}
        c["cpu_jvm_user_avg_pct"] = mean_or_nan(case["_jvm_user_values"])
        c["cpu_jvm_user_p95_pct"] = p95_or_nan(case["_jvm_user_values"])
        c["cpu_jvm_system_avg_pct"] = mean_or_nan(case["_jvm_system_values"])
        c["cpu_jvm_system_p95_pct"] = p95_or_nan(case["_jvm_system_values"])
        c["cpu_machine_total_avg_pct"] = mean_or_nan(case["_machine_total_values"])
        c["cpu_machine_total_p95_pct"] = p95_or_nan(case["_machine_total_values"])
        c["alloc_events_total"] = c["alloc_tlab_events"] + c["alloc_outside_tlab_events"]
        c["cpu_samples_per_sec"] = (
            c["cpu_execution_samples"] / c["cpu_profile_duration_s"]
            if c["cpu_profile_duration_s"] > 0
            else float("nan")
        )
        c["alloc_events_per_sec"] = (
            c["alloc_events_total"] / c["alloc_profile_duration_s"]
            if c["alloc_profile_duration_s"] > 0
            else float("nan")
        )
        c["alloc_tlab_share_pct"] = (
            (100.0 * c["alloc_tlab_events"] / c["alloc_events_total"])
            if c["alloc_events_total"] > 0
            else float("nan")
        )
        finalized_cases.append(c)

    def case_sort_key(c: Dict[str, Any]) -> Tuple[Any, ...]:
        mode = str(c.get("mode", "unknown"))
        params = c.get("params", {}) or {}
        return (
            PROFILE_MODE_ORDER.get(mode, 99),
            mode,
            *[str(params.get(k, "")) for k in ordered_param_keys],
        )

    finalized_cases.sort(key=case_sort_key)
    stats["by_case"] = finalized_cases
    stats["param_keys"] = ordered_param_keys
    return stats


def build_profile_metrics_lines(stats: Dict[str, float]) -> List[str]:
    def fmt_float(v: float) -> str:
        if pd.isna(v):
            return "-"
        return f"{v:.3f}"

    def fmt_int(v: float) -> str:
        if pd.isna(v):
            return "-"
        return str(int(round(v)))

    rows = [
        ("cpu_profile_duration_s", fmt_float(stats.get("cpu_profile_duration_s", float("nan")))),
        ("cpu_jvm_user_avg_pct", fmt_float(stats.get("cpu_jvm_user_avg_pct", float("nan")))),
        ("cpu_jvm_user_p95_pct", fmt_float(stats.get("cpu_jvm_user_p95_pct", float("nan")))),
        ("cpu_jvm_system_avg_pct", fmt_float(stats.get("cpu_jvm_system_avg_pct", float("nan")))),
        ("cpu_jvm_system_p95_pct", fmt_float(stats.get("cpu_jvm_system_p95_pct", float("nan")))),
        ("cpu_machine_total_avg_pct", fmt_float(stats.get("cpu_machine_total_avg_pct", float("nan")))),
        ("cpu_machine_total_p95_pct", fmt_float(stats.get("cpu_machine_total_p95_pct", float("nan")))),
        ("cpu_execution_samples_total", fmt_int(stats.get("cpu_execution_samples", float("nan")))),
        ("cpu_samples_per_sec", fmt_float(stats.get("cpu_samples_per_sec", float("nan")))),
        ("alloc_profile_duration_s", fmt_float(stats.get("alloc_profile_duration_s", float("nan")))),
        ("alloc_events_total", fmt_int(stats.get("alloc_events_total", float("nan")))),
        ("alloc_events_per_sec", fmt_float(stats.get("alloc_events_per_sec", float("nan")))),
        ("alloc_tlab_events", fmt_int(stats.get("alloc_tlab_events", float("nan")))),
        ("alloc_outside_tlab_events", fmt_int(stats.get("alloc_outside_tlab_events", float("nan")))),
        ("alloc_tlab_share_pct", fmt_float(stats.get("alloc_tlab_share_pct", float("nan")))),
    ]
    lines = ["| metric | value |", "|---|---:|"]
    for k, v in rows:
        lines.append(f"| {k} | {v} |")
    lines.append("")
    return lines


def build_profile_case_lines(stats: Dict[str, Any]) -> List[str]:
    cases = stats.get("by_case", []) or []
    if not cases:
        return []

    metric_cols = [
        "cpu_jvm_user_avg_pct",
        "cpu_machine_total_avg_pct",
        "cpu_samples_per_sec",
        "alloc_events_per_sec",
        "alloc_events_total",
        "alloc_tlab_share_pct",
    ]

    rows: List[Dict[str, Any]] = []
    for c in cases:
        params = c.get("params", {}) or {}
        row: Dict[str, Any] = {"mode": c.get("mode")}
        row.update(params)
        for m in metric_cols:
            row[m] = c.get(m)
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return []

    for m in metric_cols:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors="coerce")

    def sort_values(values: List[Any]) -> List[Any]:
        def sort_key(v: Any) -> Tuple[int, float, str]:
            s = str(v)
            try:
                return (0, float(s), s)
            except ValueError:
                return (1, 0.0, s)
        return sorted(values, key=sort_key)

    def fmt_metric(v: Any, metric_name: str) -> str:
        if pd.isna(v):
            return "-"
        if metric_name == "alloc_events_total":
            return str(int(round(float(v))))
        return f"{float(v):.3f}"

    usable_metrics = [m for m in metric_cols if m in df.columns and not df[m].isna().all()]
    if not usable_metrics:
        return []

    param_cols = [c for c in df.columns if c not in {"mode", "seed", *metric_cols}]
    varying_param_cols = [c for c in param_cols if df[c].astype(str).nunique(dropna=True) > 1]
    if not varying_param_cols:
        return []

    lines: List[str] = []
    if len(varying_param_cols) == 1:
        idx_col = varying_param_cols[0]
        agg = df.groupby([idx_col], dropna=False, as_index=False)[usable_metrics].mean(numeric_only=True)
        idx_values = sort_values(agg[idx_col].dropna().unique().tolist())
        header = [idx_col, *usable_metrics]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] + ["---:"] * len(usable_metrics)) + "|")
        for idx_val in idx_values:
            cell_df = agg[agg[idx_col].astype(str) == str(idx_val)]
            if cell_df.empty:
                continue
            row_cells = [str(idx_val).replace("|", "\\|")]
            for metric_name in usable_metrics:
                row_cells.append(fmt_metric(cell_df[metric_name].iloc[0], metric_name))
            lines.append("| " + " | ".join(row_cells) + " |")
        lines.append("")
        return lines

    row_col: str
    col_col: str
    if any(c.lower() == "n" for c in varying_param_cols) and any(c.lower() == "bucketcapacity" for c in varying_param_cols):
        row_col = next(c for c in varying_param_cols if c.lower() == "n")
        col_col = next(c for c in varying_param_cols if c.lower() == "bucketcapacity")
    else:
        row_col, col_col = sorted(varying_param_cols)[:2]

    agg = (
        df.groupby([row_col, col_col], dropna=False, as_index=False)[usable_metrics]
        .mean(numeric_only=True)
    )
    row_values = sort_values(agg[row_col].dropna().unique().tolist())
    col_values = sort_values(agg[col_col].dropna().unique().tolist())

    for metric_name in usable_metrics:
        lines.append(f"**{metric_name}**")
        lines.append("")
        header = [f"{row_col} \\ {col_col}", *[str(v).replace("|", "\\|") for v in col_values]]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] * len(header)) + "|")
        for row_val in row_values:
            row_cells = [str(row_val).replace("|", "\\|")]
            for col_val in col_values:
                cell_df = agg[
                    (agg[row_col].astype(str) == str(row_val))
                    & (agg[col_col].astype(str) == str(col_val))
                ]
                if cell_df.empty:
                    row_cells.append("-")
                    continue
                value = cell_df[metric_name].iloc[0]
                row_cells.append(fmt_metric(value, metric_name))
            lines.append("| " + " | ".join(row_cells) + " |")
        lines.append("")
    return lines


def build_jmh_table_lines(rows: List[Dict[str, Any]]) -> List[str]:
    df = pd.DataFrame(rows)
    if df.empty:
        return ["_No JMH rows._", ""]
    if "metric" in df.columns:
        primary = df[df["metric"].astype(str) == "primary"].copy()
        if not primary.empty:
            df = primary
        else:
            df = df[df["metric"].astype(str) != "async"].copy()
        if df.empty:
            return ["_No JMH rows after filtering technical profiler rows._", ""]

    df = df.copy()
    if "mode" not in df.columns:
        df["mode"] = "unknown"
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    if "scoreError" in df.columns:
        df["scoreError"] = pd.to_numeric(df["scoreError"], errors="coerce")

    fixed_cols = {"benchmark", "metric", "mode", "score", "scoreError", "scoreUnit"}
    param_cols = sorted(c for c in df.columns if c not in fixed_cols)
    param_cols = [c for c in param_cols if c != "seed"]

    def sort_values(values: List[Any]) -> List[Any]:
        def sort_key(v: Any) -> Tuple[int, float, str]:
            s = str(v)
            try:
                return (0, float(s), s)
            except ValueError:
                return (1, 0.0, s)
        return sorted(values, key=sort_key)

    def fmt_num(v: Any) -> str:
        if pd.isna(v):
            return "-"
        return f"{float(v):.6f}"

    def fmt_score(score: Any, score_error: Any, score_unit: str) -> str:
        if pd.isna(score):
            return "-"
        score_txt = fmt_num(score)
        unit_txt = score_unit if score_unit else ""
        if pd.isna(score_error):
            return (f"{score_txt} {unit_txt}").strip()
        return (f"{score_txt} +- {fmt_num(score_error)} {unit_txt}").strip()

    def mode_order_key(mode_name: str) -> Tuple[int, str]:
        mode_label = PROFILE_MODE_LABEL.get(mode_name, mode_name)
        order = {
            "SingleShotTime": 0,
            "ss": 0,
            "AverageTime": 1,
            "avgt": 1,
            "SampleTime": 2,
            "sample": 2,
            "Throughput": 3,
            "thrpt": 3,
        }
        return (order.get(mode_name, 99), mode_label)

    def pick_param_col(columns: List[str], expected: str) -> Optional[str]:
        expected_l = expected.lower()
        for col in columns:
            if col.lower() == expected_l:
                return col
        return None

    def pick_unit(unit_series: Optional[pd.Series]) -> str:
        if unit_series is None or unit_series.empty:
            return ""
        unit_non_na = unit_series.dropna().astype(str)
        if unit_non_na.empty:
            return ""
        return unit_non_na.mode().iloc[0]

    mode_values = sorted(df["mode"].astype(str).unique().tolist(), key=mode_order_key)
    varying_param_cols_all = [c for c in param_cols if df[c].astype(str).nunique(dropna=True) > 1]

    if not mode_values:
        return ["_No JMH rows._", ""]

    lines: List[str] = []
    if len(varying_param_cols_all) == 0:
        mode_headers = [PROFILE_MODE_LABEL.get(m, m) for m in mode_values]
        lines.append("| " + " | ".join(mode_headers) + " |")
        lines.append("|" + "|".join(["---:"] * len(mode_headers)) + "|")
        row_cells: List[str] = []
        for mode_val in mode_values:
            mode_df = df[df["mode"].astype(str) == mode_val].copy()
            score = mode_df["score"].mean() if "score" in mode_df.columns else float("nan")
            score_error = mode_df["scoreError"].mean() if "scoreError" in mode_df.columns else float("nan")
            unit = pick_unit(mode_df.get("scoreUnit"))
            row_cells.append(fmt_score(score, score_error, unit))
        lines.append("| " + " | ".join(row_cells) + " |")
        lines.append("")
        return lines

    if len(varying_param_cols_all) == 1:
        idx_col = varying_param_cols_all[0]
        idx_values = sort_values(df[idx_col].dropna().unique().tolist())
        mode_headers = [PROFILE_MODE_LABEL.get(m, m) for m in mode_values]
        lines.append("| " + " | ".join([idx_col, *mode_headers]) + " |")
        lines.append("|" + "|".join(["---"] + ["---:"] * len(mode_headers)) + "|")
        for idx_val in idx_values:
            row_cells = [str(idx_val).replace("|", "\\|")]
            for mode_val in mode_values:
                cell_df = df[
                    (df["mode"].astype(str) == mode_val)
                    & (df[idx_col].astype(str) == str(idx_val))
                ]
                if cell_df.empty:
                    row_cells.append("-")
                    continue
                score = cell_df["score"].mean() if "score" in cell_df.columns else float("nan")
                score_error = cell_df["scoreError"].mean() if "scoreError" in cell_df.columns else float("nan")
                unit = pick_unit(cell_df.get("scoreUnit"))
                row_cells.append(fmt_score(score, score_error, unit))
            lines.append("| " + " | ".join(row_cells) + " |")
        lines.append("")
        return lines

    # 2+ varying params: keep pivot per mode
    for mode_val in mode_values:
        mode_df = df[df["mode"].astype(str) == mode_val].copy()
        mode_label = PROFILE_MODE_LABEL.get(mode_val, mode_val)
        if len(mode_values) > 1:
            lines.append(f"**{mode_label}**")
            lines.append("")

        varying_param_cols = [c for c in varying_param_cols_all if mode_df[c].astype(str).nunique(dropna=True) > 1]

        if not varying_param_cols:
            score = mode_df["score"].mean() if "score" in mode_df.columns else float("nan")
            score_error = mode_df["scoreError"].mean() if "scoreError" in mode_df.columns else float("nan")
            unit = pick_unit(mode_df.get("scoreUnit"))
            lines.append("| value |")
            lines.append("|---:|")
            lines.append(f"| {fmt_score(score, score_error, unit)} |")
            lines.append("")
            continue

        if len(varying_param_cols) == 1:
            idx_col = varying_param_cols[0]
            idx_values = sort_values(mode_df[idx_col].dropna().unique().tolist())
            lines.append(f"| {idx_col} | value |")
            lines.append("|---|---:|")
            for idx_val in idx_values:
                cell_df = mode_df[mode_df[idx_col].astype(str) == str(idx_val)]
                score = cell_df["score"].mean() if "score" in cell_df.columns else float("nan")
                score_error = cell_df["scoreError"].mean() if "scoreError" in cell_df.columns else float("nan")
                unit = pick_unit(cell_df.get("scoreUnit"))
                lines.append(f"| {str(idx_val).replace('|', '\\|')} | {fmt_score(score, score_error, unit)} |")
            lines.append("")
            continue

        row_col: str
        col_col: str
        n_col = pick_param_col(varying_param_cols, "n")
        bucket_col = pick_param_col(varying_param_cols, "bucketCapacity")
        if n_col is not None and bucket_col is not None:
            row_col, col_col = n_col, bucket_col
        else:
            row_col, col_col = sorted(varying_param_cols)[:2]

        row_values = sort_values(mode_df[row_col].dropna().unique().tolist())
        col_values = sort_values(mode_df[col_col].dropna().unique().tolist())
        header = [f"{row_col} \\ {col_col}", *[str(v).replace("|", "\\|") for v in col_values]]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] * len(header)) + "|")
        for row_val in row_values:
            row_cells = [str(row_val).replace("|", "\\|")]
            for col_val in col_values:
                cell_df = mode_df[
                    (mode_df[row_col].astype(str) == str(row_val))
                    & (mode_df[col_col].astype(str) == str(col_val))
                ]
                if cell_df.empty:
                    row_cells.append("-")
                    continue
                score = cell_df["score"].mean() if "score" in cell_df.columns else float("nan")
                score_error = cell_df["scoreError"].mean() if "scoreError" in cell_df.columns else float("nan")
                unit = pick_unit(cell_df.get("scoreUnit"))
                row_cells.append(fmt_score(score, score_error, unit))
            lines.append("| " + " | ".join(row_cells) + " |")
        lines.append("")

    return lines


def build_perfect_hash_additional_lines(bench_sections: Dict[str, List[Dict[str, Any]]]) -> List[str]:
    target_rows: List[Dict[str, Any]] = []
    for class_name, sections in bench_sections.items():
        if class_name != "PerfectHashingBenchmark":
            continue
        for section in sections:
            bench = str(section.get("benchmark", ""))
            if benchmark_method(bench) != "buildIndex":
                continue
            target_rows.extend(section.get("rows", []))

    if not target_rows:
        return []

    df = pd.DataFrame(target_rows)
    if df.empty or "metric" not in df.columns:
        return []

    wanted_metrics = ["secondaryTableSize", "primaryCollisions"]
    df = df[df["metric"].astype(str).isin(wanted_metrics)].copy()
    if df.empty:
        return []

    if "mode" in df.columns and not df["mode"].dropna().empty:
        mode_values = [str(m) for m in df["mode"].dropna().unique().tolist()]
        selected_mode = "avgt" if "avgt" in mode_values else sorted(mode_values)[0]
        df = df[df["mode"].astype(str) == selected_mode].copy()
        if df.empty:
            return []

    fixed_cols = {"benchmark", "metric", "mode", "score", "scoreError", "scoreUnit", "seed"}
    param_cols = [c for c in df.columns if c not in fixed_cols]
    if not param_cols:
        return []
    idx_col = "n" if "n" in param_cols else sorted(param_cols)[0]

    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    if "scoreError" in df.columns:
        df["scoreError"] = pd.to_numeric(df["scoreError"], errors="coerce")
    else:
        df["scoreError"] = float("nan")

    def sort_values(values: List[Any]) -> List[Any]:
        def sort_key(v: Any) -> Tuple[int, float, str]:
            s = str(v)
            try:
                return (0, float(s), s)
            except ValueError:
                return (1, 0.0, s)
        return sorted(values, key=sort_key)

    def fmt_cell(score: Any, score_error: Any, unit: str) -> str:
        if pd.isna(score):
            return "-"
        if unit == "#":
            score_txt = str(int(round(float(score))))
            if pd.isna(score_error):
                return score_txt
            err_txt = str(int(round(float(score_error))))
            return f"{score_txt} +- {err_txt}"
        score_txt = f"{float(score):.6f}"
        if pd.isna(score_error):
            return f"{score_txt} {unit}".strip()
        return f"{score_txt} +- {float(score_error):.6f} {unit}".strip()

    metrics_available = [m for m in wanted_metrics if m in set(df["metric"].astype(str))]
    if not metrics_available:
        return []
    idx_values = sort_values(df[idx_col].dropna().unique().tolist())

    metric_headers: List[str] = []
    for metric_name in metrics_available:
        metric_df = df[df["metric"].astype(str) == metric_name]
        unit = ""
        if "scoreUnit" in metric_df.columns and not metric_df["scoreUnit"].dropna().empty:
            unit = metric_df["scoreUnit"].dropna().astype(str).mode().iloc[0]
        if unit and unit != "---":
            metric_headers.append(f"{metric_name} ({unit})")
        else:
            metric_headers.append(metric_name)

    lines: List[str] = []
    lines.append("## Дополнительно")
    lines.append("")
    lines.append("### PerfectHashingBenchmark.buildIndex")
    lines.append("")
    lines.append("| " + " | ".join([idx_col, *metric_headers]) + " |")
    lines.append("|" + "|".join(["---"] + ["---:"] * len(metric_headers)) + "|")
    for idx_val in idx_values:
        row_cells = [str(idx_val).replace("|", "\\|")]
        for metric_name in metrics_available:
            cell_df = df[
                (df[idx_col].astype(str) == str(idx_val))
                & (df["metric"].astype(str) == metric_name)
            ]
            if cell_df.empty:
                row_cells.append("-")
                continue
            score = cell_df["score"].mean()
            score_error = cell_df["scoreError"].mean()
            unit_series = cell_df.get("scoreUnit")
            unit = ""
            if unit_series is not None and not unit_series.empty:
                unit_non_na = unit_series.dropna().astype(str)
                if not unit_non_na.empty:
                    unit = unit_non_na.mode().iloc[0]
            row_cells.append(fmt_cell(score, score_error, unit))
        lines.append("| " + " | ".join(row_cells) + " |")
    lines.append("")
    return lines


def summarize_score_df(flat_df: pd.DataFrame) -> pd.DataFrame:
    p = primary_metrics_df(flat_df)
    if p.empty:
        return p
    p = p.copy()
    p["score"] = pd.to_numeric(p["score"], errors="coerce")
    return p


def generate_report(
    out_dir: Path,
    operation_summary: pd.DataFrame,
    bench_sections: Dict[str, List[Dict[str, Any]]],
    cmdline: str,
) -> Path:
    report_path = out_dir / "report.md"
    lines: List[str] = []

    for class_name in sorted(bench_sections):
        lines.append(f"## {class_name}")
        lines.append("")

        for section in sorted(bench_sections[class_name], key=lambda s: s["benchmark"]):
            bench = section["benchmark"]
            rows = section["rows"]
            score_plot = section["score_plot"]
            profile_stats: Dict[str, float] = section.get("profile_stats", {})

            lines.append(f"### {benchmark_method(bench)}")
            lines.append("")
            lines.append(f'<img src="{score_plot.as_posix()}" alt="score" width="72%">')
            lines.append("")

            lines.append("#### JMH Results")
            lines.extend(build_jmh_table_lines(rows))

            profile_lines = build_profile_case_lines(profile_stats)
            if profile_lines:
                lines.append("#### Async Profiler Metrics by Params/Mode")
                lines.extend(profile_lines)

            lines.append("#### Notes")
            lines.append("```text")
            lines.append("- ")
            lines.append("```")
            lines.append("")

    extra_lines = build_perfect_hash_additional_lines(bench_sections)
    if extra_lines:
        lines.extend(extra_lines)

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def build_summary_row(bench: str, duration_s: float, flat_df: pd.DataFrame, profile_files: List[Path]) -> Dict[str, Any]:
    score_df = summarize_score_df(flat_df)
    score_col = pd.to_numeric(score_df.get("score", pd.Series(dtype=float)), errors="coerce")

    units = ", ".join(sorted(set(str(u) for u in score_df.get("scoreUnit", pd.Series(dtype=object)).dropna().tolist())))
    return {
        "benchmark": bench,
        "class": benchmark_class(bench),
        "method": benchmark_method(bench),
        "operation_kind": operation_kind(benchmark_method(bench)),
        "duration_s": duration_s,
        "profile_files": len(profile_files),
        "jmh_cases": len(score_df),
        "score_min": score_col.min(),
        "score_median": score_col.median(),
        "score_max": score_col.max(),
        "score_units": units,
    }


def rebuild_from_existing(out_dir: Path, include_re: re.Pattern[str], profiles_subdir: str) -> None:
    json_dir = out_dir / "json"
    csv_dir = out_dir / "csv"
    plots_dir = out_dir / "plots"
    profiles_dir = out_dir / profiles_subdir
    for d in (json_dir, csv_dir, plots_dir, profiles_dir):
        d.mkdir(parents=True, exist_ok=True)

    json_files = sorted(json_dir.glob("*.json"))
    if not json_files:
        raise SystemExit(f"No JSON files found in: {json_dir}")

    runtime_csv = out_dir / "benchmark_runtime.csv"
    runtime_map: Dict[str, float] = {}
    if runtime_csv.exists():
        runtime_df = pd.read_csv(runtime_csv)
        if {"benchmark", "duration_s"}.issubset(runtime_df.columns):
            for _, row in runtime_df.iterrows():
                runtime_map[str(row["benchmark"])] = float(row["duration_s"])

    all_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {}

    for json_path in json_files:
        records = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(records, list) or not records:
            continue
        bench = str(records[0].get("benchmark") or "")
        if not bench or not include_re.search(bench):
            continue

        slug = benchmark_slug(bench)
        jmh_csv = csv_dir / f"{slug}__jmh.csv"
        score_plot_path = plots_dir / f"{slug}__score.png"

        flat_df = flatten_jmh_records(records)
        flat_df.to_csv(jmh_csv, index=False)
        score_plot_actual = build_score_plot(flat_df, bench, score_plot_path)
        profile_files = collect_profile_files(out_dir, profiles_dir, slug)
        profile_stats = summarize_profile_stats(out_dir, profile_files, bench)

        all_rows.extend(records)
        section = {
            "benchmark": bench,
            "rows": flat_df.to_dict(orient="records"),
            "score_plot": score_plot_actual.relative_to(out_dir),
            "jmh_csv": jmh_csv.relative_to(out_dir),
            "profile_stats": profile_stats,
        }
        sections.setdefault(benchmark_class(bench), []).append(section)

        summary_rows.append(build_summary_row(bench, runtime_map.get(bench, float("nan")), flat_df, profile_files))

    if not summary_rows:
        raise SystemExit("No matching benchmarks found in existing JSON files.")

    flat_all = flatten_jmh_records(all_rows)
    flat_all.to_csv(out_dir / "jmh_results_flat.csv", index=False)
    summary_df = pd.DataFrame(summary_rows).sort_values(["class", "method"]).reset_index(drop=True)
    summary_df.to_csv(out_dir / "operation_summary.csv", index=False)

    cmdline = " ".join(shlex.quote(x) for x in sys.argv)
    report_path = generate_report(out_dir, summary_df, sections, cmdline)
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
        rebuild_from_existing(out_dir, include_re, args.profiles_subdir)
        return

    json_dir = out_dir / "json"
    logs_dir = out_dir / "logs"
    csv_dir = out_dir / "csv"
    plots_dir = out_dir / "plots"
    profiles_dir = out_dir / args.profiles_subdir
    for d in (json_dir, logs_dir, csv_dir, plots_dir, profiles_dir):
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

    async_lib = resolve_async_lib(args.async_lib)
    events = parse_events(args.async_events)

    all_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []
    sections: Dict[str, List[Dict[str, Any]]] = {}
    runtime_rows: List[Dict[str, Any]] = []

    extra_jmh_args = list(args.jmh_arg)
    if args.jmh_args.strip():
        extra_jmh_args.extend(shlex.split(args.jmh_args))
    if extra_jmh_args:
        print("Using JMH CLI overrides:", " ".join(extra_jmh_args))
    else:
        print("Using JMH defaults from benchmark annotations (@Warmup/@Measurement/@Fork).")

    primary_event = events[0]
    extra_events = events[1:]

    print("Using async-profiler lib:", async_lib)
    print("Using async events:", ", ".join(events))
    print("Using async output:", args.async_output)
    if extra_events:
        print(
            "Note: JMH runs once for primary event "
            f"'{primary_event}', then repeats for extra events: {', '.join(extra_events)}."
        )

    for i, bench in enumerate(benchmarks, start=1):
        slug = benchmark_slug(bench)
        json_path = json_dir / f"{slug}.json"
        log_path = logs_dir / f"{slug}.log"
        jmh_csv = csv_dir / f"{slug}__jmh.csv"
        score_plot_path = plots_dir / f"{slug}__score.png"

        primary_profile_dir = profiles_dir / slug / primary_event
        primary_profile_dir.mkdir(parents=True, exist_ok=True)
        primary_profile_dir_rel = os.path.relpath(primary_profile_dir, module_dir)

        cmd = ["java", "-jar", str(jar_path), bench, "-foe", "true", "-rf", "json", "-rff", str(json_path)]
        cmd.extend(extra_jmh_args)
        cmd.extend(build_async_profiler_arg(async_lib, primary_profile_dir_rel, primary_event, args.async_output))

        print(f"[{i}/{len(benchmarks)}] Running {bench}")
        print("$", " ".join(shlex.quote(c) for c in cmd))
        rc, duration_s = run_benchmark(cmd=cmd, cwd=module_dir, log_path=log_path)

        if rc != 0:
            raise SystemExit(f"Benchmark failed ({bench}). See log: {log_path}")
        if not json_path.exists():
            raise SystemExit(f"JMH output missing: {json_path}")

        for event in extra_events:
            event_profile_dir = profiles_dir / slug / event
            event_profile_dir.mkdir(parents=True, exist_ok=True)
            event_profile_dir_rel = os.path.relpath(event_profile_dir, module_dir)
            event_log_path = logs_dir / f"{slug}__{event}.log"
            cmd_event = ["java", "-jar", str(jar_path), bench, "-foe", "true"]
            cmd_event.extend(extra_jmh_args)
            cmd_event.extend(build_async_profiler_arg(async_lib, event_profile_dir_rel, event, args.async_output))

            print(f"    -> profiling extra event: {event}")
            print("$", " ".join(shlex.quote(c) for c in cmd_event))
            event_rc, _ = run_benchmark(cmd=cmd_event, cwd=module_dir, log_path=event_log_path)
            if event_rc != 0:
                raise SystemExit(
                    f"Benchmark profiling failed ({bench}, event={event}). See log: {event_log_path}"
                )

        records = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise SystemExit(f"Unexpected JMH JSON format: {json_path}")

        flat_df = flatten_jmh_records(records)
        flat_df.to_csv(jmh_csv, index=False)
        score_plot_actual = build_score_plot(flat_df, bench, score_plot_path)
        profile_files = collect_profile_files(out_dir, profiles_dir, slug)
        profile_stats = summarize_profile_stats(out_dir, profile_files, bench)

        all_rows.extend(records)
        runtime_rows.append({"benchmark": bench, "duration_s": duration_s})

        section = {
            "benchmark": bench,
            "rows": flat_df.to_dict(orient="records"),
            "score_plot": score_plot_actual.relative_to(out_dir),
            "jmh_csv": jmh_csv.relative_to(out_dir),
            "profile_stats": profile_stats,
        }
        sections.setdefault(benchmark_class(bench), []).append(section)

        summary_rows.append(build_summary_row(bench, duration_s, flat_df, profile_files))

    flat_all = flatten_jmh_records(all_rows)
    flat_all.to_csv(out_dir / "jmh_results_flat.csv", index=False)

    summary_df = pd.DataFrame(summary_rows).sort_values(["class", "method"]).reset_index(drop=True)
    summary_df.to_csv(out_dir / "operation_summary.csv", index=False)
    pd.DataFrame(runtime_rows).to_csv(out_dir / "benchmark_runtime.csv", index=False)

    cmdline = " ".join(shlex.quote(x) for x in sys.argv)
    report_path = generate_report(out_dir, summary_df, sections, cmdline)

    print("OK:", (out_dir / "operation_summary.csv"))
    print("OK:", (out_dir / "jmh_results_flat.csv"))
    print("OK:", (out_dir / "benchmark_runtime.csv"))
    print("OK:", report_path)


if __name__ == "__main__":
    main()
