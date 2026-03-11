# Default usage:
#   python3 hw01/tools/jmh_html_report.py
# Optional explicit JSON:
#   python3 hw01/tools/jmh_html_report.py hw01/target/jmh-all.json
import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

X_PARAM_PRIORITY = [
    "bucketCapacity",
    "n",
    "docs",
    "baseDocs",
    "threshold",
    "wordsPerDoc",
    "seed",
]

GC_METRICS = [
    {
        "key": "gc.alloc.rate.norm",
        "id": "gc_alloc_rate_norm",
        "label": "GC alloc rate norm",
    },
    {
        "key": "gc.alloc.rate",
        "id": "gc_alloc_rate",
        "label": "GC alloc rate",
    },
    {
        "key": "gc.count",
        "id": "gc_count",
        "label": "GC count",
    },
    {
        "key": "gc.time",
        "id": "gc_time",
        "label": "GC time",
    },
]


def _short_benchmark_name(full_name: str) -> str:
    return full_name.split(".")[-1]


def _algorithm_name(full_benchmark: str) -> str:
    # bench.ExtendableHashTableBenchmark.getHit -> ExtendableHashTableBenchmark
    parts = (full_benchmark or "").split(".")
    if len(parts) >= 2:
        return parts[-2]
    return full_benchmark or "(unknown)"


def _algorithm_heading(algorithm: str) -> str:
    # Человекочитаемые заголовки разделов по алгоритмам
    friendly = {
        "ExtendableHashTableBenchmark": "Extendable Hashing",
        "PerfectHashingBenchmark": "Perfect Hashing",
        "LshBenchmark": "LSH",
    }
    name = friendly.get(algorithm, algorithm)
    return f"{name} ({algorithm})"


def _parse_param_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return s
    return v


def _flatten_raw_data(raw_data: Any) -> List[float]:
    out: List[float] = []

    def walk(v: Any) -> None:
        if isinstance(v, (int, float)):
            out.append(float(v))
        elif isinstance(v, list):
            for item in v:
                walk(item)

    walk(raw_data)
    return out


def _try_convert_numeric_column(df: pd.DataFrame, col: str) -> None:
    # Переводим колонку в numeric, если почти все непустые значения числовые.
    non_na = df[col].notna().sum()
    if non_na == 0:
        return
    converted = pd.to_numeric(df[col], errors="coerce")
    numeric_count = converted.notna().sum()
    if numeric_count >= non_na:
        df[col] = converted


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if pd.isna(n):
        return None
    return n


def _secondary_col(metric_id: str, suffix: str) -> str:
    return f"{metric_id}_{suffix}"


def load_jmh_json(path: Path) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError("JMH JSON должен быть непустым списком объектов")

    first = data[0]
    meta = {
        "jmhVersion": first.get("jmhVersion"),
        "jdkVersion": first.get("jdkVersion"),
        "vmName": first.get("vmName"),
        "vmVersion": first.get("vmVersion"),
        "jvm": first.get("jvm"),
        "threads": first.get("threads"),
        "forks": first.get("forks"),
    }

    rows: List[Dict[str, Any]] = []
    param_keys: set[str] = set()

    for r in data:
        params = r.get("params") or {}
        pm = r.get("primaryMetric") or {}
        secondary = r.get("secondaryMetrics") or {}

        score_conf = pm.get("scoreConfidence") or [None, None]
        if isinstance(score_conf, list) and len(score_conf) >= 2:
            ci_low, ci_high = score_conf[0], score_conf[1]
        else:
            ci_low, ci_high = None, None

        raw_flat = _flatten_raw_data(pm.get("rawData"))

        row: Dict[str, Any] = {
            "benchmark": r.get("benchmark", ""),
            "bench": _short_benchmark_name(r.get("benchmark", "")),
            "mode": r.get("mode", ""),
            "score": _safe_float(pm.get("score")),
            "scoreError": _safe_float(pm.get("scoreError")),
            "ciLow": ci_low,
            "ciHigh": ci_high,
            "unit": pm.get("scoreUnit"),
            "rawCount": len(raw_flat),
            "rawMin": min(raw_flat) if raw_flat else None,
            "rawMax": max(raw_flat) if raw_flat else None,
        }

        for metric in GC_METRICS:
            sm = secondary.get(metric["key"]) or {}
            score_conf = sm.get("scoreConfidence") or [None, None]
            if isinstance(score_conf, list) and len(score_conf) >= 2:
                m_ci_low, m_ci_high = score_conf[0], score_conf[1]
            else:
                m_ci_low, m_ci_high = None, None

            row[_secondary_col(metric["id"], "score")] = _safe_float(sm.get("score"))
            row[_secondary_col(metric["id"], "scoreError")] = _safe_float(sm.get("scoreError"))
            row[_secondary_col(metric["id"], "ciLow")] = _safe_float(m_ci_low)
            row[_secondary_col(metric["id"], "ciHigh")] = _safe_float(m_ci_high)
            row[_secondary_col(metric["id"], "unit")] = sm.get("scoreUnit")

        for k, v in params.items():
            param_keys.add(k)
            row[k] = _parse_param_value(v)

        rows.append(row)

    df = pd.DataFrame(rows)
    param_cols = sorted(param_keys)
    for p in param_cols:
        if p not in df.columns:
            df[p] = pd.NA

    numeric_cols = ["score", "scoreError", "ciLow", "ciHigh", "rawMin", "rawMax"]
    for metric in GC_METRICS:
        numeric_cols.extend(
            [
                _secondary_col(metric["id"], "score"),
                _secondary_col(metric["id"], "scoreError"),
                _secondary_col(metric["id"], "ciLow"),
                _secondary_col(metric["id"], "ciHigh"),
            ]
        )

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for p in param_cols:
        _try_convert_numeric_column(df, p)

    sort_cols = ["benchmark", "mode"]
    if "n" in param_cols:
        sort_cols.append("n")
    if "bucketCapacity" in param_cols:
        sort_cols.append("bucketCapacity")
    for p in param_cols:
        if p not in sort_cols:
            sort_cols.append(p)

    df = df.sort_values(sort_cols, na_position="last").reset_index(drop=True)
    return df, meta, param_cols


def _value_sort_key(v: Any) -> Tuple[int, Any]:
    if v is None:
        return (2, "")
    if isinstance(v, (int, float)):
        return (0, float(v))
    return (1, str(v))


def _varying_params(df: pd.DataFrame, param_cols: Sequence[str]) -> List[str]:
    out: List[str] = []
    for p in param_cols:
        if p not in df.columns:
            continue
        s = df[p].dropna()
        if s.empty:
            continue
        if s.astype(str).nunique() > 1:
            out.append(p)
    return out


def _pick_axis_params(varying: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
    if not varying:
        return None, None

    x_param = next((p for p in X_PARAM_PRIORITY if p in varying), varying[0])
    rest = [p for p in varying if p != x_param]
    if not rest:
        return x_param, None
    series_param = next((p for p in X_PARAM_PRIORITY if p in rest), rest[0])
    return x_param, series_param


def _should_log_scale(scores: pd.Series) -> bool:
    s = scores.dropna()
    if s.empty:
        return False
    mn = float(s.min())
    mx = float(s.max())
    if mn <= 0:
        return False
    return (mx / mn) >= 50.0


def _aggregate_for_plot(
    df: pd.DataFrame,
    keys: List[str],
    score_col: str,
    score_error_col: str,
    ci_low_col: str,
    ci_high_col: str,
) -> pd.DataFrame:
    if not keys:
        return pd.DataFrame(
            [
                {
                    "score": df[score_col].mean(),
                    "scoreError": df[score_error_col].mean(),
                    "ciLow": df[ci_low_col].mean(),
                    "ciHigh": df[ci_high_col].mean(),
                }
            ]
        )

    return (
        df.groupby(keys, dropna=False, as_index=False)
        .agg(
            score=(score_col, "mean"),
            scoreError=(score_error_col, "mean"),
            ciLow=(ci_low_col, "mean"),
            ciHigh=(ci_high_col, "mean"),
        )
    )


def make_metric_chart(
    df_group: pd.DataFrame,
    param_cols: Sequence[str],
    title: str,
    score_col: str,
    score_error_col: str,
    ci_low_col: str,
    ci_high_col: str,
    unit_col: str,
) -> Tuple[go.Figure, Dict[str, Any]]:
    if score_col not in df_group.columns:
        raise KeyError(f"Missing score column for chart: {score_col}")

    varying = _varying_params(df_group, param_cols)
    x_param, series_param = _pick_axis_params(varying)
    unit_values = df_group[unit_col].dropna().unique() if unit_col in df_group.columns else []
    unit_str = unit_values[0] if len(unit_values) else ""

    fig = go.Figure()
    info = {"xParam": x_param, "seriesParam": series_param, "varyingParams": varying}

    if x_param is None:
        agg = _aggregate_for_plot(df_group, [], score_col, score_error_col, ci_low_col, ci_high_col)
        fig.add_trace(
            go.Bar(
                x=["all"],
                y=agg["score"],
                error_y=dict(type="data", array=agg["scoreError"], visible=True),
                name="score",
            )
        )
    elif series_param is None:
        agg = _aggregate_for_plot(
            df_group,
            [x_param],
            score_col,
            score_error_col,
            ci_low_col,
            ci_high_col,
        ).sort_values(x_param, na_position="last")
        fig.add_trace(
            go.Scatter(
                x=agg[x_param],
                y=agg["score"],
                mode="lines+markers",
                name="score",
                error_y=dict(type="data", array=agg["scoreError"], visible=True),
            )
        )
    else:
        agg = _aggregate_for_plot(
            df_group,
            [x_param, series_param],
            score_col,
            score_error_col,
            ci_low_col,
            ci_high_col,
        )
        series_values = sorted(agg[series_param].dropna().unique(), key=_value_sort_key)

        for sval in series_values:
            sub = agg[agg[series_param] == sval].sort_values(x_param, na_position="last")
            name = f"{series_param}={sval}"

            fig.add_trace(
                go.Scatter(
                    x=sub[x_param],
                    y=sub["score"],
                    mode="lines+markers",
                    name=name,
                    error_y=dict(type="data", array=sub["scoreError"], visible=True),
                )
            )

            if sub["ciLow"].notna().all() and sub["ciHigh"].notna().all():
                x = list(sub[x_param]) + list(sub[x_param][::-1])
                y = list(sub["ciHigh"]) + list(sub["ciLow"][::-1])
                fig.add_trace(
                    go.Scatter(
                        x=x,
                        y=y,
                        fill="toself",
                        mode="lines",
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo="skip",
                        opacity=0.15,
                    )
                )

    fig.update_layout(
        title=title,
        xaxis_title=x_param if x_param else "all",
        yaxis_title=f"score ({unit_str})" if unit_str else "score",
        legend_title=series_param if series_param else "Series",
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40),
    )

    if _should_log_scale(df_group[score_col]):
        fig.update_yaxes(type="log")

    return fig, info


def make_generic_chart(
    df_group: pd.DataFrame,
    param_cols: Sequence[str],
    title: str,
) -> Tuple[go.Figure, Dict[str, Any]]:
    return make_metric_chart(
        df_group=df_group,
        param_cols=param_cols,
        title=title,
        score_col="score",
        score_error_col="scoreError",
        ci_low_col="ciLow",
        ci_high_col="ciHigh",
        unit_col="unit",
    )


MODE_PRIORITY = {"thrpt": 0, "avgt": 1, "sample": 2, "ss": 3}


def _mode_sort_key(mode: Any) -> Tuple[int, str]:
    key = str(mode)
    return MODE_PRIORITY.get(key, 99), key


def build_mode_collage_figure(
    gb: pd.DataFrame,
    param_cols: Sequence[str],
    title: str,
    score_col: str,
    score_error_col: str,
    ci_low_col: str,
    ci_high_col: str,
    unit_col: str,
    hide_legend: bool = True,
) -> Tuple[Optional[go.Figure], List[Any]]:
    modes = sorted(gb["mode"].dropna().unique(), key=_mode_sort_key)
    if not modes:
        return None, []

    cols = 1 if len(modes) == 1 else 2
    rows = (len(modes) + cols - 1) // cols
    subplot_titles = [f"mode={m}" for m in modes]

    collage = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.08,
        vertical_spacing=0.12,
    )

    for idx, mode in enumerate(modes):
        row = (idx // cols) + 1
        col = (idx % cols) + 1
        g_mode = gb[gb["mode"] == mode].copy()
        fig_mode, _ = make_metric_chart(
            g_mode,
            param_cols,
            f"{title} [{mode}]",
            score_col,
            score_error_col,
            ci_low_col,
            ci_high_col,
            unit_col,
        )

        for trace in fig_mode.data:
            trace.showlegend = not hide_legend
            collage.add_trace(trace, row=row, col=col)

        x_title = fig_mode.layout.xaxis.title.text if fig_mode.layout.xaxis.title else ""
        y_title = fig_mode.layout.yaxis.title.text if fig_mode.layout.yaxis.title else ""
        collage.update_xaxes(title_text=x_title, row=row, col=col)
        collage.update_yaxes(title_text=y_title, row=row, col=col)

        if fig_mode.layout.yaxis.type == "log":
            collage.update_yaxes(type="log", row=row, col=col)

    collage.update_layout(
        title=title,
        template="plotly_white",
        showlegend=not hide_legend,
        height=max(420, 380 * rows),
        margin=dict(l=40, r=20, t=70, b=40),
    )
    return collage, list(modes)


def build_memory_collage_figure(
    gb: pd.DataFrame,
    param_cols: Sequence[str],
    title: str,
    gc_metrics: Sequence[Dict[str, str]],
    hide_legend: bool = True,
) -> Tuple[Optional[go.Figure], List[Dict[str, str]], List[Any]]:
    valid_metrics: List[Dict[str, str]] = []
    for metric in gc_metrics:
        score_col = _secondary_col(metric["id"], "score")
        if score_col in gb.columns and gb[score_col].notna().any():
            valid_metrics.append(metric)

    modes = sorted(gb["mode"].dropna().unique(), key=_mode_sort_key)
    if not valid_metrics or not modes:
        return None, valid_metrics, list(modes)

    rows = len(valid_metrics)
    cols = len(modes)
    subplot_titles = [
        f"{metric['label']} | mode={mode}"
        for metric in valid_metrics
        for mode in modes
    ]

    collage = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.08,
        vertical_spacing=0.1,
    )

    for metric_index, metric in enumerate(valid_metrics):
        score_col = _secondary_col(metric["id"], "score")
        err_col = _secondary_col(metric["id"], "scoreError")
        ci_low_col = _secondary_col(metric["id"], "ciLow")
        ci_high_col = _secondary_col(metric["id"], "ciHigh")
        unit_col = _secondary_col(metric["id"], "unit")

        for mode_index, mode in enumerate(modes):
            row = metric_index + 1
            col = mode_index + 1
            g_mode = gb[gb["mode"] == mode].copy()
            fig_mode, _ = make_metric_chart(
                g_mode,
                param_cols,
                f"{title} :: {metric['label']} [{mode}]",
                score_col,
                err_col,
                ci_low_col,
                ci_high_col,
                unit_col,
            )

            for trace in fig_mode.data:
                trace.showlegend = not hide_legend
                collage.add_trace(trace, row=row, col=col)

            x_title = fig_mode.layout.xaxis.title.text if fig_mode.layout.xaxis.title else ""
            y_title = fig_mode.layout.yaxis.title.text if fig_mode.layout.yaxis.title else ""
            collage.update_xaxes(title_text=x_title, row=row, col=col)
            collage.update_yaxes(title_text=y_title, row=row, col=col)
            if fig_mode.layout.yaxis.type == "log":
                collage.update_yaxes(type="log", row=row, col=col)

    collage.update_layout(
        title=title,
        template="plotly_white",
        showlegend=not hide_legend,
        height=max(460, 320 * rows),
        margin=dict(l=40, r=20, t=90, b=40),
    )
    return collage, valid_metrics, list(modes)


def _available_gc_metrics(df: pd.DataFrame) -> List[Dict[str, str]]:
    available: List[Dict[str, str]] = []
    for metric in GC_METRICS:
        score_col = _secondary_col(metric["id"], "score")
        if score_col in df.columns and df[score_col].notna().any():
            available.append(metric)
    return available


def _format_numeric(val: Any) -> str:
    if pd.isna(val):
        return ""
    if isinstance(val, (int, float)):
        return f"{float(val):.3f}"
    return str(val)


def df_to_html_table(df: pd.DataFrame, title: str) -> str:
    fmt = df.copy()
    for col in fmt.columns:
        if pd.api.types.is_numeric_dtype(fmt[col]):
            fmt[col] = fmt[col].map(_format_numeric)

    html = fmt.to_html(
        index=False,
        escape=True,
        border=0,
        classes="table table-striped table-sm table-hover align-middle",
    )
    return f"<h4>{title}</h4>\n<div class='table-responsive'>{html}</div>"


def _md_cell(text: str) -> Dict[str, Any]:
    return {"cell_type": "markdown", "metadata": {}, "source": text}


def _code_cell(code: str) -> Dict[str, Any]:
    return {
        "cell_type": "code",
        "metadata": {
            "jupyter": {"source_hidden": True},
            "tags": ["hide-input"],
        },
        "execution_count": None,
        "outputs": [],
        "source": code,
    }


def build_notebook_report(
    df: pd.DataFrame,
    meta: Dict[str, Any],
    param_cols: Sequence[str],
    out_path: Path,
    source_json: Path,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    params_json = json.dumps(list(param_cols), ensure_ascii=False)
    x_priority_json = json.dumps(X_PARAM_PRIORITY, ensure_ascii=False)
    gc_metrics_json = json.dumps(GC_METRICS, ensure_ascii=False)
    source_abs_json = json.dumps(str(source_json.resolve()), ensure_ascii=False)
    source_rel = os.path.relpath(source_json.resolve(), out_path.parent.resolve())
    source_rel_json = json.dumps(source_rel.replace("\\", "/"), ensure_ascii=False)

    meta_lines = []
    for k in ("jmhVersion", "jdkVersion", "vmName", "vmVersion", "jvm", "threads", "forks"):
        v = meta.get(k)
        if v is not None:
            meta_lines.append(f"- **{k}**: {v}")

    init_code = f"""import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from IPython.display import display

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

json_path_candidates = [
    Path({source_rel_json}),
    Path({source_abs_json}),
]
json_path = next((p for p in json_path_candidates if p.exists()), None)
if json_path is None:
    raise FileNotFoundError(
        "JMH JSON not found. Checked: " + ", ".join(str(p) for p in json_path_candidates)
    )

raw = json.loads(json_path.read_text(encoding="utf-8"))
param_cols = {params_json}
X_PARAM_PRIORITY = {x_priority_json}
GC_METRICS = {gc_metrics_json}

def _parse_param_value(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return s
    return v

def _flatten_raw_data(raw_data):
    out = []
    def walk(x):
        if isinstance(x, (int, float)):
            out.append(float(x))
        elif isinstance(x, list):
            for item in x:
                walk(item)
    walk(raw_data)
    return out

def _try_convert_numeric_column(df, col):
    non_na = df[col].notna().sum()
    if non_na == 0:
        return
    converted = pd.to_numeric(df[col], errors="coerce")
    if converted.notna().sum() >= non_na:
        df[col] = converted

def _safe_float(v):
    if v is None:
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if pd.isna(n):
        return None
    return n

def _secondary_col(metric_id, suffix):
    return f"{{metric_id}}_{{suffix}}"

rows = []
param_keys = set()
for r in raw:
    params = r.get("params") or {{}}
    pm = r.get("primaryMetric") or {{}}
    secondary = r.get("secondaryMetrics") or {{}}

    score_conf = pm.get("scoreConfidence") or [None, None]
    if isinstance(score_conf, list) and len(score_conf) >= 2:
        ci_low, ci_high = score_conf[0], score_conf[1]
    else:
        ci_low, ci_high = None, None

    raw_flat = _flatten_raw_data(pm.get("rawData"))
    row = {{
        "benchmark": r.get("benchmark", ""),
        "bench": (r.get("benchmark", "").split(".")[-1] if r.get("benchmark") else ""),
        "mode": r.get("mode", ""),
        "score": _safe_float(pm.get("score")),
        "scoreError": _safe_float(pm.get("scoreError")),
        "ciLow": ci_low,
        "ciHigh": ci_high,
        "unit": pm.get("scoreUnit"),
        "rawCount": len(raw_flat),
        "rawMin": min(raw_flat) if raw_flat else None,
        "rawMax": max(raw_flat) if raw_flat else None,
    }}

    for metric in GC_METRICS:
        sm = secondary.get(metric["key"]) or {{}}
        sm_conf = sm.get("scoreConfidence") or [None, None]
        if isinstance(sm_conf, list) and len(sm_conf) >= 2:
            sm_ci_low, sm_ci_high = sm_conf[0], sm_conf[1]
        else:
            sm_ci_low, sm_ci_high = None, None

        row[_secondary_col(metric["id"], "score")] = _safe_float(sm.get("score"))
        row[_secondary_col(metric["id"], "scoreError")] = _safe_float(sm.get("scoreError"))
        row[_secondary_col(metric["id"], "ciLow")] = _safe_float(sm_ci_low)
        row[_secondary_col(metric["id"], "ciHigh")] = _safe_float(sm_ci_high)
        row[_secondary_col(metric["id"], "unit")] = sm.get("scoreUnit")

    for k, v in params.items():
        param_keys.add(k)
        row[k] = _parse_param_value(v)

    rows.append(row)

df = pd.DataFrame(rows)
param_cols = sorted(set(param_cols) | param_keys)
for p in param_cols:
    if p not in df.columns:
        df[p] = pd.NA

numeric_cols = ["score", "scoreError", "ciLow", "ciHigh", "rawMin", "rawMax"]
for metric in GC_METRICS:
    numeric_cols.extend([
        _secondary_col(metric["id"], "score"),
        _secondary_col(metric["id"], "scoreError"),
        _secondary_col(metric["id"], "ciLow"),
        _secondary_col(metric["id"], "ciHigh"),
    ])

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

for p in param_cols:
    _try_convert_numeric_column(df, p)

def _should_log_scale(series):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return False
    mn, mx = float(s.min()), float(s.max())
    return mn > 0 and (mx / mn) >= 50.0

def _varying_params(g):
    out = []
    for p in param_cols:
        if p not in g.columns:
            continue
        s = g[p].dropna()
        if s.empty:
            continue
        if s.astype(str).nunique() > 1:
            out.append(p)
    return out

def _pick_axes(varying):
    if not varying:
        return None, None
    x_param = next((p for p in X_PARAM_PRIORITY if p in varying), varying[0])
    rest = [p for p in varying if p != x_param]
    if not rest:
        return x_param, None
    series_param = next((p for p in X_PARAM_PRIORITY if p in rest), rest[0])
    return x_param, series_param

def _aggregate(g, keys, score_col, score_error_col, ci_low_col, ci_high_col):
    if not keys:
        return pd.DataFrame([{{"score": g[score_col].mean(), "scoreError": g[score_error_col].mean(), "ciLow": g[ci_low_col].mean(), "ciHigh": g[ci_high_col].mean()}}])
    return g.groupby(keys, dropna=False, as_index=False).agg(
        score=(score_col, "mean"),
        scoreError=(score_error_col, "mean"),
        ciLow=(ci_low_col, "mean"),
        ciHigh=(ci_high_col, "mean"),
    )

def build_chart_metric(g, title, score_col, score_error_col, ci_low_col, ci_high_col, unit_col):
    varying = _varying_params(g)
    x_param, series_param = _pick_axes(varying)
    unit_values = g[unit_col].dropna().unique() if unit_col in g.columns else []
    unit = unit_values[0] if len(unit_values) else ""
    fig = go.Figure()

    if x_param is None:
        agg = _aggregate(g, [], score_col, score_error_col, ci_low_col, ci_high_col)
        fig.add_trace(go.Bar(
            x=["all"], y=agg["score"],
            error_y=dict(type="data", array=agg["scoreError"], visible=True),
            name="score"
        ))
    elif series_param is None:
        agg = _aggregate(g, [x_param], score_col, score_error_col, ci_low_col, ci_high_col).sort_values(x_param, na_position="last")
        fig.add_trace(go.Scatter(
            x=agg[x_param], y=agg["score"], mode="lines+markers", name="score",
            error_y=dict(type="data", array=agg["scoreError"], visible=True),
        ))
    else:
        agg = _aggregate(g, [x_param, series_param], score_col, score_error_col, ci_low_col, ci_high_col)
        for sval in sorted(agg[series_param].dropna().unique(), key=lambda v: (2, "") if v is None else ((0, float(v)) if isinstance(v, (int, float)) else (1, str(v)))):
            sub = agg[agg[series_param] == sval].sort_values(x_param, na_position="last")
            fig.add_trace(go.Scatter(
                x=sub[x_param], y=sub["score"], mode="lines+markers",
                name=f"{{series_param}}={{sval}}",
                error_y=dict(type="data", array=sub["scoreError"], visible=True),
            ))
            if sub["ciLow"].notna().all() and sub["ciHigh"].notna().all():
                x = list(sub[x_param]) + list(sub[x_param][::-1])
                y = list(sub["ciHigh"]) + list(sub["ciLow"][::-1])
                fig.add_trace(go.Scatter(
                    x=x, y=y, fill="toself", mode="lines",
                    line=dict(width=0), showlegend=False, hoverinfo="skip", opacity=0.15
                ))

    fig.update_layout(
        title=title,
        xaxis_title=x_param if x_param else "all",
        yaxis_title=f"score ({{unit}})" if unit else "score",
        legend_title=series_param if series_param else "Series",
        template="plotly_white",
    )
    if _should_log_scale(g[score_col]):
        fig.update_yaxes(type="log")
    return fig, x_param, series_param, varying

def build_chart(g, title):
    return build_chart_metric(
        g,
        title,
        "score",
        "scoreError",
        "ciLow",
        "ciHigh",
        "unit",
    )

MODE_PRIORITY = {{"thrpt": 0, "avgt": 1, "sample": 2, "ss": 3}}

def _mode_sort_key(mode):
    key = str(mode)
    return (MODE_PRIORITY.get(key, 99), key)

def build_mode_collage(gb, title, score_col, score_error_col, ci_low_col, ci_high_col, unit_col):
    modes = sorted(gb["mode"].dropna().unique(), key=_mode_sort_key)
    if not modes:
        return None, []

    cols = 1 if len(modes) == 1 else 2
    rows = (len(modes) + cols - 1) // cols
    subplot_titles = [f"mode={{m}}" for m in modes]

    collage = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.08,
        vertical_spacing=0.12,
    )

    for idx, mode in enumerate(modes):
        row = (idx // cols) + 1
        col = (idx % cols) + 1
        g_mode = gb[gb["mode"] == mode].copy()
        fig_mode, _, _, _ = build_chart_metric(
            g_mode,
            f"{{title}} [{{mode}}]",
            score_col,
            score_error_col,
            ci_low_col,
            ci_high_col,
            unit_col,
        )

        for trace in fig_mode.data:
            trace.showlegend = False
            collage.add_trace(trace, row=row, col=col)

        x_title = fig_mode.layout.xaxis.title.text if fig_mode.layout.xaxis.title else ""
        y_title = fig_mode.layout.yaxis.title.text if fig_mode.layout.yaxis.title else ""
        collage.update_xaxes(title_text=x_title, row=row, col=col)
        collage.update_yaxes(title_text=y_title, row=row, col=col)
        if fig_mode.layout.yaxis.type == "log":
            collage.update_yaxes(type="log", row=row, col=col)

    collage.update_layout(
        title=title,
        template="plotly_white",
        showlegend=False,
        height=max(420, 380 * rows),
        margin=dict(l=40, r=20, t=70, b=40),
    )

    return collage, modes

def build_memory_collage(gb, title, gc_metrics):
    valid_metrics = []
    for metric in gc_metrics:
        score_col = _secondary_col(metric["id"], "score")
        if score_col in gb.columns and gb[score_col].notna().any():
            valid_metrics.append(metric)

    modes = sorted(gb["mode"].dropna().unique(), key=_mode_sort_key)
    if not valid_metrics or not modes:
        return None, valid_metrics, modes

    rows = len(valid_metrics)
    cols = len(modes)
    subplot_titles = [
        f"{{metric['label']}} | mode={{mode}}"
        for metric in valid_metrics
        for mode in modes
    ]

    collage = make_subplots(
        rows=rows,
        cols=cols,
        subplot_titles=subplot_titles,
        horizontal_spacing=0.08,
        vertical_spacing=0.1,
    )

    for metric_index, metric in enumerate(valid_metrics):
        score_col = _secondary_col(metric["id"], "score")
        err_col = _secondary_col(metric["id"], "scoreError")
        ci_low_col = _secondary_col(metric["id"], "ciLow")
        ci_high_col = _secondary_col(metric["id"], "ciHigh")
        unit_col = _secondary_col(metric["id"], "unit")

        for mode_index, mode in enumerate(modes):
            row = metric_index + 1
            col = mode_index + 1
            g_mode = gb[gb["mode"] == mode].copy()
            fig_mode, _, _, _ = build_chart_metric(
                g_mode,
                f"{{title}} :: {{metric['label']}} [{{mode}}]",
                score_col,
                err_col,
                ci_low_col,
                ci_high_col,
                unit_col,
            )

            for trace in fig_mode.data:
                trace.showlegend = False
                collage.add_trace(trace, row=row, col=col)

            x_title = fig_mode.layout.xaxis.title.text if fig_mode.layout.xaxis.title else ""
            y_title = fig_mode.layout.yaxis.title.text if fig_mode.layout.yaxis.title else ""
            collage.update_xaxes(title_text=x_title, row=row, col=col)
            collage.update_yaxes(title_text=y_title, row=row, col=col)
            if fig_mode.layout.yaxis.type == "log":
                collage.update_yaxes(type="log", row=row, col=col)

    collage.update_layout(
        title=title,
        template="plotly_white",
        showlegend=False,
        height=max(460, 320 * rows),
        margin=dict(l=40, r=20, t=90, b=40),
    )

    return collage, valid_metrics, modes
"""

    gc_metrics_available = _available_gc_metrics(df)
    gc_summary_cols: List[str] = []
    for metric in gc_metrics_available:
        gc_summary_cols.extend(
            [
                _secondary_col(metric["id"], "score"),
                _secondary_col(metric["id"], "unit"),
            ]
        )

    summary_cols = [
        "benchmark",
        "mode",
        *[p for p in param_cols if p in df.columns],
        "score",
        "scoreError",
        "ciLow",
        "ciHigh",
        "unit",
        "rawCount",
        "rawMin",
        "rawMax",
        *gc_summary_cols,
    ]

    cells: List[Dict[str, Any]] = [
        _md_cell(
            "# JMH Notebook Report\n\n"
            f"Сгенерировано: **{now}**\n\n"
            f"Источник JSON: `{source_json}`"
        ),
        _md_cell("## Метаданные запуска\n" + "\n".join(meta_lines)),
        _code_cell(init_code),
        _md_cell("## Сводная таблица"),
        _code_cell(
            "summary_cols = " + repr(summary_cols) + "\n"
            "summary_df = df[summary_cols].sort_values(['benchmark', 'mode']).reset_index(drop=True)\n"
            "display(summary_df)"
        ),
    ]

    benchmarks = (
        df[["benchmark"]]
        .drop_duplicates()
        .sort_values(["benchmark"], na_position="last")
        .itertuples(index=False, name=None)
    )

    current_algorithm: Optional[str] = None
    for (benchmark,) in benchmarks:
        algorithm = _algorithm_name(benchmark)
        if algorithm != current_algorithm:
            current_algorithm = algorithm
            cells.append(_md_cell(f"## Algorithm: {_algorithm_heading(algorithm)}"))

        b_lit = json.dumps(benchmark, ensure_ascii=False)
        primary_title_lit = json.dumps(f"{benchmark} :: primary score by mode", ensure_ascii=False)

        cells.append(_md_cell(f"### {benchmark}\n**Коллаж по mode**"))
        cells.append(
            _code_cell(
                f"gb = df[df['benchmark'] == {b_lit}].copy()\n"
                f"fig, modes = build_mode_collage(gb, {primary_title_lit}, 'score', 'scoreError', 'ciLow', 'ciHigh', 'unit')\n"
                "if fig is not None:\n"
                "    fig.show()\n"
                "else:\n"
                "    print('No primary metric data')\n"
                "table_cols = ['benchmark', 'mode'] + [c for c in param_cols if c in gb.columns] + ['score', 'scoreError', 'ciLow', 'ciHigh', 'unit']\n"
                "display(gb[table_cols].sort_values(['mode'] + [c for c in param_cols if c in gb.columns], na_position='last').reset_index(drop=True))"
            )
        )
        gc_metrics_lit = json.dumps(gc_metrics_available, ensure_ascii=False)
        memory_title_lit = json.dumps(f"{benchmark} :: memory metrics by mode", ensure_ascii=False)
        cells.append(_md_cell("#### Memory (single collage: metrics x mode)"))
        cells.append(
            _code_cell(
                f"gb = df[df['benchmark'] == {b_lit}].copy()\n"
                f"gc_metrics_local = {gc_metrics_lit}\n"
                f"fig_mem, used_metrics, modes = build_memory_collage(gb, {memory_title_lit}, gc_metrics_local)\n"
                "if fig_mem is not None:\n"
                "    fig_mem.show()\n"
                "    mem_cols = ['benchmark', 'mode'] + [c for c in param_cols if c in gb.columns]\n"
                "    for metric in used_metrics:\n"
                "        mem_cols.extend([\n"
                "            _secondary_col(metric['id'], 'score'),\n"
                "            _secondary_col(metric['id'], 'scoreError'),\n"
                "            _secondary_col(metric['id'], 'unit'),\n"
                "        ])\n"
                "    seen = set()\n"
                "    mem_cols = [c for c in mem_cols if not (c in seen or seen.add(c))]\n"
                "    display(gb[mem_cols].sort_values(['mode'] + [c for c in param_cols if c in gb.columns], na_position='last').reset_index(drop=True))\n"
                "else:\n"
                "    print('No GC memory metrics in this benchmark block')"
            )
        )
        cells.append(
            _md_cell(
                "#### Заметки\n"
                "- Основной вывод:\n"
                "- Отличия между mode:\n"
                "- Наблюдения по памяти:"
            )
        )

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "file_extension": ".py"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    out_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")


def build_html_report(
    df: pd.DataFrame,
    meta: Dict[str, Any],
    param_cols: Sequence[str],
    out_path: Path,
    use_cdn: bool,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    meta_lines = []
    for k in ("jmhVersion", "jdkVersion", "vmName", "vmVersion", "jvm", "threads", "forks"):
        v = meta.get(k)
        if v is not None:
            meta_lines.append(f"<li><b>{k}</b>: {v}</li>")

    gc_metrics_available = _available_gc_metrics(df)
    gc_summary_cols: List[str] = []
    for metric in gc_metrics_available:
        gc_summary_cols.extend(
            [
                _secondary_col(metric["id"], "score"),
                _secondary_col(metric["id"], "unit"),
            ]
        )

    summary_cols = [
        "benchmark",
        "mode",
        *[p for p in param_cols if p in df.columns],
        "score",
        "scoreError",
        "ciLow",
        "ciHigh",
        "unit",
        "rawCount",
        "rawMin",
        "rawMax",
        *gc_summary_cols,
    ]

    sections: List[str] = []
    sections.append("<h2>Сводная таблица</h2>")
    sections.append(df_to_html_table(df[summary_cols], "Все измерения"))
    sections.append("<h2>Графики по benchmark + mode</h2>")

    include_plotlyjs_flag = "cdn" if use_cdn else True
    plotly_included = False

    current_algorithm: Optional[str] = None
    for (benchmark, mode), g in df.groupby(["benchmark", "mode"], dropna=False, sort=True):
        algorithm = _algorithm_name(benchmark)
        if algorithm != current_algorithm:
            current_algorithm = algorithm
            sections.append(f"<hr><h2>Algorithm: {_algorithm_heading(algorithm)}</h2>")

        title = f"{benchmark} [{mode}]"
        fig, info = make_generic_chart(g, param_cols, title=title)
        fig_html = fig.to_html(
            include_plotlyjs=include_plotlyjs_flag if not plotly_included else False,
            full_html=False,
        )
        plotly_included = True

        table_cols = [
            "benchmark",
            "mode",
            *[p for p in param_cols if p in g.columns],
            "score",
            "scoreError",
            "ciLow",
            "ciHigh",
            "unit",
        ]

        sections.append(f"<hr><h3>{title}</h3>")
        sections.append(
            "<p class='text-muted'>"
            f"x = <b>{info['xParam']}</b>, series = <b>{info['seriesParam']}</b>, "
            f"varying params = <b>{', '.join(info['varyingParams']) if info['varyingParams'] else '(none)'}</b>"
            "</p>"
        )
        sections.append(fig_html)
        sections.append(df_to_html_table(g[table_cols], "Данные группы"))

        for metric in gc_metrics_available:
            score_col = _secondary_col(metric["id"], "score")
            if score_col not in g.columns or not g[score_col].notna().any():
                continue

            err_col = _secondary_col(metric["id"], "scoreError")
            ci_low_col = _secondary_col(metric["id"], "ciLow")
            ci_high_col = _secondary_col(metric["id"], "ciHigh")
            unit_col = _secondary_col(metric["id"], "unit")
            metric_title = f"{title} :: {metric['label']}"
            fig_mem, info_mem = make_metric_chart(
                g,
                param_cols,
                title=metric_title,
                score_col=score_col,
                score_error_col=err_col,
                ci_low_col=ci_low_col,
                ci_high_col=ci_high_col,
                unit_col=unit_col,
            )
            fig_mem_html = fig_mem.to_html(
                include_plotlyjs=False,
                full_html=False,
            )

            mem_table_cols = [
                "benchmark",
                "mode",
                *[p for p in param_cols if p in g.columns],
                score_col,
                err_col,
                unit_col,
            ]

            sections.append(f"<h4>Memory Metric: {metric['label']}</h4>")
            sections.append(
                "<p class='text-muted'>"
                f"x = <b>{info_mem['xParam']}</b>, series = <b>{info_mem['seriesParam']}</b>, "
                f"varying params = <b>{', '.join(info_mem['varyingParams']) if info_mem['varyingParams'] else '(none)'}</b>"
                "</p>"
            )
            sections.append(fig_mem_html)
            sections.append(df_to_html_table(g[mem_table_cols], f"Данные по {metric['label']}"))

    bootstrap_css = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    html_parts = [
        "<!doctype html>",
        "<html lang='ru'>",
        "<head>",
        "  <meta charset='utf-8'>",
        "  <meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"  <link rel='stylesheet' href='{bootstrap_css}'>",
        "  <title>JMH Report</title>",
        "  <style>",
        "    body { padding: 24px; }",
        "    .container-max { max-width: 1280px; }",
        "    h1, h2, h3 { margin-top: 1.2rem; }",
        "  </style>",
        "</head>",
        "<body>",
        "<div class='container container-max'>",
        "  <h1>Отчёт по JMH-бенчмаркам</h1>",
        f"  <p class='text-muted'>Сгенерировано: {now}</p>",
        "  <h2>Метаданные запуска</h2>",
        f"  <ul>{''.join(meta_lines)}</ul>",
        (
            "  <p class='text-muted'>"
            "Примечание: если для графика включена log-шкала по Y, "
            "значит разброс score очень большой."
            "</p>"
        ),
        *sections,
        "</div>",
        "</body>",
        "</html>",
    ]

    out_path.write_text("\n".join(html_parts), encoding="utf-8")


def _slugify(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_")
    return slug or "plot"


def _write_figure(
    fig: go.Figure,
    output_path: Path,
    plots_format: str,
    use_cdn: bool,
) -> None:
    if plots_format == "html":
        fig.write_html(
            str(output_path),
            include_plotlyjs="cdn" if use_cdn else True,
            full_html=True,
        )
        return

    if plots_format == "png":
        try:
            fig.write_image(str(output_path), format="png", scale=2)
        except Exception as e:
            raise SystemExit(
                "PNG export requires Plotly static image engine.\n"
                "Install dependency: pip install kaleido"
            ) from e
        return

    raise ValueError(f"Unsupported plots format: {plots_format}")


def build_markdown_report(
    df: pd.DataFrame,
    meta: Dict[str, Any],
    param_cols: Sequence[str],
    out_path: Path,
    plots_dir: Path,
    use_cdn: bool,
    plots_format: str = "html",
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rel_plots_dir = os.path.relpath(plots_dir.resolve(), out_path.parent.resolve()).replace("\\", "/")

    lines: List[str] = []
    lines.append("# JMH Markdown Report")
    lines.append("")
    lines.append(f"- Generated: `{now}`")
    lines.append(f"- Plots directory: `{rel_plots_dir}`")
    lines.append(f"- Plots format: `{plots_format}`")
    lines.append("")
    lines.append("## Metadata")
    lines.append("")
    for k in ("jmhVersion", "jdkVersion", "vmName", "vmVersion", "jvm", "threads", "forks"):
        v = meta.get(k)
        if v is not None:
            lines.append(f"- `{k}`: {v}")

    lines.append("")
    lines.append("## Summary")
    lines.append("")
    summary_cols = [
        "benchmark",
        "mode",
        *[p for p in param_cols if p in df.columns],
        "score",
        "scoreError",
        "unit",
    ]
    summary_csv = plots_dir / "summary.csv"
    df[summary_cols].to_csv(summary_csv, index=False)
    lines.append(f"- Summary data CSV: [{summary_csv.name}]({rel_plots_dir}/{summary_csv.name})")

    gc_metrics_available = _available_gc_metrics(df)

    benchmarks = (
        df[["benchmark"]]
        .drop_duplicates()
        .sort_values(["benchmark"], na_position="last")
        .itertuples(index=False, name=None)
    )

    current_algorithm: Optional[str] = None
    for (benchmark,) in benchmarks:
        algorithm = _algorithm_name(benchmark)
        if algorithm != current_algorithm:
            current_algorithm = algorithm
            lines.append("")
            lines.append(f"## Algorithm: {_algorithm_heading(algorithm)}")

        gb = df[df["benchmark"] == benchmark].copy()
        bench_slug = _slugify(benchmark)
        lines.append("")
        lines.append(f"### {benchmark}")

        primary_fig, primary_modes = build_mode_collage_figure(
            gb=gb,
            param_cols=param_cols,
            title=f"{benchmark} :: primary score by mode",
            score_col="score",
            score_error_col="scoreError",
            ci_low_col="ciLow",
            ci_high_col="ciHigh",
            unit_col="unit",
            hide_legend=True,
        )
        if primary_fig is not None:
            primary_ext = "html" if plots_format == "html" else "png"
            primary_file = plots_dir / f"{bench_slug}__primary.{primary_ext}"
            _write_figure(primary_fig, primary_file, plots_format, use_cdn)
            if plots_format == "png":
                lines.append(f"- Primary collage image: ![{primary_file.name}]({rel_plots_dir}/{primary_file.name})")
            else:
                lines.append(f"- Primary collage: [{primary_file.name}]({rel_plots_dir}/{primary_file.name})")
            lines.append(f"- Modes: `{', '.join(str(m) for m in primary_modes)}`")
        else:
            lines.append("- Primary collage: no data")

        memory_fig, used_metrics, mem_modes = build_memory_collage_figure(
            gb=gb,
            param_cols=param_cols,
            title=f"{benchmark} :: memory metrics by mode",
            gc_metrics=gc_metrics_available,
            hide_legend=True,
        )
        if memory_fig is not None:
            memory_ext = "html" if plots_format == "html" else "png"
            memory_file = plots_dir / f"{bench_slug}__memory.{memory_ext}"
            _write_figure(memory_fig, memory_file, plots_format, use_cdn)
            metric_labels = ", ".join(metric["label"] for metric in used_metrics)
            if plots_format == "png":
                lines.append(f"- Memory collage image: ![{memory_file.name}]({rel_plots_dir}/{memory_file.name})")
            else:
                lines.append(f"- Memory collage: [{memory_file.name}]({rel_plots_dir}/{memory_file.name})")
            lines.append(f"- Memory metrics: `{metric_labels}`")
            lines.append(f"- Memory modes: `{', '.join(str(m) for m in mem_modes)}`")
        else:
            lines.append("- Memory collage: no GC metrics in this benchmark block")

        bench_csv = plots_dir / f"{bench_slug}__data.csv"
        gb.to_csv(bench_csv, index=False)
        lines.append(f"- Raw benchmark data: [{bench_csv.name}]({rel_plots_dir}/{bench_csv.name})")
        lines.append("")
        lines.append("#### Notes")
        lines.append("- Основной вывод:")
        lines.append("- Отличия между mode:")
        lines.append("- Наблюдения по памяти:")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def execute_notebook_inplace(notebook_path: Path, timeout: int) -> None:
    jupyter = shutil.which("jupyter")
    if not jupyter:
        raise SystemExit(
            "Cannot execute notebook automatically: 'jupyter' command not found.\n"
            "Install it (e.g. 'pip install jupyter nbconvert')."
        )

    cmd = [
        jupyter,
        "nbconvert",
        "--to",
        "notebook",
        "--execute",
        "--inplace",
        str(notebook_path),
        f"--ExecutePreprocessor.timeout={timeout}",
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate HTML / Notebook / Markdown reports from JMH JSON results."
    )
    parser.add_argument(
        "json_path",
        nargs="?",
        type=Path,
        default=Path("hw01/target/jmh-all.json"),
        help="Path to JMH result JSON (default: hw01/target/jmh-all.json)",
    )
    parser.add_argument(
        "--format",
        choices=("html", "notebook", "both", "markdown", "all"),
        default="notebook",
        help="Output format (default: notebook)",
    )
    parser.add_argument("--out", type=Path, default=Path("hw01/target/jmh-report.html"), help="HTML output path")
    parser.add_argument(
        "--notebook-out",
        type=Path,
        default=Path("hw01/target/jmh-report.ipynb"),
        help="Notebook output path",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        default=Path("hw01/target/jmh-report.md"),
        help="Markdown output path",
    )
    parser.add_argument(
        "--plots-dir",
        type=Path,
        default=Path("hw01/target/jmh-plots"),
        help="Directory for plot files used by markdown report",
    )
    parser.add_argument(
        "--plots-format",
        choices=("html", "png"),
        default="html",
        help="Plot file format used by markdown report (default: html)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Embed plotly.js into HTML (bigger file, works offline)",
    )
    args = parser.parse_args()

    if not args.json_path.exists():
        nearby = sorted(args.json_path.parent.glob("*.json"))
        lines = [f"JMH JSON not found: {args.json_path}"]
        if nearby:
            lines.append("Available JSON files in the same directory:")
            for p in nearby:
                lines.append(f"  - {p}")
        else:
            lines.append(f"No JSON files found in: {args.json_path.parent}")
        lines.append("Hint: run JMH with '-rf json -rff <path>.json' first.")
        raise SystemExit("\n".join(lines))

    df, meta, param_cols = load_jmh_json(args.json_path)
    generated: List[Path] = []

    if args.format in ("html", "both", "all"):
        build_html_report(df, meta, param_cols, args.out, use_cdn=not args.offline)
        generated.append(args.out)

    if args.format in ("notebook", "both", "all"):
        build_notebook_report(df, meta, param_cols, args.notebook_out, args.json_path)
        generated.append(args.notebook_out)
        execute_notebook_inplace(args.notebook_out, timeout=-1)
        print(f"OK: executed notebook in-place: {args.notebook_out.resolve()}")

    if args.format in ("markdown", "all"):
        build_markdown_report(
            df=df,
            meta=meta,
            param_cols=param_cols,
            out_path=args.markdown_out,
            plots_dir=args.plots_dir,
            use_cdn=not args.offline,
            plots_format=args.plots_format,
        )
        generated.append(args.markdown_out)

    for p in generated:
        print(f"OK: {p.resolve()}")


if __name__ == "__main__":
    main()
