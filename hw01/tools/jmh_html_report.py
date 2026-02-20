# python tools/jmh_html_report.py target/jmh-result.json --out target/jmh-report.html
import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import plotly.graph_objects as go


def _short_benchmark_name(full_name: str) -> str:
    # bench.Hw01Benchmark.getHit -> getHit
    return full_name.split(".")[-1]


def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _flatten_raw_data(raw_data: Any) -> List[float]:
    # JMH JSON обычно: rawData = [ [ [iter...], [iter...], ... ] ]  (forks x iterations)
    # или иногда: rawData = [ [iter...], [iter...] ]
    out: List[float] = []

    def walk(v: Any) -> None:
        if isinstance(v, (int, float)):
            out.append(float(v))
        elif isinstance(v, list):
            for item in v:
                walk(item)

    walk(raw_data)
    return out


def load_jmh_json(path: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError("JMH JSON должен быть непустым списком объектов")

    # Метаданные возьмём из первого элемента (обычно одинаковые для всего файла)
    first = data[0]
    meta = {
        "jmhVersion": first.get("jmhVersion"),
        "jdkVersion": first.get("jdkVersion"),
        "vmName": first.get("vmName"),
        "vmVersion": first.get("vmVersion"),
        "jvm": first.get("jvm"),
        "threads": first.get("threads"),
        "forks": first.get("forks"),
        "mode": first.get("mode"),
    }

    rows: List[Dict[str, Any]] = []
    for r in data:
        params = r.get("params") or {}
        pm = r.get("primaryMetric") or {}

        score_conf = pm.get("scoreConfidence") or [None, None]
        if isinstance(score_conf, list) and len(score_conf) >= 2:
            ci_low, ci_high = score_conf[0], score_conf[1]
        else:
            ci_low, ci_high = None, None

        raw_data = pm.get("rawData")
        raw_flat = _flatten_raw_data(raw_data)

        row = {
            "benchmark": r.get("benchmark", ""),
            "bench": _short_benchmark_name(r.get("benchmark", "")),
            "bucketCapacity": _safe_int(params.get("bucketCapacity")),
            "n": _safe_int(params.get("n")),
            "seed": _safe_int(params.get("seed")),
            "score": pm.get("score"),
            "scoreError": pm.get("scoreError"),
            "ciLow": ci_low,
            "ciHigh": ci_high,
            "unit": pm.get("scoreUnit"),
            "rawCount": len(raw_flat),
            "rawMin": min(raw_flat) if raw_flat else None,
            "rawMax": max(raw_flat) if raw_flat else None,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Приведём типы и отсортируем для красоты
    for col in ("bucketCapacity", "n", "seed"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("score", "scoreError", "ciLow", "ciHigh", "rawMin", "rawMax"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["bench", "n", "bucketCapacity"], na_position="last").reset_index(drop=True)
    return df, meta


def _should_log_scale(scores: pd.Series) -> bool:
    s = scores.dropna()
    if s.empty:
        return False
    mn = float(s.min())
    mx = float(s.max())
    if mn <= 0:
        return False
    # если разброс больше 50x — логарифм сильно улучшает читаемость
    return (mx / mn) >= 50.0


def make_line_chart(df_bench: pd.DataFrame, title: str) -> go.Figure:
    # Линии: X=bucketCapacity, серии по n, Y=score
    fig = go.Figure()

    unit = df_bench["unit"].dropna().unique()
    unit_str = unit[0] if len(unit) else ""

    # Если n отсутствует (вдруг), делаем одну серию
    if df_bench["n"].isna().all():
        groups = [(None, df_bench)]
    else:
        groups = list(df_bench.groupby("n", dropna=False))

    for n_val, g in groups:
        g = g.sort_values("bucketCapacity")
        name = f"n={int(n_val)}" if pd.notna(n_val) else "n=?"

        # основная линия
        fig.add_trace(
            go.Scatter(
                x=g["bucketCapacity"],
                y=g["score"],
                mode="lines+markers",
                name=name,
                error_y=dict(type="data", array=g["scoreError"], visible=True),
                hovertemplate=(
                    "bucketCapacity=%{x}<br>"
                    f"{name}<br>"
                    "score=%{y:.3f}<br>"
                    "±err=%{error_y.array:.3f}<extra></extra>"
                ),
            )
        )

        # CI-band (если есть ciLow/ciHigh)
        if g["ciLow"].notna().all() and g["ciHigh"].notna().all():
            x = list(g["bucketCapacity"]) + list(g["bucketCapacity"][::-1])
            y = list(g["ciHigh"]) + list(g["ciLow"][::-1])
            fig.add_trace(
                go.Scatter(
                    x=x,
                    y=y,
                    fill="toself",
                    mode="lines",
                    line=dict(width=0),
                    name=f"{name} CI",
                    showlegend=False,
                    hoverinfo="skip",
                    opacity=0.15,
                )
            )

    fig.update_layout(
        title=title,
        xaxis_title="bucketCapacity",
        yaxis_title=f"score ({unit_str})" if unit_str else "score",
        legend_title="Series",
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40),
    )

    # авто log-scale для “тяжёлых” бенчей
    if _should_log_scale(df_bench["score"]):
        fig.update_yaxes(type="log")

    return fig


def make_pivot_table(df_bench: pd.DataFrame) -> pd.DataFrame:
    # pivot: index=bucketCapacity, columns=n, values=score
    pivot = df_bench.pivot_table(
        index="bucketCapacity",
        columns="n",
        values="score",
        aggfunc="mean",
    )
    # упорядочим
    pivot = pivot.sort_index()
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)
    return pivot


def df_to_html_table(df: pd.DataFrame, title: str) -> str:
    # Небольшое форматирование чисел
    df_fmt = df.copy()
    for col in ("score", "scoreError", "ciLow", "ciHigh", "rawMin", "rawMax"):
        if col in df_fmt.columns:
            df_fmt[col] = df_fmt[col].map(lambda x: f"{x:.3f}" if pd.notna(x) else "")

    html = df_fmt.to_html(
        index=False,
        escape=True,
        border=0,
        classes="table table-striped table-sm table-hover align-middle",
    )
    return f"<h3>{title}</h3>\n<div class='table-responsive'>{html}</div>"


def pivot_to_html_table(pivot: pd.DataFrame, title: str) -> str:
    pivot_fmt = pivot.copy()
    pivot_fmt = pivot_fmt.map(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    html = pivot_fmt.to_html(
        escape=True,
        border=0,
        classes="table table-bordered table-sm table-hover align-middle text-center",
    )
    return f"<h4>{title}</h4>\n<div class='table-responsive'>{html}</div>"


def build_report(df: pd.DataFrame, meta: Dict[str, Any], out_path: Path, use_cdn: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Сводная таблица
    summary_cols = [
        "bench", "bucketCapacity", "n", "seed",
        "score", "scoreError", "ciLow", "ciHigh", "unit",
        "rawCount", "rawMin", "rawMax",
    ]
    summary = df[summary_cols].copy()

    # HTML head + стили
    plotly_js = "https://cdn.plot.ly/plotly-2.27.0.min.js" if use_cdn else None
    bootstrap_css = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"

    meta_lines = []
    for k in ("jmhVersion", "jdkVersion", "vmName", "vmVersion", "jvm", "threads", "forks", "mode"):
        v = meta.get(k)
        if v is not None:
            meta_lines.append(f"<li><b>{k}</b>: {v}</li>")

    sections: List[str] = []
    sections.append("<h2>Сводная таблица результатов</h2>")
    sections.append(df_to_html_table(summary, "Все измерения (score / error / CI)"))

    # По каждому бенчу: график + pivot
    sections.append("<h2>Детализация по каждому бенчмарку</h2>")

    include_plotlyjs_flag = "cdn" if use_cdn else True
    plotly_included = False

    for bench, g in df.groupby("bench"):
        fig = make_line_chart(g, title=f"{bench}: score vs bucketCapacity (серии по n)")
        fig_html = fig.to_html(
            include_plotlyjs=include_plotlyjs_flag if not plotly_included else False,
            full_html=False,
        )
        plotly_included = True

        pivot = make_pivot_table(g)
        pivot_html = pivot_to_html_table(pivot, title="Pivot: bucketCapacity × n (score)")

        # компактная таблица по этому бенчу
        local = g[summary_cols].copy()
        local_html = df_to_html_table(local, title=f"Таблица: {bench}")

        sections.append(f"<hr><h3>{bench}</h3>")
        sections.append(fig_html)
        sections.append(pivot_html)
        sections.append(local_html)

    # Собираем HTML
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
        "    .container-max { max-width: 1200px; }",
        "    h1, h2, h3 { margin-top: 1.2rem; }",
        "    pre { background: #f7f7f7; padding: 12px; border-radius: 8px; }",
        "  </style>",
        "</head>",
        "<body>",
        "<div class='container container-max'>",
        "  <h1>Отчёт по JMH-бенчмаркам</h1>",
        f"  <p class='text-muted'>Сгенерировано: {now}</p>",
        "  <h2>Метаданные запуска</h2>",
        f"  <ul>{''.join(meta_lines)}</ul>",
        "  <p class='text-muted'>Примечание: если график использует log-шкалу по Y, значит разброс score очень большой и так читаемее.</p>",
        *sections,
        "</div>",
    ]

    # Если use_cdn=False и plotly был включён inline, отдельный script не нужен.
    # Если use_cdn=True — plotly сам подтянется из CDN (в include_plotlyjs='cdn'),
    # то отдельный script тоже не нужен.
    html_parts.extend(["</body>", "</html>"])

    out_path.write_text("\n".join(html_parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate HTML report from JMH JSON results.")
    parser.add_argument("json_path", type=Path, help="Path to jmh-result.json")
    parser.add_argument("--out", type=Path, default=Path("target/jmh-report.html"))
    parser.add_argument("--offline", action="store_true", help="Embed plotly.js into HTML (bigger file, works offline)")
    args = parser.parse_args()

    df, meta = load_jmh_json(args.json_path)
    build_report(df, meta, args.out, use_cdn=not args.offline)

    print(f"OK: report saved to {args.out.resolve()}")


if __name__ == "__main__":
    main()