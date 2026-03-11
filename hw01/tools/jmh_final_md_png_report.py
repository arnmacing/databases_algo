import argparse
from pathlib import Path

from jmh_html_report import build_markdown_report, load_jmh_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create final markdown report with PNG collages from JMH JSON "
            "(default: hw01/target/jmh-all-gc.json)."
        )
    )
    parser.add_argument(
        "json_path",
        nargs="?",
        type=Path,
        default=Path("hw01/target/jmh-all-gc.json"),
        help="Path to JMH JSON (default: hw01/target/jmh-all-gc.json)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("hw01/target/jmh-final-report.md"),
        help="Markdown output path",
    )
    parser.add_argument(
        "--plots-dir",
        type=Path,
        default=Path("hw01/target/jmh-final-plots"),
        help="Directory for PNG collages and CSV files",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Ignored for PNG (kept for CLI compatibility).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

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
    build_markdown_report(
        df=df,
        meta=meta,
        param_cols=param_cols,
        out_path=args.out,
        plots_dir=args.plots_dir,
        use_cdn=not args.offline,
        plots_format="png",
    )

    print(f"OK: {args.out.resolve()}")
    print(f"OK: {args.plots_dir.resolve()}")


if __name__ == "__main__":
    main()
