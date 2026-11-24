#!/usr/bin/env python3
"""Validate a test sequence-diagram model and render it into a self-contained HTML page.

Usage: python3 render.py <model.json> [-o out.html]
"""
import argparse
import json
import sys
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent
TEMPLATE = SKILL_DIR / "template.html"
STEP_KINDS = {"message", "signal", "self", "assert", "note"}
TONES = {"info", "ok", "warn", "error"}


def find_repo_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / ".git").exists() or (p / "settings.gradle").exists() or (p / "settings.gradle.kts").exists():
            return p
    return Path.cwd()


def validate(model: dict, source_lines: int) -> list[str]:
    errs = []
    for key in ("title", "participants", "columns", "initialDb", "steps"):
        if key not in model:
            errs.append(f"missing top-level key '{key}'")
    if errs:
        return errs

    ids = [p.get("id") for p in model["participants"]]
    if len(set(ids)) != len(ids):
        errs.append(f"duplicate participant ids: {ids}")
    pid = set(ids)
    ncols = len(model["columns"])

    def check_rows(rows, where):
        if not isinstance(rows, list):
            errs.append(f"{where}: rows must be a list")
            return
        for r in rows:
            if not isinstance(r, list) or len(r) != ncols:
                errs.append(f"{where}: row {r!r} must have {ncols} cells ({model['columns']})")

    check_rows(model["initialDb"], "initialDb")

    for i, s in enumerate(model["steps"], 1):
        w = f"step {i}"
        kind = s.get("kind")
        if kind not in STEP_KINDS:
            errs.append(f"{w}: unknown kind {kind!r} (expected one of {sorted(STEP_KINDS)})")
            continue
        refs = []
        if kind in ("message", "signal"):
            refs = [s.get("from"), s.get("to")]
        elif kind in ("self", "assert"):
            refs = [s.get("at")]
        elif kind == "note":
            refs = s.get("over") or []
            if not refs:
                errs.append(f"{w}: note needs non-empty 'over'")
        for r in refs:
            if r not in pid:
                errs.append(f"{w}: unknown participant {r!r}")
        if kind != "note" and not s.get("label"):
            errs.append(f"{w}: missing 'label'")
        if kind == "note" and not s.get("text"):
            errs.append(f"{w}: note missing 'text'")
        if s.get("tone") and s["tone"] not in TONES:
            errs.append(f"{w}: tone must be one of {sorted(TONES)}")
        if s.get("tx") and s["tx"] not in ("begin", "commit", "rollback"):
            errs.append(f"{w}: tx must be begin|commit|rollback")
        for ln in s.get("lines", []):
            if not isinstance(ln, int) or ln < 1 or (source_lines and ln > source_lines):
                errs.append(f"{w}: line {ln!r} outside source (1..{source_lines})")
        st = s.get("state")
        if st:
            if "committed" in st:
                check_rows(st["committed"], f"{w}.state.committed")
            for tx, rows in (st.get("pending") or {}).items():
                if tx not in pid:
                    errs.append(f"{w}.state.pending: unknown participant {tx!r}")
                check_rows(rows, f"{w}.state.pending.{tx}")
            for tx in (st.get("views") or {}):
                if tx not in pid:
                    errs.append(f"{w}.state.views: unknown participant {tx!r}")
    return errs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("model")
    ap.add_argument("-o", "--out")
    args = ap.parse_args()

    model_path = Path(args.model).resolve()
    model = json.loads(model_path.read_text(encoding="utf-8"))
    root = find_repo_root(model_path.parent)

    source = ""
    if model.get("sourcePath"):
        src = root / model["sourcePath"]
        if not src.exists():
            print(f"error: sourcePath not found: {src}", file=sys.stderr)
            return 1
        source = src.read_text(encoding="utf-8")

    errs = validate(model, len(source.splitlines()))
    if errs:
        print("Model is invalid:\n  - " + "\n  - ".join(errs), file=sys.stderr)
        return 1

    def embed(obj) -> str:
        # Safe inside <script type="application/json">
        return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")

    html = TEMPLATE.read_text(encoding="utf-8")
    html = html.replace("__TITLE__", model["title"].replace("<", "&lt;"))
    html = html.replace("/*__MODEL__*/", embed(model))
    html = html.replace("/*__SOURCE__*/", embed(source))

    out = Path(args.out) if args.out else model_path.with_suffix(".html")
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out} ({len(model['steps'])} steps)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
