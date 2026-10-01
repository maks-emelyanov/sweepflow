"""Render user systemd units for a checkout without installing or starting them."""

from __future__ import annotations

import argparse
from pathlib import Path

ROOT_MARKER = "@SWEEPFLOW_ROOT@"
TEMPLATE_DIR = Path(__file__).resolve().parent / "systemd"
DEFAULT_ROOT = Path(__file__).resolve().parent.parent


def render_units(root: Path, output_dir: Path) -> tuple[Path, Path]:
    root, output_dir = root.resolve(), output_dir.resolve()
    root_text = str(root)
    if not root.is_dir():
        raise ValueError(f"Checkout root does not exist: {root}")
    if (
        root_text != root_text.strip()
        or any(character in root_text for character in ('"', "'", "\\"))
        or any(ord(character) < 32 or ord(character) == 127 for character in root_text)
    ):
        raise ValueError(
            "Checkout paths cannot contain quotes, backslashes, control characters, "
            "or leading/trailing whitespace"
        )
    if output_dir == TEMPLATE_DIR:
        raise ValueError("Choose an output directory separate from ops/systemd templates")

    template = (TEMPLATE_DIR / "sweepflow-paper.service").read_text(encoding="utf-8")
    timer = (TEMPLATE_DIR / "sweepflow-paper.timer").read_text(encoding="utf-8")
    # Path directives consume the complete value; quoting WorkingDirectory would
    # become part of its path. Exec executable tokens are quoted in the template;
    # unlike subsequent arguments, they do not expand environment variables.
    path_value = root_text.replace("%", "%%")
    documentation_url = (root / "README.md").as_uri().replace("%", "%%")
    rendered = []
    for line in template.splitlines(keepends=True):
        if line.startswith("Documentation="):
            rendered.append(f"Documentation={documentation_url}\n")
        else:
            rendered.append(line.replace(ROOT_MARKER, path_value))

    output_dir.mkdir(parents=True, exist_ok=True)
    service_path = output_dir / "sweepflow-paper.service"
    timer_path = output_dir / "sweepflow-paper.timer"
    service_path.write_text("".join(rendered), encoding="utf-8")
    timer_path.write_text(timer, encoding="utf-8")
    return service_path, timer_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path, help="Directory for the rendered units")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Checkout path (defaults to this repository)",
    )
    args = parser.parse_args()
    try:
        paths = render_units(args.root, args.output_dir)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
