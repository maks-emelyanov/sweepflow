"""Generated paper units keep checkout paths literal and reviewable."""

from pathlib import Path

import pytest

from ops.render_systemd import ROOT_MARKER, TEMPLATE_DIR, render_units


def test_renderer_writes_paper_units_and_escapes_literal_checkout_path(tmp_path):
    root = tmp_path / "checkout space $literal 10%"
    root.mkdir()
    output = tmp_path / "rendered"
    paths = render_units(root, output)
    assert {path.name for path in paths} == {
        "sweepflow-paper.service",
        "sweepflow-paper.timer",
    }
    for path in paths:
        text = path.read_text()
        assert ROOT_MARKER not in text
        if path.suffix == ".service":
            escaped = str(root).replace("%", "%%")
            assert f"WorkingDirectory={escaped}\n" in text
            assert f'ExecStart="{escaped}/.venv/bin/python"' in text
            assert f"Documentation={(root / 'README.md').as_uri().replace('%', '%%')}\n" in text
        else:
            assert text == (TEMPLATE_DIR / path.name).read_text()
    assert "sweepflow.scheduled --config" in (output / "sweepflow-paper.service").read_text()


@pytest.mark.parametrize(
    "name", ['quote"', "quote'", "back\\slash", "line\nbreak", " trailing ", "control\x7f"]
)
def test_renderer_rejects_ambiguous_systemd_paths_without_partial_output(tmp_path, name):
    root = tmp_path / name
    root.mkdir()
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="Checkout paths"):
        render_units(root, output)
    assert not output.exists()


def test_renderer_requires_checkout_and_preserves_templates(tmp_path):
    before = {path.name: path.read_bytes() for path in TEMPLATE_DIR.glob("*")}
    with pytest.raises(ValueError, match="does not exist"):
        render_units(tmp_path / "missing", tmp_path / "output")
    with pytest.raises(ValueError, match="separate"):
        render_units(tmp_path, TEMPLATE_DIR)
    assert {path.name: path.read_bytes() for path in TEMPLATE_DIR.glob("*")} == before


def test_rendered_units_are_independent_of_current_working_directory(tmp_path, monkeypatch):
    root = tmp_path / "checkout"
    root.mkdir()
    monkeypatch.chdir(root)
    paths = render_units(Path("."), Path("units"))
    assert all(path.is_absolute() for path in paths)
    assert f"WorkingDirectory={root}" in paths[0].read_text()
