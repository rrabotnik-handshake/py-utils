import sys
import gzip
from schema_diff.io_utils import _run
import pytest
from schema_diff.config import Config


@pytest.fixture
def cfg_like():
    # deterministic, no color noise in output
    return Config(infer_datetimes=False, color_enabled=False, show_presence=True)


@pytest.fixture
def write_file(tmp_path):
    def _w(name: str, text: str, gz: bool = False) -> str:
        p = tmp_path / name
        if gz:
            with gzip.open(p, "wt", encoding="utf-8") as f:
                f.write(text)
        else:
            p.write_text(text, encoding="utf-8")
        return str(p)

    return _w


@pytest.fixture
def run_cli():
    def _run_cli(args: list[str]):
        exe = [sys.executable, "-m", "schema_diff.cli"]

        # Auto-detect if we need to prepend "compare" subcommand
        # If first arg is not a known subcommand, assume it's a comparison
        known_subcommands = {"compare", "generate", "ddl", "config"}
        if args and not args[0] in known_subcommands:
            # Check if first arg looks like a file path or starts with --
            if args[0].startswith("--") or "/" in args[0] or "." in args[0]:
                args = ["compare"] + args

        return _run(exe + args)

    return _run_cli
