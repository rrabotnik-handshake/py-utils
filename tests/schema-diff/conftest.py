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
        return _run(exe + args)

    return _run_cli
