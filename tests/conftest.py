import pytest
from schema_diff.config import Config


@pytest.fixture
def cfg_like():
    # deterministic, no color noise in output
    return Config(infer_datetimes=False, color_enabled=False, show_presence=True)
