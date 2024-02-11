import sys
import pytest
from pathlib import Path

sys.path.append((Path(__file__).resolve().parents[1] / "src/data_loader").as_posix())
from data_loader import DataLoader

TEST_FILES = Path(__file__).parent / "test_files"
"""
DataLoader(
    path: Any | None = None,
    directories: Any | None = None,
    default_extensions: Any | None = None,
    full_posix: bool = False,
    no_method: bool = False,
    verbose: bool = False,
    generator: bool = True,
    total_workers: Any | None = None
)
"""
@pytest.fixture(params=[(
    TEST_FILES,
    True,
    False
)])
def test_basic_path_params(request):
    return request.param

def test_basic_path(test_basic_path_params):
    path, posix, gen = test_basic_path_params
    dl_data = DataLoader(
        path=path,
        full_posix=posix,
        generator=gen
    ).files
    assert len(dl_data) == 11
    