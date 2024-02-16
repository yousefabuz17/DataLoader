import sys
import pytest
from pathlib import Path

sys.path.append(
    (
        (PY_TESTS_FILE := Path(__file__)).resolve().parents[1] / "src/data_loader"
    ).as_posix()
)
from data_loader import DataLoader, DataMetrics, DLoaderException, GetLogger


logger = GetLogger(name=(PY_TESTS_FILE.parent / PY_TESTS_FILE.stem).as_posix()).logger
get_files = lambda **kwargs: DataLoader(log=logger, **kwargs).files
get_dir_files = lambda **kwargs: DataLoader(log=logger, **kwargs).dir_files
get_paths_exts = lambda func: (DataLoader._rm_period(Path(p).suffix) for p in func)
Import = lambda m, p: DataLoader._import(module_name=m, package=p)
TEST_DATAMETRICS = PY_TESTS_FILE.parent / "test_datametrics"
TEST_FILES = PY_TESTS_FILE.parent / "test_files"
TEST_DIRS = DataLoader.get_files(
    directory=TEST_FILES / "test_directories",
    startswith="test",
    generator=False,
    log=logger,
)

TEST_FILES_COUNT, TEST_DIRS_COUNT = 11, 25


def validate_posix(data, *, mod_import=None, is_abs, org_data=None, check_only=False):
    package = (
        Import(*mod_import) if mod_import and isinstance(mod_import, tuple) else None
    )
    is_abs_func = lambda pv: Path(pv).is_absolute()
    if check_only:
        assert is_abs_func(data) is is_abs
    else:
        assert all(map(lambda pv: is_abs_func(pv), data)) is is_abs
    if package:
        assert isinstance(org_data, package)


# ?> ===============================================================================
# ^ -------------------------Start of DataLoader Test Cases-------------------------
# ?> ===============================================================================


@pytest.fixture(
    params=[
        ("invalid/file/path", ("ccsv", None), "32", ("invalid/dir/1", "invalid/dir/2"))
    ]
)
def test_paths_fake_params(request):
    return request.param


def test_dataloader_exception(test_paths_fake_params):
    fake_path, fake_exts, fake_workers, fake_dirs = test_paths_fake_params
    with pytest.raises(DLoaderException):
        get_files(path=None)
        get_files(path=fake_path)
        get_files(path=TEST_FILES, default_extensions=fake_exts)
        get_files(
            path=TEST_FILES, default_extensions=fake_exts, total_workers=fake_workers
        )
        get_dir_files(directories=None)
        get_dir_files(directories=fake_dirs)
        get_dir_files(directories=TEST_DIRS, default_extensions=fake_exts)
        get_dir_files(
            directories=TEST_DIRS,
            default_extensions=fake_exts,
            total_workers=fake_workers,
        )


@pytest.fixture(params=[(True, False)])
def test_posix_params(request):
    return request.param


def test_files_posix_dict(test_posix_params):
    T, F = test_posix_params
    dl_files_posix = get_files(path=TEST_FILES, full_posix=T, generator=F)
    dl_files_not_posix = get_files(path=TEST_FILES, full_posix=F, generator=F)
    dl_dir_files_posix = get_dir_files(directories=TEST_DIRS, full_posix=T, generator=F)
    dl_dir_files_not_posix = get_dir_files(
        directories=TEST_DIRS, full_posix=F, generator=F
    )
    val_posix_func = lambda d, i: validate_posix(d, mod_import=dict, is_abs=i)

    def test_len(data, length):
        assert len(list(data)) == length

    val_posix_func(dl_files_posix, T)
    val_posix_func(dl_files_not_posix, F)
    val_posix_func(dl_dir_files_posix, T)
    val_posix_func(dl_dir_files_not_posix, F)
    test_len(dl_files_posix, TEST_FILES_COUNT)
    test_len(dl_files_not_posix, TEST_FILES_COUNT)
    test_len(dl_dir_files_posix, TEST_DIRS_COUNT)
    test_len(dl_dir_files_not_posix, TEST_DIRS_COUNT)


def test_files_posix_gen(test_posix_params):
    T, F = test_posix_params
    tee = Import("itertools", "tee")
    dl_files_posix = tee(get_files(path=TEST_FILES, full_posix=T, generator=T))
    dl_files_not_posix = tee(get_files(path=TEST_FILES, full_posix=F, generator=T))
    dl_dir_files_posix = tee(
        get_dir_files(directories=TEST_DIRS, full_posix=T, generator=T)
    )
    dl_dir_files_not_posix = tee(
        get_dir_files(directories=TEST_DIRS, full_posix=F, generator=T)
    )
    val_posix_func = lambda d, i: validate_posix(
        (g[0] for g in d), mod_import=("typing", "Iterable"), is_abs=i, org_data=d
    )

    def test_len(data, length):
        assert len(list(data)) == length

    val_posix_func(dl_files_posix[0], T)
    val_posix_func(dl_files_not_posix[0], F)
    val_posix_func(dl_dir_files_posix[0], T)
    val_posix_func(dl_dir_files_not_posix[0], F)
    test_len(dl_files_posix[1], TEST_FILES_COUNT)
    test_len(dl_files_not_posix[1], TEST_FILES_COUNT)
    test_len(dl_dir_files_posix[1], TEST_DIRS_COUNT)
    test_len(dl_dir_files_not_posix[1], TEST_DIRS_COUNT)


def test_no_method_param():
    dl_files = get_files(path=TEST_FILES, no_method=True)
    dl_dir_files = get_dir_files(directories=TEST_DIRS, no_method=True)
    textio = Import("io", "TextIOWrapper")

    def is_instance(f, o_type):
        assert all(isinstance(v, o_type) for _k, v in f)

    is_instance(dl_files, textio)
    is_instance(dl_dir_files, textio)


def test_default_extensions():
    def loader(method=get_files, **kwargs):
        return lambda exts: method(
            **kwargs, default_extensions=exts, full_posix=True, generator=False
        )

    def ext_checker(test_dl_func):
        tests_exts = get_paths_exts(test_dl_func(exts=None))
        if test_dl_func == get_dir_files:
            ext_data = test_dl_func(exts=tests_exts)
            assert all(s.endswith(ext) for s in ext_data for ext in tests_exts)

        for ext in tests_exts:
            assert ext in DataLoader.EXTENSIONS
            ext_data = test_dl_func(exts=[ext])
            assert all(s.endswith(ext) for s in ext_data)

    dl_files = loader(path=TEST_FILES)
    dl_dir_files = loader(method=get_dir_files, directories=TEST_DIRS)
    ext_checker(dl_files)
    ext_checker(dl_dir_files)


# ?> ===============================================================================
# ^ -------------------------End of DataLoader Test Cases---------------------------
# ?> ===============================================================================


# ?> ===============================================================================
# ^ -------------------------Start of DataMetrics Test Cases------------------------
# ?> ===============================================================================


@pytest.fixture(
    params=[
        (
            ("fake/path/1", "fake/path/2"),
            "32",
            (dm_paths := get_files(path=TEST_FILES, generator=False)),
        )
    ]
)
def test_datametrics_fake_params(request):
    return request.param


def test_datametrics_params(test_datametrics_fake_params):
    fake_path, fake_workers, working_paths = test_datametrics_fake_params
    with pytest.raises(DLoaderException):
        DataMetrics(files=fake_path)
        DataMetrics(files=working_paths, total_workers=fake_workers)


def test_datametrics_class():
    d_files_metrics_not_posix = DataMetrics(
        files=dm_paths,
        full_posix=False,
        file_name=(
            file_metrics_stats_file := TEST_DATAMETRICS / "test_datametrics_files"
        ),
    )
    d_files_metrics_posix = DataMetrics(files=dm_paths, full_posix=True)
    dir_files = get_dir_files(directories=TEST_DIRS, generator=False)
    d_dir_metrics_not_posix = DataMetrics(
        files=dir_files,
        full_posix=False,
        file_name=(
            dir_metrics_stats_file := TEST_DATAMETRICS / "test_datametrics_dir_files"
        ),
    )
    d_dir_metrics_posix = DataMetrics(files=dir_files, full_posix=True)

    def test_len(data, length):
        assert len(list(data.all_stats)) == length
        assert data.total_files == length

    def validate_tuple(d_tuple):
        assert hasattr(d_tuple, "__module__") and d_tuple.__module__ == "StatsTuple"

    def stats_checker(metrics_data, posix=False, length=0):
        test_len(data=metrics_data, length=length)
        all_stats = metrics_data.all_stats
        assert isinstance(all_stats, dict) and len(all_stats) > 0
        assert isinstance(metrics_data.total_size.bytes_size, int)
        for p, d in all_stats.items():
            if Path(p).name.startswith("test"):
                assert Path(p).name.startswith("test")
            validate_posix(p, is_abs=posix, check_only=True)
            for k, v in d.items():
                assert k.startswith("st")
                if k == "st_vsize":
                    assert all(k in v for k in ("total", "used", "free"))
                    for sv in v.values():
                        validate_tuple(sv)
                else:
                    validate_tuple(v)
                    assert all(
                        s in v._asdict()
                        for s in ("symbolic", "calculated_size", "bytes_size")
                    )

    stats_checker(d_files_metrics_not_posix, posix=False, length=TEST_FILES_COUNT)
    stats_checker(d_files_metrics_posix, posix=True, length=TEST_FILES_COUNT)
    stats_checker(d_dir_metrics_not_posix, posix=False, length=TEST_DIRS_COUNT)
    stats_checker(d_dir_metrics_posix, posix=True, length=TEST_DIRS_COUNT)
    d_files_metrics_not_posix.export_stats()
    d_dir_metrics_not_posix.export_stats()

    def file_checker(stats_file):
        assert stats_file.with_suffix(".json").is_file()

    file_checker(file_metrics_stats_file)
    file_checker(dir_metrics_stats_file)


# ?> ===============================================================================
# ^ -------------------------End of DataMetrics Test Cases--------------------------
# ?> ===============================================================================
