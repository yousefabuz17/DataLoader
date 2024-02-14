"""
# DataLoader Project

## Overview:
This project provides utility classes and functions for dynamic loading and processing of data from specified directories.
The main components include the `DataLoader` class, responsible for loading files from directories, and the `DataMetrics` class, which gathers and exports OS statistics for specified paths.

## Classes:
1. `DataLoader`:
    - Main class for dynamically loading and processing data.
    - Args:
        - `path` (str or Path): Path of the directory to load files.
        - `directories` (Iterable): Additional directories to merge with the specified path.
        - `default_extensions` (Iterable): Default file extensions to be processed.
        - `full_posix` (bool): Display full POSIX paths.
        - `no_method` (bool): Skip loading method matching execution.
        - `verbose` (bool): Display verbose output.
        - `generator` (bool): Return loaded files as a generator.
        - `total_workers` (int): Number of workers for parallel execution.
    - Methods:
        - `load_file`: Load a single file.
        - `get_files`: Class method to get files from a directory based on default extensions.
        - `dir_files` (property): Load files from specified directories.
        - `files` (property): Load files from specified path.

2. `DataMetrics`:
    - Class for gathering and exporting OS stats for specified paths.
    - Args:
        - `paths` (Iterable): Paths for which to gather statistics.
        - `full_posix` (bool): Display full POSIX paths.
        - `file_name` (str): The file name to be used when exporting all files metadata stats.
        - `total_workers` (int): Number of workers for parallel execution.
    - Methods:
        - `export_stats()`: Export gathered statistics to a JSON file.
        - `all_stats`: Get all gathered statistics.

## Important Notes and Features:
- The project includes a custom exception class `DLoaderException` for handling DataLoader-specific exceptions.
- Logging is facilitated through the `get_logger` function to enable configurable logging levels and output.
- The project supports parallel execution of file loading and statistics gathering using `ThreadPoolExecutor`.
- Metadata information about the project is available in the `METADATA` dictionary.
- File extensions and their corresponding loading methods are defined in the `Extensions` class.
- The project provides a timer class, `Timer`, as a context manager to measure execution time.
- The `data_loader.py` module contains additional utility classes and functions for file handling.
"""

import sys
import importlib
import inspect
import json
import logging
import mimetypes
import operator
import os
import pickle
import re
import shutil
from collections import namedtuple
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import cache, cached_property, partial, wraps
from io import TextIOWrapper
from itertools import chain
from logging import Logger
from pathlib import Path
from reprlib import recursive_repr
from time import time
from typing import Any, Generator, Iterable, Iterator, NamedTuple, Union

import pandas as pd
from pandas.errors import DtypeWarning, EmptyDataError, ParserError
from json.decoder import JSONDecodeError

sys.path.append((Path(__file__).parent).as_posix())
from other_extensions import OTHER_EXTS


def get_logger(
    *,
    name: str = __name__,
    level: int = logging.DEBUG,
    formatter_kwgs: dict = None,
    handler_kwgs: dict = None,
    mode: str = "a",
    write_log: bool = True,
) -> Logger:
    logging.getLogger().setLevel(logging.NOTSET)
    logger_ = logging.getLogger(name)

    if logging.getLevelName(level):
        logger_.setLevel(level=level)

    file_name = Path(__file__).with_suffix(".log")
    formatter_kwgs_ = {
        **{
            "fmt": "[%(asctime)s][LOG %(levelname)s]:%(message)s",
            "datefmt": "%Y-%m-%d %I:%M:%S %p",
        },
        **(formatter_kwgs or {}),
    }
    handler_kwgs_ = {**{"filename": file_name, "mode": mode}, **(handler_kwgs or {})}

    formatter = logging.Formatter(**formatter_kwgs_)

    if write_log or level == logging.DEBUG:
        stream_handler = logging.FileHandler(**handler_kwgs_)
    else:
        stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger_.addHandler(stream_handler)
    return logger_


logger = get_logger(level=logging.INFO, write_log=True)


class DLoaderException(BaseException):
    __slots__ = ("__weakrefs__", "log_method")

    def __init__(self, *args: object, log_method=logger.critical) -> None:
        self.log_method = log_method
        super().__init__(*args)
        self.log_method(*args)


@dataclass(slots=True, weakref_slot=True)
class Timer:
    message: str = field(default="")
    verbose: bool = field(default=False)
    _T_START: float = field(init=False, repr=False, default_factory=lambda: time)
    _T_END: float = field(init=False, repr=False, default_factory=lambda: time)

    def __enter__(self):
        self._T_START = self._T_START()
        return self._T_START

    def __exit__(self, *args, **kwargs):
        elapsed_time = self._T_END() - self._T_START
        minutes, seconds = divmod(elapsed_time, 60)
        if self.verbose:
            if self.message:
                print(f"\033[33m{self.message!r}\033[0m")
            print(
                f"\033[32mExecution Time:\033[0m {minutes:.0f} minutes and {seconds:.5f} seconds."
            )


class _BaseLoader:
    _EXECUTOR = ThreadPoolExecutor

    @classmethod
    def _compiler(
        cls,
        defaults: Iterable,
        k: str,
        escape_default: bool = True,
        escape_k: bool = True,
        search: bool = True,
    ) -> re.Match:
        valid_instances = (int, str, bool, bytes, Iterable)
        if any((not k, not isinstance(k, valid_instances), hasattr(k, "__str__"))):
            esc_k = str(k)
        else:
            raise DLoaderException(
                f"The value for 'k' is not a valid type."
                f"\nk value and type: ({k =}, {type(k) =})"
            )

        defaults_ = map(re.escape, map(str, defaults))
        flag = "|" if escape_default else ""
        pattern = f"{flag}".join(defaults_)
        if escape_k:
            esc_k = "|".join(map(re.escape, k))

        compiler = re.compile(pattern, re.IGNORECASE)
        if not search:
            compiled = compiler.match(esc_k)
        else:
            compiled = compiler.search(esc_k)
        return compiled

    @staticmethod
    def _import(module_name: str = "typing", *, package: str = "Any", boolean=False):
        try:
            library = getattr(importlib.import_module(module_name), package)
        except (AttributeError, ModuleNotFoundError):
            library = False
            if not boolean:
                raise DLoaderException(
                    f"An error occured trying to import {package = } from {module_name = }"
                )
        return library

    @classmethod
    def _cap_cls_name(cls, cls_value: Any):
        c_name = (
            cls_value.__name__
            if hasattr(cls_value, "__name__")
            else cls_value.__class__.__name__
        )
        if c_name.startswith("_"):
            c_name = c_name.lstrip("_")
        if cls._compiler(r"\_", c_name, escape_k=False):
            c_name = "".join((i.capitalize() for i in c_name.split("_")))
        elif not c_name[0].isupper():
            c_name = c_name.capitalize()
        return c_name

    @classmethod
    def _exporter(cls, file: str | Path, data: Any):
        fp = Path(file).with_suffix(".json")
        with open(fp, mode="w") as metadata:
            json.dump(data, metadata, indent=4)
        print(f"\033[34m{fp!r}\033[0m. has successfully been exported.")

    @classmethod
    def _too_large(
        cls,
        value: Any,
        max_length: int = None,
        boolean: bool = False,
        tag: bool = False,
    ):
        max_len = (
            max_length if isinstance(max_length, int) else cls._terminal_size().columns
        )
        cls_tag = f"<{cls._cap_cls_name(value)}>"
        too_large = False
        try:
            org_length = len(str(value))
        except TypeError:
            org_length = max_len
        if org_length >= max_len or any(
            (
                isinstance(value, Generator),
                isinstance(value, ConfigParser),
                isinstance(value, TextIOWrapper),
                isinstance(value, (pd.DataFrame, pd.Series)),
                type(value) == type,
            )
        ):
            too_large = True
        if boolean:
            return too_large
        if tag:
            return cls_tag
        return value if not too_large else cls_tag

    @classmethod
    def _validate_file(
        cls, file_path: Union[str, Path], directory: bool = False, verbose: bool = False
    ) -> Path:
        try:
            fp = Path(file_path)
        except TypeError as t_error:
            raise DLoaderException(t_error)

        if not fp:
            raise DLoaderException(f"File arugment must not be empty: {fp =!r}")
        elif not fp.exists():
            raise DLoaderException(
                f"File does not exist: {fp =!r}. Please check system files."
            )
        elif all((not fp.is_file(), not fp.is_absolute())):
            raise DLoaderException(
                f"Invalid path type: {fp =!r}. Path must be a file type."
            )
        elif directory and fp.is_dir():
            raise DLoaderException(
                f"File is a directory: {fp =!r}. Argument must be a valid file."
            )
        elif any(
            (
                cls._compiler(r"^[._]", fp.stem, escape_default=False),
                fp.stem.startswith((".", "_")),
            )
        ):
            if verbose:
                DLoaderException(f"Skipping {fp.name =!r}", log_method=logger.warning)
            return
        return fp

    @staticmethod
    def _terminal_size() -> tuple[int, int]:
        return shutil.get_terminal_size()

    @staticmethod
    def none_generator(d: Any, default: Any = None) -> list:
        return [default] * len(d)

    @staticmethod
    def _rm_period(p: str, string=True):
        rm_p = lambda s: s.lstrip(".").lower()
        return type(p)(rm_p(e) for e in p) if not string else rm_p(p)

    @staticmethod
    def _all_errors():
        return frozenset(
            {
                PermissionError,
                UnicodeDecodeError,
                ParserError,
                DtypeWarning,
                OSError,
                EmptyDataError,
                JSONDecodeError,
                Exception,
            }
        )

    @classmethod
    def _base_executor(cls, func, y, total_workers=None):
        return cls._EXECUTOR(max_workers=total_workers).map(func, y)

    @classmethod
    def _read_config(cls, file):
        c = ConfigParser()
        c.read(file)
        return c

    @staticmethod
    def _check_workers(workers: int | float):
        if not workers or isinstance(workers, (int, float)):
            return workers
        raise DLoaderException(
            f"The 'total_workers' parameter must be of type {int!r} or {float!r}"
            f"\n>>>Workers Input: {workers =!r}"
        )

    @classmethod
    def _create_subclass(
        cls,
        typename: str = "FieldTuple",
        /,
        field_names: Iterable = None,
        *,
        module: str = None,
        defaults: Iterable = None,
        values: Iterable = None,
        field_doc: str = "",
    ) -> NamedTuple:
        default_vals = defaults or ([None] * len(field_names))

        field_docs = field_doc or "Field documentation not provided."
        module_name = module or typename
        new_tuple = namedtuple(
            typename=typename,
            field_names=field_names,
            defaults=default_vals,
            module=module_name,
        )
        setattr(new_tuple, "__doc__", field_docs)
        if values:
            return new_tuple(*values)
        return new_tuple

    @classmethod
    def _get_params(cls, __object: Any = None):
        try:
            obj = __object or cls
            params = inspect.signature(obj)
        except TypeError:
            raise DLoaderException(
                "Failed to retrieve parameters. Ensure that the provided object or class is inspectable."
            )
        return {k: v.default for k, v in params.parameters.items()}

    @classmethod
    def _get_subclass(cls):
        return cls._create_subclass(
            "ExtTuple",
            ("suffix_", "loader_"),
            field_doc="Primary NamedTuple containing the suffix and matched loader for it.",
        )


class _SpecialDictRepr(dict):
    def __init__(self, *args, **kwargs):
        module = kwargs.pop("module", "Dict")
        super().__init__(*args, **kwargs)
        self._module = module

    @recursive_repr()
    def __repr__(self) -> str:
        cls_name = self._module
        p_holder, spacing = "({}, {})", f",\n{' ':>{len(cls_name)+1}}"
        too_large = lambda val: _BaseLoader._too_large(
            val, tag=self._check_length(self.values())
        )
        repr_items = (p_holder.format(k, too_large(v)) for k, v in self.items())
        return f"{cls_name}({spacing.join(repr_items)})"

    __str__ = __repr__

    def _check_length(self, values: Iterable):
        return any(map(partial(_BaseLoader._too_large, boolean=True), values))

    def reset(self):
        return dict(self.items())


class _SpecialGenRepr(Iterable):
    __slots__ = ("__weakrefs__", "_id", "_gen", "_module", "_gen_id")

    def __init__(self, gen: Iterable, module: str = None) -> None:
        self._id = hex(id(self))
        self._gen = gen
        self._module = module or Iterable.__name__
        self._gen_id = hex(id(self._gen))

    def __iter__(self) -> Iterator:
        return iter(self._gen)

    @recursive_repr()
    def __repr__(self) -> str:
        if self._module == "DataLoader":
            return (
                f"<generator object {self._module}.files.<key-value> at {self._gen_id}>"
            )
        return f"<{self.__class__.__module__}.{self._module} object at {self._id}>"

    __str__ = __repr__


@dataclass(slots=True, weakref_slot=True)
class Extensions:
    _ALL_EXTS: dict = field(init=False, default=None)

    def __repr__(self) -> str:
        return f"{_SpecialDictRepr(sorted(self.ALL_EXTS.items(), key=lambda kv: kv[0]), module='Extensions')}"

    __str__ = __repr__

    @property
    def ALL_EXTS(self) -> dict:
        if not self._ALL_EXTS:
            self._ALL_EXTS = {
                **self._other_exts(),
                **self.defaults,
            }
        return self._ALL_EXTS

    def period_remover(cls_method):
        @wraps(cls_method)
        def wrappper(self, *args, **kwargs):
            args = map(self.subclass._rm_period, args)
            m = cls_method(self, *args, **kwargs)
            return m

        return wrappper

    @period_remover
    def __contains__(self, __ext: str) -> bool:
        return __ext in self.ALL_EXTS

    @period_remover
    def __getitem__(self, __ext: str) -> Any:
        return self.ALL_EXTS[__ext]

    def __getattr__(self, __ext: str) -> Any:
        return self[__ext]

    @property
    def subclass(self) -> _BaseLoader:
        return _BaseLoader()

    @property
    def defaults(self) -> dict:
        return {
            "csv": self._ext_tuple("csv", pd.read_csv),
            "hdf": self._ext_tuple("hdf", pd.read_hdf),
            "sql": self._ext_tuple("sql", pd.read_sql),
            "xml": self._ext_tuple("xml", pd.read_xml),
            "pickle": self._ext_tuple(
                "pickle", lambda path: pickle.load(open(path, mode="rb"))
            ),
            "json": self._ext_tuple("json", lambda path: json.load(open(path))),
            "empty": self._ext_tuple("", lambda path: open(path).read()),
        }

    def _other_exts(self) -> dict:
        all_other_exts = (
            self.subclass._rm_period(e)
            for e in chain.from_iterable((mimetypes.types_map.keys(), OTHER_EXTS))
        )
        return {k: self._ext_tuple(k, self._method_matcher(k)) for k in all_other_exts}

    def _ext_tuple(self, *args, **kwargs) -> NamedTuple:
        return self.subclass._get_subclass()(*args, **kwargs)

    @period_remover
    def _method_matcher(self, ext: str) -> Any:
        import_ext = (
            lambda m, p: self.subclass._import(module_name=m, package=p, boolean=True)
            or open
        )
        if ext in ("xls", "xlsx"):
            return pd.read_excel
        elif ext in ("cfg", "ini", "conf"):
            return lambda path: self.subclass._read_config(path)
        elif ext == "pdf":
            return import_ext("pdfminer.high_level", "extract_pages")
        else:
            return self.defaults["empty"].loader_

    def get_loader(self, ext: str) -> Any:
        return self[ext]

    def is_supported(self, ext: str) -> bool:
        return ext in self

    def has_loader(self, ext: str) -> bool:
        return self.get_loader(ext) is not open

    def customize(self, **kwargs: dict[str, Any, dict]) -> dict:
        new_exts = {}
        for ext, ext_values in kwargs.items():
            l_method, l_kwargs = ext_values
            l_params = self.subclass._get_params(l_method)
            l_kwargs = {k: v for k, v in l_kwargs.items() if k in l_params}
            new_exts[ext] = self._ext_tuple(
                ext, lambda path: l_method(path, **l_kwargs)
            )
        if new_exts:
            new_exts.update({"empty": self.defaults["empty"]})
        return new_exts


class DataLoader(_BaseLoader):
    """
    The DataLoader class is designed to dynamically load and manage data from specified directories.

    Args:
        - `path` (str or Path): The path of the directory from which to load files.
        - `directories` (Iterable): An iterable of directories from which to all files.
        - `default_extensions` (Iterable): Default file extensions to be processed.
        - `full_posix` (bool): Indicates whether to display full POSIX paths.
        - `no_method` (bool): Indicates whether to skip loading method matching execution.
        - `verbose` (bool): Indicates whether to display verbose output.
        - `generator` (bool): Indicates whether to return the loaded files as a generator; otherwise, returns as a dictionary.
        - `total_workers` (int): Number of workers for parallel execution.
        - `ext_loaders` (Iterable[str, Any, dict]): Dictionary containing extensions mapped to specified loaders.

    Methods:
        - `load_file(file_path)`: Load a single file and return a named tuple containing path and contents.
        - `get_files(directory, defaults, verbose)`: Class method to get files from a directory based on default extensions.
        - `dir_files`: Load files from specified directories and return as a generator.
        - `files`: Get loaded files as a dictionary or generator, depending on the `generator` parameter.

    Attributes:
        - `all_exts`: Dictionary containing file extensions and their corresponding loading methods.

    Note:
        The DataLoader class inherits from the _BaseLoader class, which provides common utility functions and features.
    """

    __slots__ = (
        "__weakrefs__",
        "_path",
        "_directories",
        "_default_exts",
        "_full_posix",
        "_no_method",
        "_verbose",
        "_generator",
        "_total_workers",
        "_ext_loaders",
    )
    EXTENSIONS = Extensions()

    def __init__(
        self,
        path: str | Path = None,
        directories: Iterable = None,
        default_extensions: Iterable = None,
        full_posix: bool = True,
        no_method: bool = False,
        verbose: bool = False,
        generator: bool = True,
        total_workers: int | float = None,
        ext_loaders: dict = None,
    ):
        self._path = path
        self._directories = directories
        self._default_exts = default_extensions
        self._full_posix = full_posix
        self._no_method = no_method
        self._verbose = verbose
        self._generator = generator
        self._total_workers = self._check_workers(total_workers)
        self._files = None
        self._dir_files = None
        self._ext_loaders = ext_loaders
        logger.write_log = self._verbose

    @property
    def all_exts(self):
        extensions = self.EXTENSIONS
        return extensions.customize(**{}) or extensions

    def _ext_method(self, fp: str | Path):
        fp = self._validate_file(fp)
        suffix = self._rm_period(fp.suffix)
        empty_method = self.all_exts["empty"]
        if suffix == empty_method.suffix_:
            loading_method = empty_method.loader_
        elif self.all_exts.is_supported(suffix):
            try:
                loading_method = self.all_exts[suffix].loader_
            except AttributeError:
                DLoaderException(
                    f"ExtensionTypeError>>Check file {fp =!r} extensions. Failed to find a relative working loading method ({loading_method =!r}). Defaulting to {open!r}",
                    log_method=logger.warning,
                )
        return loading_method

    @cached_property
    def _get_files(self):
        return self.get_files(
            self._path,
            self.all_exts["empty"].suffix_
            if not self._default_exts
            else self._validate_exts(self._default_exts),
            self._verbose,
        )

    @classmethod
    def load_file(cls, fp_or_dir: str | Path):
        return cls()._load_file(cls._validate_file(fp_or_dir))

    @classmethod
    def get_files(
        cls,
        directory: str | Path,
        defaults: Iterable = "",
        startswith: str = "",
        verbose: bool = False,
    ):
        validate_file = partial(cls._validate_file, verbose=verbose)
        directory = validate_file(directory)
        no_dirs = lambda p: p.is_file() and not p.is_dir()
        ext_pattern = partial(cls._compiler, defaults, escape_k=False)
        filter_files = lambda fp: all(
            (no_dirs(fp), ext_pattern(cls._rm_period(fp.suffix)), validate_file(fp))
        )
        return (
            p
            for p in directory.iterdir()
            if p.name.startswith(f"{startswith}") and startswith or filter_files(p)
        )

    def _validate_exts(self, extensions: Iterable):
        if extensions is None:
            return

        try:
            org_exts = set(self._rm_period(e) for e in extensions)
            valid_exts = set(e for e in org_exts if e in self.all_exts)
            failed_exts = set(
                filter(lambda ext: self._rm_period(ext) not in valid_exts, extensions)
            )
        except Exception:
            raise Exception
        if failed_exts == org_exts:
            raise DLoaderException(
                f"DefaultExtensionsError>>All provided extensions are invalid: {org_exts!r}\nAll available extensions:\n{sorted(self.all_exts)}"
            )
        if failed_exts:
            DLoaderException(
                f"ExtensionsError>>Skipping invalid extensions: {failed_exts =!r}",
                log_method=logger.warning,
            )
        return valid_exts

    def _load_file(self, file_path: str | Path):
        fp_name = "/".join(file_path.parts[-2:])
        loading_method = open if self._no_method else self._ext_method(file_path)
        PathInfo = self._create_subclass(
            "PathInfo",
            ("path", "contents"),
            field_doc="Primary NamedTuple containing the path and its contents.",
        )
        with Timer(message=f"Executing {fp_name!r}", verbose=self._verbose):
            try:
                p_contents = loading_method(file_path)
            except tuple(self._all_errors()) as e:
                DLoaderException(e, log_method=logger.warning)
                p_contents = open(file_path)
            return PathInfo(path=file_path, contents=p_contents)

    @classmethod
    def _base_executor(cls, func: Any, y: Any, total_workers: int = None):
        return cls._EXECUTOR(max_workers=total_workers).map(func, y)

    def _get_dir_files(self):
        if not self._directories:
            raise DLoaderException(
                "The 'directories' parameter must be passed in for this type of execution."
            )
        executor = partial(self._base_executor, total_workers=self._total_workers)
        directories = executor(
            partial(self._validate_file, verbose=self._verbose), self._directories
        )
        dl_func = lambda p: DataLoader(
            path=p,
            full_posix=self._full_posix,
            default_extensions=self._default_exts,
            no_method=self._no_method,
            verbose=self._verbose,
            total_workers=self._total_workers,
        ).files
        dir_files = executor(dl_func, directories)
        return chain.from_iterable(dir_files)

    @cached_property
    def dir_files(self):
        if self._dir_files is None:
            self._dir_files = self._get_dir_files()
        return self.special_repr(self._dir_files)

    def _execute_path(self):
        try:
            files = self._base_executor(
                self._load_file, self._get_files, total_workers=self._total_workers
            )
        except Exception as error:
            raise DLoaderException(error)
        return (
            (f.name if not self._full_posix else f.as_posix(), v)
            for f, v in files
            if v is not None
        )

    @cached_property
    def files(self):
        if self._files is None:
            self._files = self._execute_path()
        return self.special_repr(self._files)

    @cache
    def special_repr(self, data: Iterable):
        module = self.__class__.__name__
        return (
            _SpecialDictRepr(data, module=module)
            if not self._generator
            else _SpecialGenRepr(data, module=module)
        )


class DataMetrics(_BaseLoader):
    """
    The DataMetrics class is designed to collect and export OS statistics for specified paths.

    #### Args:
        - `paths` (Iterable): Paths for which to gather statistics.
        - `file_name` (str): The file name to be used when exporting all files metadata stats.
        - `full_posix` (bool): Indicates whether to display full POSIX paths.

    #### Methods:
        - `export_stats()`: Export gathered statistics to a JSON file.
        - `all_stats` (property): Get all gathered statistics as a dictionary.
        - `total_size` (property): Retrieve the total size of all specified paths.
    """

    __slots__ = (
        "__weakrefs__",
        "_files",
        "_file_name",
        "_full_posix",
        "_total_workers",
        "_total_files",
        "_all_stats",
    )

    def __init__(
        self,
        files: Iterable,
        file_name: str = None,
        full_posix: bool = False,
        total_workers: int | float = None,
    ) -> None:
        self._files = self._validate_paths(files)
        self._file_name = file_name
        self._full_posix = full_posix
        self._total_workers = self._check_workers(total_workers)
        self._all_stats = None
        self._total_files = 0

    def __iter__(self):
        return iter(self._files)

    def __sizeof__(self) -> int:
        total_bytes = sum(
            j.bytes_size
            for _k, v in self.all_stats.items()
            for i, j in v.items()
            if i == "st_fsize"
        )
        return self._bytes_converter(total_bytes)

    def _validate_paths(self, paths: str | Path):
        return self._EXECUTOR().map(self._validate_file, paths)

    def _get_stats(self):
        return {
            p.name if not self._full_posix else p.as_posix(): self._os_stats(p)
            for p in self._files
        }

    @classmethod
    def _bytes_converter(
        cls,
        num,
        symbol_only=False,
        total_only=False,
    ):
        # XXX (KB)-1024, (MB)-1048576, (GB)-1073741824, (TB)-1099511627776
        if not num:
            return
        Stats = cls._get_subclass()
        conversions = dict(
            zip(
                (
                    "KB (Kilobytes)",
                    "MB (Megabytes)",
                    "GB (Gigabytes)",
                    "TB (Terabytes)",
                ),
                (operator.pow(base := 1024, n) for n in range(1, 5)),
            )
        )
        results = next(
            (f"{(total:=num/v):.2f} {k[:2] if symbol_only else k}", total, num)
            for k, v in conversions.items()
            if (num / base) < v
        )
        if not total:
            return
        if total_only:
            return total
        return Stats(*results)

    @classmethod
    def _get_subclass(cls):
        return cls._create_subclass(
            "Stats",
            ("symbolic", "calculated_size", "bytes_size"),
            field_doc="Primary NamedTuple containing OS stats results for specified paths.",
            module="StatsTuple",
        )

    @classmethod
    def _os_stats(cls, path: str | Path):
        Stats = cls._get_subclass()
        bytes_converter = DataMetrics._bytes_converter
        stats_results = os.stat_result(os.stat(path))
        disk_usage = shutil.disk_usage(path)._asdict()
        gattr = partial(getattr, stats_results)
        volume_stats = {k: bytes_converter(v) for k, v in disk_usage.items()}
        os_stats = {
            **{
                attr: bytes_converter(gattr(attr))
                for attr in dir(stats_results)
                if attr.startswith("st") and gattr(attr)
            },
            **{
                "st_fsize": Stats(
                    *bytes_converter(stats_results.st_size, symbol_only=True)
                ),
                "st_vsize": volume_stats,
            },
        }
        return os_stats

    def export_stats(self):
        fp_name = self._file_name or "all_metadata_stats"
        return self._exporter(fp_name, self.all_stats)

    @cached_property
    def all_stats(self):
        if self._all_stats is None:
            self._all_stats = self._get_stats()
        return _SpecialDictRepr(self._all_stats, module=self.__class__.__name__)

    @cached_property
    def total_size(self):
        return self.__sizeof__()

    @cached_property
    def total_files(self):
        if not self._total_files:
            self._total_files = len(list(self.all_stats))
        return self._total_files


# XXX Metadata Information
METADATA = {
    "version": (__version__ := "1.0.9"),
    "license": (__license__ := "Apache License, Version 2.0"),
    "url": (__url__ := "https://github.com/yousefabuz17/DataLoader"),
    "author": (__author__ := "Yousef Abuzahrieh <yousef.zahrieh17@gmail.com"),
    "copyright": (__copyright__ := f"Copyright © 2024, {__author__}"),
    "summary": (
        __summary__ := "Python utility designed to enable dynamic loading and processing of files."
    ),
    "doc": __doc__,
}

__all__ = (
    "METADATA",
    "DataLoader",
    "DataMetrics",
    "DLoaderException",
    "Extensions",
    "_SpecialDictRepr",
    "_SpecialGenRepr",
)
