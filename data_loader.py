import logging
import mimetypes
import re
import sys
import json
from collections import OrderedDict, namedtuple
from configparser import ConfigParser, NoSectionError
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache, singledispatch, singledispatchmethod
from itertools import filterfalse
from multiprocessing import cpu_count
from pathlib import Path
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView, Iterable,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from constants import _PASS, _ERRORS
import pandas as pd
from pandas.errors import ParserError, DtypeWarning, EmptyDataError
from pdfminer.high_level import extract_pages
from cryptography.fernet import Fernet
from dataclasses import dataclass, field
from reprlib import recursive_repr as _recursive_repr
from json.decoder import JSONDecodeError
import inspect


logging.basicConfig(level=logging.WARNING)

rm_p = lambda i: i.lstrip('.').lower()
compiler = lambda __defaults, __k: re.compile('|'.join(map(re.escape, __defaults)), re.IGNORECASE).search(__k)

class CConfigParser(ConfigParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, section, option, *, raw=False, vars=None, fallback=None):
        value = super().get(section, option, raw=raw, vars=vars, fallback=fallback)
        return self.convert_value(value)

    @staticmethod
    def convert_value(value):
        value = str(value).lower()
        _vals = {'true': True,
                'false': False,
                'none': None}
        return _vals.get(value, value)
    
    @staticmethod
    def encrypt_text(text, __export=False):
        Encrypter = namedtuple('Encrypter', ['text', 'key'])
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        encrypted_bytes = cipher_suite.encrypt(text.encode())
        encrypted_text = encrypted_bytes.hex()
        if not __export:
            return Encrypter(encrypted_text, key)
        
        CConfigParser._exporter(text, Encrypter(encrypted_text, key))
        return Encrypter(encrypted_text, key)

    @staticmethod
    def decrypt_text(encrypted_text, key):
        cipher_suite = Fernet(key)
        encrypted_bytes = bytes.fromhex(encrypted_text)
        decrypted_message = cipher_suite.decrypt(encrypted_bytes).decode()
        return decrypted_message
    
    @staticmethod
    def _exporter(org_text, encrypted, __path=None):
        _config = ConfigParser(
                        allow_no_value=True,
                        delimiters='=',
                        dict_type=DynamicDict,
                        converters={'*': CConfigParser.convert_value})
        _items = {'ENCRYPTED INFO': 
                dict(zip(['org_text', 'encrypted_text', 'decrypter_key'],
                        [org_text, encrypted.text, encrypted.key]))
                }
        
        _config.update(**dict(_items.items()))
        _path = DataLoader._validate_path(__path, True) or Path(__file__).parent.absolute() / 'encrypted_config.ini'
        _method = partial(_config.write) if not _path.is_file() else partial(_config.read)
        file = _method(_path)
        # if file!=_config:
        #     mode = 'a' if _path.is_file() else 'w'
        #     with open(_path, mode=mode) as c_file:
        #         _config.write(c_file)

class DynamicDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __missing__(self, __key):
        raise LoaderException(0,
            message=f"'{self.__class__.__name__}' has no attribute `{__key}`'")
    
    def __getattr__(self, __attr):
        assert __attr in self, self.__missing__(__attr)
        return self[__attr]
    
    def __setattr__(self, __attr, __value):
        self[__attr] = __value
    
    def __str__(self):
        return self.__repr__()
    
    @_recursive_repr()
    def __repr__(self):
        return '{}([{}])'.format(
            self.__class__.__name__,
            f',\n{' ':>{len(self.__class__.__name__)+2}}'.join(
            [f'({k}, {self._too_large(v)})' for k, v in self.items()]
            )
        )
    
    @staticmethod
    def _too_large(value, max_length=50):
        ellipsis = '...'
        try:
            length = len(value)
        except TypeError:
            length = None

        if (length is not None) and (length >= max_length):
            return ellipsis
        elif hasattr(value, '__str__') and len(str(value)) >= max_length:
            return ellipsis
        else:
            return value

    def to_dict(self, __cls=OrderedDict):
        return __cls(self)
    
    @property
    def to_dynamic(self):
        return DynamicDict(self)

class LoaderException(Exception):
    def __init__(self, *args, message=None, _log_method=logging.error) -> None:
        self.__message = message
        self._log_method = _log_method
        self.error_message = self.match_error(*args)
        super().__init__(self.error_message)
        self._log_error(*args)

    def __str__(self):
        return self.error_message
    
    def match_error(self, *args):
        if self.__message:
            return self.__message
        
        __code, *__obj = args
        _human_error = _ERRORS[-1].format(__code)
        assert __code in _ERRORS, AttributeError(_human_error)
        
        if __code in _ERRORS:
            str_code = _ERRORS[__code]
            holders = (Path(obj).name if isinstance(obj, Path|str) \
                        else ' '.join(obj) if isinstance(obj, Iterable) \
                        else obj for obj in __obj)
            return str_code.format(*holders)
    
    def _log_error(self, *args):
        self._log_method(f' {self.match_error(*args)}')

@dataclass(slots=True)
class Extensions:
    
    _defaults: Any = field(init=False, default=None)
    _ALL: List = field(init=False, default=None)
    
    def __post_init__(self):
        ExtInfo = namedtuple('ExtInfo', ['suffix_', 'loader_'])
        _mimetypes = mimetypes.types_map
        _mimetypes['xlsx'] = None
        _defaults = {
                'csv': ExtInfo('csv', lambda path, **kwargs: pd.read_csv(path, **kwargs)),
                'hdf': ExtInfo('hdf', lambda path, **kwargs: pd.read_hdf(path, **kwargs)),
                'json': ExtInfo('json', lambda path, **kwargs: json.load(open(path, **kwargs))),
                'pdf': ExtInfo('pdf', lambda path, **kwargs: extract_pages(path, **kwargs)),
                'sql': ExtInfo('sql', lambda path, **kwargs: pd.read_sql(path, **kwargs)),
                'txt': ExtInfo('txt', lambda path, **kwargs: open(path, **kwargs).read()),
                'xml': ExtInfo('xml', lambda path, **kwargs: pd.read_xml(path, **kwargs)),
                'empty': ExtInfo('', lambda path, **kwargs: open(path, **kwargs).read().splitlines())
                }
        self._defaults = (i.suffix_ for i in _defaults.values() if i.suffix_)
        _all_exts = set(ExtInfo(rm_p(i), pd.read_excel if rm_p(i) in ['xls','xlsx'] else open) \
                        for i in _mimetypes \
                        if rm_p(i) not in self._defaults)
        self._ALL = {ext.suffix_: ext for ext in _all_exts}
        self._ALL.update(**_defaults)

EXTENSIONS = Extensions()

class DataLoader:
    __files = None
    __index = 0
    
    def __init__(self, ext_path=None, ext_defaults=None, **kwargs) -> None:
        self.ext_path = ext_path
        self.ext_defaults = ext_defaults
        self.kwargs = kwargs
        self._validate_args()
    
    def __getitem__(self, __item):
        assert self.get(__item) is not None, LoaderException(0, message=f'{__item} is not found.')
        return self.get(__item)
    
    def __getattr__(self, __item):
        return self.__getitem__(__item)
    
    def __len__(self):
        return 0 if self.files is None else len(self.files)
    
    def __iter__(self):
        assert len(self.files), LoaderException(0, '')
        return iter(self.files)
    
    def __next__(self):
        assert self.__index >= len(self), StopIteration
        file_name = self.files[self.__index]
        self.__index += 1
        return file_name
    
    def __str__(self):
        return self.__repr__()

    @_recursive_repr()
    def __repr__(self):
        assert len(self.files), f'{self.__class__.__name__}([])'
        return '{}([{}])'.format(
                        self.__class__.__name__,
                        f',\n{' ':>{len(self.__class__.__name__)+2}}'.join(
                        [f'({file}, {DynamicDict._too_large(content)})' for file, content in self.files.items()])
                        )

    def __contains__(self, __item):
        return __item in self.files
    
    def get(self, __key, __default=None):
        if __key and __key in self.files:
            return self.files[__key]
        return __default
    
    def _validate_args(self):
        __all = self.kwargs.get('all_', False)
        __args = [self.ext_defaults, __all]
        
        assert all(__args) is False, LoaderException(810, '')
        
        self.__all = __all
        self.ext_path = self._validate_path(self.ext_path) if self.ext_path is not None else None
        self.ext_defaults = self._validate_exts(self.ext_defaults) if self.ext_defaults is not None else None
    
    @cached_property
    def _get_files(self):
        _defaults = EXTENSIONS._defaults
        if self.ext_defaults:
            _defaults = [ext for ext in self._validate_exts(self.ext_defaults)]
        elif self.__all:
            _defaults = EXTENSIONS._ALL['empty'].suffix_
        return self.get_files(self.ext_path, _defaults)
    
    @staticmethod
    def get_files(__path, __defaults=EXTENSIONS._defaults):
        _no_dirs = lambda _path: _path.is_file() and not _path.is_dir()
        _ext_pat = partial(compiler, __defaults)
        return (i for i in __path.glob('*') \
                if _ext_pat(rm_p(i.suffix)) \
                and DataLoader._validate_path(i) \
                and _no_dirs(i))
    
    @staticmethod
    def _validate_exts(__exts):
        __valid_exts = [rm_p(ext) \
                        for ext in __exts \
                        if rm_p(ext) in EXTENSIONS._ALL]
        
        __failed = list(filterfalse(lambda ext: ext in __valid_exts, __exts))
        _s = '{}'.format('s' if len(__failed)>1 else '')
        _all_exts = sorted(filterfalse(lambda i: i=='empty', EXTENSIONS._ALL.keys()))
        assert len(__valid_exts), LoaderException(0,
                                message=f'All provided default extension{_s} are invalid:\n{__failed} \
                                        \nAll available extensions:\n{_all_exts}')
        
        if __failed:
            LoaderException(0, message=f'The extensions provided are invalid and will be skipped: {__failed}', _log_method=logging.warning)
        return iter(__valid_exts)
    
    @staticmethod
    def _validate_path(__path, __raise=False):
        def _raise(_exception):
            assert __raise is False, _exception
        
        path = Path(__path)
        
        if not path:
            _raise(LoaderException(800, path))
            return None
        
        elif not path.exists():
            _raise(LoaderException(404, path))
            return None
        
        elif (not path.is_file()) \
            and (not path.is_dir()) \
            and (not path.is_absolute()):
            _raise(LoaderException(707, path))
            return None
        
        elif re.match(r'^[._]', path.stem):
            _raise(LoaderException(0, message=f'Skipping {path.stem}', _log_method=logging.warning))
            return None
        return path

    @staticmethod
    def _get_params(__method):
        try: 
            __sig = inspect.signature(__method)
        except TypeError:
            raise LoaderException(800, __method)
        return iter(__sig.parameters.keys())
    
    @staticmethod
    def _ext_method(__path):
        __suffix = rm_p(__path.suffix)
        __method = None
        __all = EXTENSIONS._ALL
        if __suffix==__all['empty'].suffix_:
            __method = __all['empty'].loader_
        elif __suffix in __all:
            __method = __all[__suffix].loader_
        else: raise LoaderException(702, __method)
        return __method
    
    def _check_ext(self, __path):
        __method = self._ext_method(__path)
        __method_params = self._get_params(__method)
        __kwargs = {param: value for param, value in self.kwargs.items() \
                    if param in __method_params \
                    and value is not None}
        return self._load_data(__path, __method, __kwargs)
    
    @staticmethod
    @cache
    def load_file(file_path, __kwargs=None):
        dl = DataLoader
        _path = dl._validate_path(file_path, True)
        p_method = dl._ext_method(_path)
        __kwargs = {} if __kwargs is None else __kwargs
        loaded_file = dl._load_data(_path, p_method, __kwargs)
        return loaded_file

    @staticmethod
    def _load_data(path, method, __kwargs):
        import numpy as np
        p_name = Path(path.parts[-1])
        p_contents = None
        FileInfo = namedtuple('FileInfo', ['name_', 'contents_'])
        _empty_key = f'{_ERRORS[607].format(p_name)} File will be skipped.'
        try:
            p_contents = method(path, **__kwargs)
        except PermissionError: raise LoaderException(13, p_name)
        except UnicodeDecodeError: raise LoaderException(100, p_name)
        except ParserError: raise LoaderException(303, p_name)
        except DtypeWarning: raise LoaderException(400, p_name)
        except OSError: raise LoaderException(400 ,p_name)
        except EmptyDataError:
            LoaderException(0, message=_empty_key,_log_method=logging.warning)
            p_contents = 0
        except JSONDecodeError as e: LoaderException(102, p_name, e.pos, e.lineno, e.colno, _log_method=logging.warning)
        except Exception as _e: raise LoaderException(500, f'{p_name}: {_e}')
        
        if p_contents is None:
            raise LoaderException(p_name, 530)
        elif isinstance(p_contents, int) and p_contents==0:
            return FileInfo(p_name, None)
        elif hasattr(p_contents, 'empty') and p_contents.empty:
            LoaderException(0, message=_empty_key, _log_method=logging.warning)
            return FileInfo(p_name, None)
        return FileInfo(p_name, p_contents)
    
    @cache
    def _execute_path(self, __files=None):
        with ThreadPoolExecutor(max_workers=max(1, cpu_count()-2)) as executor:
            _files = executor.map(self._check_ext, self._get_files)
        return {Path(file.name_).stem: file.contents_ for file in _files if file.contents_ is not None}
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        return self.__files
    
    def __add__(self, *__files):
        return self.add(*__files)
    
    @staticmethod
    def add_files(*__files):
        dl = DataLoader
        files = map(Path, __files)
        loaded_files = (dl.load_file(file) for file in \
                        (dl._validate_path(path) for path in files))
        return {Path(file.name_).stem: file.contents_ for file in loaded_files if file.contents_ is not None}
    
    @staticmethod
    def add_dirs(*__dirs, __merge=False):
        dl = DataLoader
        directories = map(Path, __dirs)
        if not __merge:
            loaded_directories = (dl(i) for i in directories)
            return DynamicDict({i.ext_path.stem: i for i in loaded_directories})
        loaded_directories = (dl.load_file(j) for i in directories for j in dl.get_files(i))
        return DynamicDict({p.name_.stem: p.contents_ for p in loaded_directories if p.contents_ is not None})
    
    @staticmethod
    def load_sql(__database=''):
        class DataSQL: pass
        return DataSQL
    
    @staticmethod
    def config_info(**kwargs):
        if kwargs.pop('instance_only', False):
            return ConfigManager
        return ConfigManager(**kwargs)
    
@dataclass(kw_only=True)
class ConfigManager:
    config_ini: Any = None
    sections: Any = None
    config: Any = field(init=False, repr=False, default_factory=DynamicDict)
    _sql_keys: Any = field(init=True, repr=False, default=None)
    _encrypt: Any = field(init=True,
                        default=False,
                        repr=False)
    
    _ini_name: Any = field(init=False, default=None)
    _config_parser: Any = field(init=False,
                                repr=False,
                                default_factory=lambda: CConfigParser(
                                                            allow_no_value=True,
                                                            dict_type=DynamicDict))
    
    def __post_init__(self):
        self._sql_keys = self._create_sql_config(True)
        self.config_ini = self._validate_config()
        self.config = self._get_config()
    
    def _validate_config(self):
        _ini = self.config_ini
        if not _ini:
            raise LoaderException(880, _ini)
        elif _ini and _ini.lower() == 'sql':
            return self._create_sql_config()
        return DataLoader._validate_path(_ini)
    
    def _create_sql_config(self, __sections_only=False):
        _ini_name = Path('sql_config.ini')
        __config = self._config_parser
        _generic = {
                'host': None,
                'dbname': None,
                'user': None,
                'password': None
            }
        _other = {'database': None}
        _mysql = {
                **{k:v for k,v in _generic.items() if k!='user'},
                'username': None
                }
        
        sections = {
                    'MySQL': _mysql,
                    'PostgreSQL': _generic, 'SQLAlchemy': _generic,
                    'Oracle': _generic, 'IBM DB2': _generic,
                    'Microsoft SQL Server': _generic,
                    'Amazon Redshift': _generic,
                    'SQLite': _other, 'MariaDB': _other
                    }
        
        if __sections_only:
            return list(sections)
        
        for section, values in sections.items():
            __config[section] = dict(values.items())
        
        if not _ini_name.is_file():
            with open(_ini_name, mode='w') as sql_file:
                __config.write(sql_file)
        
        self._update_attrs(_ini_name=_ini_name.stem)
        return __config

    def _get_config(self):
        CC = CConfigParser
        config_parser = self._config_parser
        config_parser.read(self.config_ini)
        _ini_sections = list(filter(lambda __key: __key!='DEFAULT', config_parser))
        __name = Path(self._ini_name) if self._ini_name else Path(self.config_ini)
        
        def _clean_config():
            _has_nulls = lambda __key, __method=all: __method(CC.convert_value(val) for val in \
                                                        dict(config_parser.items(__key)).values())
            _config = {
                        key: DynamicDict(
                                {k: CC.encrypt_text(v) if compiler(_PASS, k) and self._encrypt \
                                else v for k, v in config_parser.items(key)})
                                for key in _ini_sections if _has_nulls(key, any)
                    }
            if not _config:
                raise LoaderException(404, __name)
            elif self.sections:
                try:
                    for _ in self.sections:
                        config_parser.items(_)
                except NoSectionError:
                    if isinstance(self.sections, str):
                        self.sections = [self.sections]
                    _possible_keys = list(filter(compiler, _ini_sections))
                    if _possible_keys:
                        good = {ini_key: _config.get(ini_key) for ini_key in _possible_keys if _has_nulls(ini_key)}
                        bad = list(filter(lambda _bad: not _has_nulls(_bad), _possible_keys))
                        if not any(good):
                            LoaderException(1001, self.sections, _possible_keys)
                        elif bad:
                            LoaderException(1002, ', '.join(bad), _log_method=logging.warning)
                        return good
                    elif not _possible_keys:
                        LoaderException(890, ', '.join(self.sections), _ini_sections)
                        sys.exit(0)
            return DynamicDict(_config)
        
        if __name and __name.stem=='sql_config':
            LoaderException(7, __name, _log_method=logging.info)
            self._update_attrs(__name.absolute(), _ini_sections, __name.stem)
            return self
        
        self._update_attrs(__name.absolute(), _ini_sections, __name.stem)
        return _clean_config()

    def _update_attrs(self, config_ini=None, sections=None, _ini_name=None):
        self.__dict__.update(**dict(zip(DataLoader._get_params(self._update_attrs),
                                        [config_ini or self.config_ini,
                                        sections or self.sections,
                                        _ini_name or self._ini_name
                                        ])))
    
    def _update_sql_values(self):
        print(self._sql_keys)
        #! Depending on section key, loop through and give input() for each value depening on section
        return self.sections
