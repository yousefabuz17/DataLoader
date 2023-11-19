import logging
import re
from os import cpu_count
from collections import OrderedDict, namedtuple
from configparser import ConfigParser, NoSectionError, MissingSectionHeaderError
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache
from itertools import filterfalse
from pathlib import Path
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView, Iterable,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from constants import _PASS, _ERRORS
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr as _recursive_repr


logging.basicConfig(level=logging.INFO)

rm_p = lambda __i: __i.lstrip('.').lower()
_s = lambda __i: '{}'.format('s' if len(__i)>1 else '')
compiler = lambda __defaults, __k: \
            re.compile('|'.join(map(re.escape, __defaults)), re.IGNORECASE).search(__k if isinstance(__k, str) \
                                                                                    else '|'.join(map(re.escape, __k)))

ExtInfo = lambda typename='ExtInfo', \
                field_names=('suffix_', 'loader_'), \
                defaults=(None,)*2, \
                **kwargs: namedtuple(typename,
                                    field_names,
                                    defaults=defaults,
                                    **kwargs)

dl_executor = ThreadPoolExecutor(
                                thread_name_prefix='DataLoader(DL)',
                                max_workers=min(cpu_count() * 2, 32)
                                )
_dl_initializer = lambda *_: print(f'DataLoader(DL) Initialized {dl_executor._max_workers} worker DL threads...\n')

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
    
    @_recursive_repr()
    def __repr__(self):
        __cls = self.__class__.__name__
        if not self:
            return f'{__cls}([])'
        return '{}([{}])'.format(
                        __cls,
                        f',\n{' ':>{len(__cls)+2}}'.join(
                        [f'({k}, {DynamicDict._too_large(v)})' for k, v in self.items()]
                        )
                    )
    
    __str__ = __repr__
    
    @staticmethod
    def _too_large(value, max_length=100):
        ellipsis = '...'
        try:
            length = len(value)
        except TypeError:
            length = None

        if (length is not None) and (length >= max_length):
            return ellipsis
        elif hasattr(value, '__str__') \
            and len(str(value)) >= max_length:
            
            return ellipsis
        
        return value

    def to_dict(self, dict_type=OrderedDict):
        return dict_type(self)
    
    @property
    def to_dynamic(self):
        return DynamicDict(self)

class CConfigParser(ConfigParser):
    dict_type = DynamicDict
    
    def __init__(self, *args, dict_type=None, **kwargs):
        self.dict_type = dict_type or self.dict_type
        super().__init__(*args, **kwargs)

    def get(self, section, option, *, raw=False, vars=None, fallback=None):
        value = super().get(section, option, raw=raw, vars=vars, fallback=fallback)
        return self.convert_value(value)

    @classmethod
    def convert_value(cls, value):
        _value = str(value).lower()
        _vals = {'true': True, 'false': False, 'none': None}
        return _vals.get(_value, value)
    
    @classmethod
    def encrypt_text(cls, text, ini_name='config', export=False):
        global Fernet
        from cryptography.fernet import Fernet
        
        Encrypter = ExtInfo('Encrypter', ['text', 'key'])
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        encrypted_bytes = cipher_suite.encrypt(text.encode())
        encrypted_text = encrypted_bytes.hex()
        encrypted_data = Encrypter(encrypted_text, key)
        if export:
            cls._exporter(text, encrypted_data, ini_name)
            return encrypted_data
        
        return encrypted_data

    @classmethod
    def decrypt_text(cls, encrypted_text, key):
        cipher_suite = Fernet(key)
        encrypted_bytes = bytes.fromhex(encrypted_text)
        decrypted_message = cipher_suite.decrypt(encrypted_bytes).decode()
        return decrypted_message
    
    @classmethod
    def _exporter(cls, org_text, encrypted, /, ini_name='config', *, refresh=False, __path=None):
        _config_parser = _new_config()
        _items = {'ENCRYPTED_DATA': 
                dict(zip(('ORIGINAL_TEXT', 'ENCRYPTED_TEXT', 'DECRYPTER_KEY'),
                        (org_text, encrypted.text, encrypted.key)))
                }
        
        _config_parser.update(**_items)
        _path = Path(f'encrypted_{ini_name}.ini') if not __path else DataLoader._validate_path(__path)
        if not _path.is_file() or refresh:
            with open(_path, mode='w') as c_file:
                _config_parser.write(c_file)
            LoaderException(0, message=f'`{_path}` has been successfully created.', _log_method=logging.info)
        return _config_parser

class LoaderException(BaseException):
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
            holders = [Path(obj).name if isinstance(obj, Path|str) \
                        else ' '.join(obj) if isinstance(obj, Iterable) \
                        else obj for obj in __obj]
            return str_code.format(*holders)
    
    def _log_error(self, *args):
        self._log_method(f' {self.match_error(*args)}')

@dataclass(slots=True, weakref_slot=True)
class Extensions:
    
    _defaults: Any = field(init=False, default_factory=lambda: Extensions.__defaults__)
    _ALL: List = field(init=False, default=None)
    
    def __post_init__(self):
        _defaults = self.__defaults__
        _mimetypes = mimetypes.types_map
        _mimetypes['xlsx'] = None
        _all_exts = set(ExtInfo()(rm_p(i), pd.read_excel if rm_p(i) in ['xls','xlsx'] else open) \
                        for i in _mimetypes \
                        if rm_p(i) not in _defaults)
        self._defaults = [ext for ext in _defaults if ext!='empty']
        self._ALL = {**{ext.suffix_: ext for ext in _all_exts},
                     **_defaults}
    
    @property
    def __defaults__(self):
        global pd, mimetypes
        import json
        import mimetypes
        import pandas as pd
        from pdfminer.high_level import extract_pages
        return {
                'csv': ExtInfo(module='Extensions')('csv', pd.read_csv),
                'hdf': ExtInfo(module='Extensions')('hdf', pd.read_hdf),
                'pdf': ExtInfo(module='Extensions')('pdf', extract_pages),
                'sql': ExtInfo(module='Extensions')('sql', pd.read_sql),
                'xml': ExtInfo(module='Extensions')('xml', pd.read_xml),
                'json': ExtInfo(module='Extensions')('json', lambda path, **kwargs: json.load(open(path, **kwargs), **kwargs)),
                'txt': ExtInfo(module='Extensions')('txt', lambda path, **kwargs: open(path, **kwargs).read()),
                'empty': ExtInfo(module='Extensions')('', lambda path, **kwargs: open(path, **kwargs).read().splitlines())
                }

    def __repr__(self):
        return f'{self.__class__.__name__}([{', '.join(f'{i.name}={list(getattr(self, i.name))}' for i in fields(self))}])'
    
    __str__ = __repr__

EXTENSIONS = Extensions()
global _new_config
_new_config = lambda: CConfigParser(allow_no_value=True,
                                    delimiters='=',
                                    dict_type=DynamicDict,
                                    converters={'*': CConfigParser.convert_value}
                                    )

class DataLoader:
    __DEFAULTS = EXTENSIONS._defaults
    __ALL = EXTENSIONS._ALL
    
    def __init__(self, ext_path=None, ext_defaults=None, all_=False, **kwargs) -> None:
        self.ext_path = ext_path
        self.ext_defaults = ext_defaults
        self.all_ = all_
        self.kwargs = kwargs
        self._validate_args()
    
    def __getitem__(self, __item):
        assert self.get(__item) is not None, LoaderException(0, message=f'{__item} is not a valid file name.')
        return self.get(__item)
    
    def __getattr__(self, __item):
        return self.get(__item)
    
    def __len__(self):
        return 0 if self.files is None else len(self.files)
    
    def __iter__(self):
        assert len(self.files), LoaderException(0)
        return iter(self.files)
    
    def __next__(self):
        assert self.__index >= len(self), StopIteration
        file_name = self.files[self.__index]
        self.__index += 1
        return file_name
    
    def __bool__(self):
        assert len(self.files), LoaderException(0)
        return True if len(self.files)>=1 else False
    
    def __repr__(self):
        return DynamicDict.__repr__(self.files)
    
    def __contains__(self, __item):
        return __item in self.files
    
    def add(cls, *__files, **kwargs):
        return cls.add_files(*__files, **kwargs)
    
    def mul(cls, *__dirs, **kwargs):
        return cls.add_dirs(*__dirs, **kwargs)
    
    def get(self, __key=None, __default=None):
        assert __key is not None, LoaderException(222)
        if __key in self.files:
            return self.files[__key]
        return __default
    
    def _validate_args(self):
        assert any((self.ext_path, self.ext_defaults)), \
                LoaderException(200, self.__class__.__name__)
        
        assert all((self.ext_defaults, self.all_)) is False, \
                LoaderException(202, self.__class__.__name__, self.__DEFAULTS)
        
        self.__files = None
        self.__index = 0
        self.ext_path = self._validate_path(self.ext_path, True) if self.ext_path is not None else None
        self.ext_defaults = self._validate_exts(self.ext_defaults) if self.ext_defaults is not None else None
    
    @cached_property
    def _get_files(self):
        _defaults = self.__DEFAULTS
        if self.ext_defaults:
            _defaults = [ext for ext in self._validate_exts(self.ext_defaults)]
        elif self.all_:
            _defaults = self.__ALL['empty'].suffix_
        
        return self.get_files(self.ext_path, _defaults)
    
    def get_files(self, __path, __defaults=EXTENSIONS._defaults):
        _no_dirs = lambda _path: _path.is_file() and not _path.is_dir()
        _ext_pat = partial(compiler, __defaults)
        return (i for i in __path.glob('*') \
                if _ext_pat(rm_p(i.suffix)) \
                and self._validate_path(i) \
                and _no_dirs(i))
    
    def _validate_exts(self, __exts):
        __valid_exts = (rm_p(ext) \
                        for ext in __exts \
                        if rm_p(ext) in self.__ALL)
        
        __failed = list(filterfalse(lambda ext: ext in __valid_exts, __exts))
        assert iter(__valid_exts), LoaderException(210, _s(__failed), __failed, list(self.__ALL))
        
        if __failed:
            LoaderException(215, __failed, _log_method=logging.warning)
        return __valid_exts
    
    @staticmethod
    def _validate_path(__path, __raise=False):
        def _raise(_exception):
            assert not __raise, _exception
        
        try:
            path = Path(__path)
        except TypeError as t_error:
            raise LoaderException(230, __path, t_error)
        
        if not path:
            _raise(LoaderException(800, path))
            return
        
        elif not path.exists():
            _raise(LoaderException(404, path))
            return
        
        elif (not path.is_file()) \
            and (not path.is_dir()) \
            and (not path.is_absolute()):
            _raise(LoaderException(707, path))
            return
        
        elif compiler(['^\\.', '^\\_'], path.stem):
            _raise(LoaderException(0, message=f'Skipping {path.stem}', _log_method=logging.warning))
            return
        
        return path

    @classmethod
    def _get_params(cls, __method):
        import inspect
        try: 
            __sig = inspect.signature(__method)
        except TypeError:
            raise LoaderException(800, __method)
        return iter(__sig.parameters)
    
    def _ext_method(self, __path):
        __suffix = rm_p(__path.suffix)
        __method = None
        __all = self.__ALL
        if __suffix==__all['empty'].suffix_:
            __method = __all['empty'].loader_
        elif __suffix in __all:
            __method = __all[__suffix].loader_
        else: raise LoaderException(702, __method)
        return __method
    
    def _check_ext(self, __path):
        __method = self._ext_method(__path)
        __kwargs = {param: value for param, value in self.kwargs.items() \
                    if param in self._get_params(__method) \
                    and value is not None}
        return self._load_data(__path, __method, __kwargs)
    
    @classmethod
    @cache
    def load_file(cls, file_path, **__kwargs):
        _path = cls._validate_path(file_path, True)
        p_method = cls._ext_method(cls, _path)
        __kwargs = {} if __kwargs is None else __kwargs
        loaded_file = cls._load_data(cls, _path, p_method, __kwargs)
        return loaded_file

    def _load_data(self, path, method, __kwargs):
        from json.decoder import JSONDecodeError
        from pandas.errors import ParserError, DtypeWarning, EmptyDataError
        
        p_name = Path(path.parts[-1])
        p_contents = None
        FileInfo = ExtInfo(typename='FileInfo',
                            field_names=('name_', 'contents_'),
                            module='DataLoader')
        __errors = dict(zip((PermissionError, UnicodeDecodeError,
                            ParserError, DtypeWarning, OSError, 
                            EmptyDataError, JSONDecodeError, Exception),
                            (13, 100, 303, 400, 402, 607, 102, 500)))
        try:
            p_contents = method(path, **__kwargs)
        except tuple(__errors) as _error:
            _exception = type(_error)
            error_code = __errors.get(_exception, 500)
            _placeholder_count = _ERRORS.get(error_code, _ERRORS[-1]).count('{}')
            __raise = partial(LoaderException, error_code, p_name, _log_method=logging.warning)
            if _exception == JSONDecodeError:
                __raise(_error.pos, _error.lineno, _error.colno)
            elif _placeholder_count==2:
                __raise(_error)
            else:
                __raise()
            p_contents = 0
        
        if (isinstance(p_contents, int) and p_contents==0) \
            or (hasattr(p_contents, 'empty') and p_contents.empty):
            return FileInfo(p_name, None)
        
        return FileInfo(p_name, p_contents)
    
    @cache
    def _execute_path(self):
        dl_executor._initializer = _dl_initializer()
        _files = dl_executor.map(self._check_ext, self._get_files)
        return {Path(file.name_).stem: file.contents_ for file in _files if file.contents_ is not None}
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        return self.__files
    
    @classmethod
    def add_files(cls, *__files, **kwargs):
        assert len(__files)>=1, LoaderException(220)
        loaded_files = dl_executor.map(partial(cls.load_file, **kwargs), (cls._validate_path(path) \
                        for path in map(cls._validate_path, __files)))
        return {Path(file.name_).stem: file.contents_ for file in loaded_files if file.contents_ is not None}
    
    @classmethod
    def add_dirs(cls, *__dirs, __merge=False, **kwargs):
        assert len(__dirs)>=1, LoaderException(220)
        directories = (cls._validate_path(_path) for _path in __dirs)
        
        if not __merge:
            loaded_directories = dl_executor.map(partial(cls, **kwargs), directories)
            return DynamicDict({i.ext_path.stem: i for i in loaded_directories})
        
        loaded_directories = (cls.load_file(j, **kwargs) for i in directories for j in cls.get_files(i))
        return DynamicDict({p.name_.stem: p.contents_ for p in loaded_directories if p.contents_ is not None})
    
    @staticmethod
    def load_sql(__database=''):
        class DataSQL: pass
        return DataSQL
    
    @staticmethod
    def load_config(**kwargs):
        if kwargs.pop('instance_only', False):
            return ConfigManager
        return ConfigManager(**kwargs)

@dataclass(order=True)
class ConfigManager:
    config_ini: Any = None
    sections: Any = field(init=True,
                        default_factory=lambda: [f'Section{i}' for i in range(1,4)]
                        )
                        
    encrypt: Any = field(init=True,
                        default=True,
                        repr=False,
                        kw_only=True)
                        
    config: Any = field(init=False,
                        repr=False,
                        default_factory=DynamicDict)
    
    _sql_keys: Any = field(init=False,
                        repr=False,
                        default_factory=lambda: ConfigManager._sql_config_sections)
    
    _ini_name: Any = field(init=False, default='db_config')
    _config_parser: Any = field(init=False,
                                repr=False,
                                default_factory=lambda: _new_config())
    
    def __post_init__(self):
        self.config_ini = self._validate_config()
        self.config = self._get_config()
    
    def _map_ini_suffix(self, __path):
        self.config_ini = Path(__path).with_suffix('.ini')
    
    def _validate_config(self):
        _ini = self.config_ini
        assert _ini is not None, LoaderException(880, _ini)
        
        return DataLoader._validate_path(_ini, True)
    
    @staticmethod
    def _sql_config_sections():
        return ConfigManager.create_sql_config(sections_only=True)
    
    @staticmethod
    def create_sql_config(__ini_name='sql_config', sections_only=False):
        _ini_name = Path(__ini_name).with_suffix('.ini')
        _config_parser = _new_config()
        
        _generic = {
                'host': 'None',
                'dbname': 'None',
                'user': 'None',
                'password': 'None'
            }
        
        _other = {'database': 'None'}
        
        _mysql = {
                **{k:v for k,v in _generic.items() if k!='user'},
                'username': 'None'
                }
        
        sections = {
                    'MySQL': _mysql,
                    'PostgreSQL': _generic, 'SQLAlchemy': _generic,
                    'Oracle': _generic, 'IBM DB2': _generic,
                    'Microsoft SQL Server': _generic,
                    'Amazon Redshift': _generic,
                    'SQLite': _other, 'MariaDB': _other
                    }
        
        if sections_only:
            return list(sections)
        
        _config_parser.update({section: 
                            dict(value.items()) for section, value in sections.items()})
        def _write_file():
            with open(_ini_name, mode='w', encoding='utf-8') as sql_file:
                _config_parser.write(sql_file)
            LoaderException(7, _ini_name, _log_method=logging.info)
            LoaderException(0, message=f'`{_ini_name}`: {repr(sections)}', _log_method=logging.info)
            return
        __input = f'The file `{_ini_name}` already exists. Overwriting will simply empty it out with null values. Proceed (N/y)? '
        if _ini_name.is_file():
            if compiler(['yes', 'y', '1'], input(__input)):
                return _write_file()
            LoaderException(0, message=f'[TERMINATED] {_ini_name} has not been overwritten.', _log_method=logging.info)
            return
        
        return _write_file()

    def _get_config(self):
        CC = self._config_parser
        CC.read(self.config_ini)
        _ini_sections = list(filterfalse(lambda __key: __key=='DEFAULT', CC))
        __name = Path(self.config_ini)
        _has_nulls = lambda __key, __method=all: __method(CC.convert_value(val) for val in \
                                                        dict(CC.items(__key)).values())
        def _clean_config():
            _config = {
                        key: 
                            {k: CC.encrypt_text(v, __name.stem, True) if all((compiler(_PASS, k), self.encrypt, CC.convert_value(v) is not None)) \
                            else v for k, v in CC.items(key)}
                            for key in _ini_sections
                    }
            assert len(_config), LoaderException(899, __name.name)
            
            __section_defaults = list(*(i.default_factory() for i in fields(self) if i.name=='sections'))
            if self.sections!=__section_defaults:
                try:
                    for _ in self.sections:
                        CC.items(_)
                except NoSectionError:
                    if isinstance(self.sections, str):
                        self.sections = [self.sections]
                    _db_pat = partial(compiler, self.sections)
                    _possible_keys = list(filter(_db_pat, _ini_sections))
                    if _possible_keys:
                        good = {ini_key: _config.get(ini_key) for ini_key in _possible_keys if _has_nulls(ini_key)}
                        good_has_nulls = list(filterfalse(_has_nulls, _possible_keys))
                        __none = lambda i: all((compiler(good, i), compiler(good_has_nulls, i)))
                        __bad = list(filter(__none, self.sections))
                        assert good, LoaderException(1003, self.sections,  _possible_keys)
                        if __bad:
                            LoaderException(0, message=f'Invalid section{_s(__bad)}, skipping: {__bad}', _log_method=logging.warning)
                        
                        if good_has_nulls:
                            LoaderException(1002, good_has_nulls, _log_method=logging.warning)
                        
                        self._update_attrs(__name.absolute(), list(good), __name.stem)
                        return good
                    
                    assert len(_possible_keys), LoaderException(890, self.sections, _ini_sections)
            self._update_attrs(__name.absolute(), list(filter(_has_nulls, _ini_sections)),__name.stem)
            return _config
        return _clean_config()

    def _update_attrs(self, config_ini=None, sections=None, _ini_name=None):
        _config_params = DataLoader._get_params(self._update_attrs)
        self.__dict__.update(**dict(zip(_config_params,
                                        (config_ini,
                                        sections,
                                        _ini_name))))
    
    def _update_config(self, __section=None, __sources=None, encrypt=True):
        __sources = {} if __sources is None else __sources
        __config = dict((k, self._config_parser.encrypt_text(v, self._ini_name) if all((compiler(_PASS, k), encrypt)) else v) for k,v in __sources.items())
        if __section:
            self.config[__section].update(**__config)
        return self.config.update(**__config)
