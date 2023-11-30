import os
import re
import sys
import json
import hashlib
import logging
import threading
import mimetypes
import pandas as pd
from pathlib import Path
from copy import deepcopy
from pdfminer.high_level import extract_pages
from collections import OrderedDict, namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache, wraps
from itertools import filterfalse, chain, count
from configparser import ConfigParser, NoSectionError, MissingSectionHeaderError
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView, Iterable,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from json.decoder import JSONDecodeError
from pandas.errors import ParserError, DtypeWarning, EmptyDataError
from constants import _PASS, _ERRORS
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr as _recursive_repr
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='[%(asctime)s][LOG %(levelname)s]:%(message)s', 
                            datefmt='%Y-%m-%d %I:%M:%S %p')
handler = logging.FileHandler(f'{Path(__file__).stem}.log', 'a')
handler.setFormatter(formatter)
logger.addHandler(handler)

rm_p = lambda __i: __i.lstrip('.').lower()
_s = lambda __i: '{}'.format('s' if (hasattr(__i, '__len__') and len(__i)>1 or __i>1) else '')

compiler = lambda __defaults, __k: \
            re.compile('|'.join(map(re.escape, __defaults)), re.IGNORECASE) \
            .match(__k if isinstance(__k, str) \
            else '|'.join(map(re.escape, __k)))

dl_executor = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) + 4))

#** Outside of ThreadPool to ensure it prints only once for each execution(path)
def _dl_initializer(*__path):
    dl_executor._thread_name_prefix = __thread_prefix = '(DL)DataLoaderExecutor'
    __thread_count = threading.active_count()
    __main_thread = threading.current_thread().name
    _repr = '\n{}: \033[1;32m`{}`\033[0m initialized {} {} worker{}\n'
    print(_repr.format(__thread_prefix, Path('/'.join(*__path)),
                        __thread_count, __main_thread, _s(__thread_count)))

class _Generic(ABC):
    @abstractmethod
    def __missing__(self, *args):
        raise DLoaderException(*args)
    
    @abstractmethod
    def _repr(self, __cls, __iter):
        __cls_name = (lambda _cls: _cls.capitalize() if not _cls[0].isupper() else _cls)(__cls.__class__.__name__)
        __iter_size = f'[{sys.getsizeof(__iter)} BYTES]'
        
        __items = ((k, self._too_large(v)) for k, v in __iter)
        __string = '\n{}([{}])'.format(
                                __cls_name,
                                f',\n{' ':>{len(__cls_name)+2}}'.join(
                                (f'({self.__key_name(k)}, {v})' for k, v in __items)
                                )
                            )
        return __iter_size + __string
    
    def __key_name(self, __path):
        try: return Path(__path).name
        except: return __path
    
    @staticmethod
    @abstractmethod
    def _validate_path(__path, __raise=False):
        def _raise(__exception):
            if __raise:
                raise __exception
        
        try:
            path = Path(__path)
        except TypeError as t_error:
            raise DLoaderException(230, __path, t_error)
        
        if not path:
            _raise(DLoaderException(800, path))
            return
        
        elif not path.exists():
            _raise(DLoaderException(404, path))
            return
        
        elif (not path.is_file()) \
            and (not path.is_dir()) \
            and (not path.is_absolute()):
            
            _raise(DLoaderException(707, path))
            return
        
        elif compiler(r'^[._]', path.stem):
            _raise(DLoaderException(0, message=f'Skipping {path.stem}', _log_method=logger.warning))
            return
        
        return path
    
    @abstractmethod
    def reset(cls):
        try:
            if isinstance(cls, DynamicDict):
                return OrderedDict(cls.items())
            elif isinstance(cls, DynamicGen):
                return ((*kv,) for kv in cls)
        except: return cls
    
    @staticmethod
    def _too_large(__value, __max_length=75):
        __cls = (lambda _cls: _cls.capitalize() if not _cls.istitle() else _cls)(__value.__class__.__name__)
        ellipsis = f'<{__cls}>'
        try:
            length = len(__value)
        except TypeError:
            length = None

        if (length is not None) and (length >= __max_length):
            return ellipsis
        elif hasattr(__value, '__str__') \
            and len(str(__value)) >= __max_length:
            
            return ellipsis
        
        elif isinstance(__value, Generator):
            return ellipsis
        
        return __value

class DynamicDict(OrderedDict, _Generic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __missing__(self,*args):
        super().__missing__(*args)
    
    @_recursive_repr()
    def __repr__(self):
        return super()._repr(self, self.items())
    
    __str__ = __repr__
    
    def __dir__(self):
        return set(super().__dir__() + [str(i) for i in self])
    
    def __getattr__(self, __item):
        return self[__item]
    
    def __getitem__(self, __item):
        if not self.__contains__(__item):
            if (_right_key:=self.__possible_key(__item)):
                return self.get(_right_key)
        
        return self.get(__item)
    
    def __setattr__(self, __attr, __value):
        self[__attr] = __value
    
    def _items_viewer(func):
        @wraps(func)
        def wrapper(self, *args, **kwarg):
            return self.__posix_converter(func(self, *args, **kwarg))
        return wrapper
    
    @_items_viewer
    def keys(self):
        return super().keys()
    
    def values(self):
        return super().values()
    
    @_items_viewer
    def items(self):
        return super().items()
    
    def __contains__(self, __key):
        return __key in self.keys()
    
    def __posix_converter(self, __items):
        if not all(isinstance(i, tuple|list) for i in __items):
            return list(map(lambda p: Path(p).name, __items))
        return list(map(lambda p: (Path(p[0]).name, p[1]), __items))
    
    def _repr(self, *args):
        return super()._repr(*args)
    
    def __possible_key(self, __key):
        try:
            return compiler(self.keys(), __key).group()
        except AttributeError:
            return
    
    def get(self, __key=None, __default=None):
        if (__key in ('', None)):
            return __default
        elif all((not __key, not __default)):
            return self.fromkeys((__key,))
        
        if self.__contains__(__key):
            for i,j in self.__posix_converter(self.items()):
                if i==__key:
                    return j
        
        if (_possible_key:=self.__possible_key(__key)):
            print(DLoaderException(221, __key, _possible_key, _log_method=logger.info))
        return __default

    def reset(cls):
        return super().reset()

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
    def encrypt_text(cls, text, ini_name='config', *, export=False):
        from cryptography.fernet import Fernet
        
        Encrypter = namedtuple('Encrypter', ['text', 'key'], module='CConfigParser')
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
        _Fernet = globals()['Fernet']
        cipher_suite = _Fernet(key)
        encrypted_bytes = bytes.fromhex(encrypted_text)
        decrypted_message = cipher_suite.decrypt(encrypted_bytes).decode()
        return decrypted_message
    
    @classmethod
    def _exporter(cls, org_text, encrypted, /, ini_name='config', *, refresh=False, __path=None):
        _config_parser = globals()['_NEW_CONFIG']()
        _items = {'ENCRYPTED_DATA': 
                dict(zip(('ORIGINAL_TEXT', 'ENCRYPTED_TEXT', 'DECRYPTER_KEY'),
                        (org_text, encrypted.text, encrypted.key)))
                }
        
        _config_parser.update(**_items)
        _path = Path(f'encrypted_{ini_name}.ini') if not __path else DataLoader._validate_path(__path)
        if not _path.is_file() or refresh:
            with open(_path, mode='w') as c_file:
                _config_parser.write(c_file)
            DLoaderException(0, message=f'`{_path}` has been successfully created.', _log_method=logger.info)
        return _config_parser

class DLoaderException(BaseException):
    def __init__(self, *args, message=None, _log_method=logger.error) -> None:
        self.__message = message
        self._log_method = _log_method
        self.__error_message = self.match_error(*args)
        super().__init__(self.__error_message)
        self._log_error(*args)

    def __str__(self):
        return self.__error_message
    
    def match_error(self, *args):
        if self.__message:
            return self.__message
        
        __code, *__obj = args
        _human_error = _ERRORS[-1000].format(__code)
        if __code not in _ERRORS:
            raise AttributeError(_human_error)
        
        if __code in _ERRORS:
            str_code = _ERRORS[__code]
            holders = (Path(obj).name if isinstance(obj, Path|str) \
                        else obj for obj in __obj)
            return str_code.format(*holders)
    
    def _log_error(self, *args):
        self._log_method(f'{self.match_error(*args)}')

@dataclass(slots=True, weakref_slot=True)
class Extensions:
    
    _defaults: Dict = field(init=False, default_factory=lambda: Extensions.__defaults__)
    _ALL: Dict = field(init=False, default=None)
    
    def __post_init__(self):
        _defaults = self.__defaults__
        _mimetypes = mimetypes.types_map
        _mimetypes['xlsx'] = None
        ExtInfo = self.__module
        _all_exts = set(ExtInfo(rm_p(i), pd.read_excel if rm_p(i) in ('xls','xlsx') else open) \
                        for i in _mimetypes \
                        if rm_p(i) not in _defaults)
        self._defaults = [ext for ext in _defaults if ext!='empty']
        self._ALL = {**{ext.suffix_: ext for ext in _all_exts},
                     **_defaults}
    
    @property
    def __defaults__(self):
        ExtInfo = self.__module
        return {
                'csv': ExtInfo('csv', pd.read_csv),
                'hdf': ExtInfo('hdf', pd.read_hdf),
                'pdf': ExtInfo('pdf', extract_pages),
                'sql': ExtInfo('sql', pd.read_sql),
                'xml': ExtInfo('xml', pd.read_xml),
                'json': ExtInfo('json', lambda path, **kwargs: json.load(open(path, **kwargs), **kwargs)),
                'txt': ExtInfo('txt', lambda path, **kwargs: open(path, **kwargs).read()),
                'empty': ExtInfo('', lambda path, **kwargs: open(path, **kwargs).read().splitlines())
                }

    def __repr__(self):
        return f'{self.__class__.__name__}([{', '.join('{}={}'.format(*i) for i in self.__getstate__().items())}])'
    
    __str__ = __repr__
    
    def __getstate__(self):
        return {i.name: list(getattr(self, i.name)) for i in fields(self)}
    
    @property
    def __module(self):
        return namedtuple('ExtInfo', ('suffix_', 'loader_'), module='Extensions')

EXTENSIONS = Extensions()
_NEW_CONFIG = lambda: CConfigParser(allow_no_value=True,
                                    delimiters='=',
                                    dict_type=DynamicDict,
                                    converters={'*': CConfigParser.convert_value}
                                    )

class DynamicGen(Iterable, _Generic):
    def __init__(self, __dict_gen):
        self.__dict_gen = __dict_gen
    
    def __missing__(self):
        super().__missing__(1, self.__class__.__name__,
                            *map(lambda __cls: getattr(__cls[1], '__name__'),
                            enumerate((DynamicGen, DynamicDict, DataLoader, DynamicGen)))
                            )
    
    def __repr__(self):
        return self._repr(self, self.__dict_gen)
    
    __str__ = __repr__
    
    def __dir__(self):
        return set(super().__dir__() + [str(i) for i,_j in self.__dict_gen])
    
    def __iter__(self):
        return iter(self.__dict_gen)
        
    def __len__(self):
        return 0 if not self.__dict_gen else sum(1 for __kv in self)
    
    def __bool__(self):
        return bool(self.__len__())
    
    def _repr(self, *args):
        return super()._repr(*args)
    
    @staticmethod
    def _validate_path(*args):
        return _Generic._validate_path(*args)
    
    def reset(cls):
        return super().reset()
    
    def _missing(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.__missing__()
        return wrapper
    
    @_missing
    def __getattr__(self):
        pass
    
    @_missing
    def __getitem__(self):
        pass
    
    @_missing
    def get(self):
        pass



class DataLoader(DynamicGen):
    _ID = count()
    _DEFAULTS = EXTENSIONS._defaults
    _ALL = EXTENSIONS._ALL
    _HASHED_FILES = {}
    _THREAD_LOCK = threading.Lock()
    _THREAD_EXECUTOR = dl_executor
    _KWARG_KEYS = frozenset(('all_', 'dynamic', 'no_method', 'posix'))
    
    def __init__(self, ext_path=None, ext_defaults=None, all_=False, **kwargs):
        self.ext_path = ext_path
        self.ext_defaults = ext_defaults
        self.all_ = all_
        self.kwargs = kwargs
        self.__post_init__()
    
    def __post_init__(self):
        if not self.ext_path:
            raise DLoaderException(200, self.__class__.__name__, self.ext_path)
        
        elif all((self.ext_defaults, self.all_)):
            raise DLoaderException(202, self.__class__.__name__, self._DEFAULTS)
        
        self.__files = None
        self.__ID = next(DataLoader._ID)
        self.ext_path = self._validate_path(self.ext_path, True)
        self.ext_defaults = self._validate_exts(self.ext_defaults)

    def __missing__(self, *args):
        super().__missing__(*args)
    
    def __repr__(self):
        return self._repr(self, self.files)
    
    __str__ = __repr__
    
    def __call__(self):
        if self.kwargs.get('dynamic') and not isinstance(self.files, DynamicDict):
            return DynamicDict(self.files)
        return DynamicGen(self.files)
    
    @cached_property
    def _get_files(self):
        _defaults = self._DEFAULTS
        if self.ext_defaults:
            _defaults = [ext for ext in self._validate_exts(self.ext_defaults)]
        elif self.all_:
            _defaults = self._ALL['empty'].suffix_
        return self.get_files(self.ext_path, _defaults)
    
    @classmethod
    def get_files(cls, __path, __defaults=None):
        _defaults = cls._DEFAULTS if __defaults is None else __defaults
        no_dirs = lambda _p: _p.is_file() and not _p.is_dir()
        _ext_pat = partial(compiler, _defaults)
        return (_p for _p in __path.iterdir() \
                if _ext_pat(rm_p(_p.suffix)) \
                and DataLoader._validate_path(_p) \
                and no_dirs(_p))
    
    def _validate_exts(self, __exts):
        if __exts is None:
            return
        
        _valid_exts = [rm_p(ext) \
                        for ext in __exts \
                        if rm_p(ext) in self._ALL]
        
        _failed = list(filterfalse(lambda ext: ext in _valid_exts, __exts))
        if len(_failed)==len(__exts):
            raise DLoaderException(210, _s(len(_failed)), _failed, list(self._ALL))
        
        if _failed:
            DLoaderException(215, _failed, _log_method=logger.warning)
        return _valid_exts
    
    @staticmethod
    def _validate_path(*args):
        return DynamicGen._validate_path(*args)
    
    @staticmethod
    def _get_params(__method):
        import inspect
        try: 
            _sig = inspect.signature(__method)
        except TypeError:
            raise DLoaderException(800, __method)
        return _sig.parameters.keys()
    
    def _ext_method(self, __path):
        _suffix = rm_p(__path.suffix)
        _method = None
        _all = self._ALL
        if _suffix==_all['empty'].suffix_:
            _method = _all['empty'].loader_
        elif _suffix in _all:
            _method = _all[_suffix].loader_
        else: raise DLoaderException(702, _method)
        return _method
    
    def _check_ext(self, __path):
        _method = self._ext_method(__path)
        _kwargs = {param: value for param, value in self.kwargs.items() \
                    if param in self._get_params(_method) \
                    and value is not None}
        if _kwargs:
            DLoaderException(-1, _log_method=logger.info)
        
        return self._load_file(__path, _method, _kwargs)
    
    @staticmethod
    def _get_type(__cls_files):
        for i in __cls_files:
            return type(i())
    
    def inject_files(self, *__dirs):
        if not len(__dirs)>=1:
            raise DLoaderException(150)
        self.id = f'{self.id}_injected'
        with self._THREAD_LOCK:
            _all_files = self._THREAD_EXECUTOR.map(partial(DataLoader, all_=self.all_, **self.kwargs), __dirs)
        _gen_files = ((i.ext_path, i()) for i in _all_files)
        
        
        _type = self._get_type(_all_files)
        
        if self.kwargs.get('dynamic') and _type==DynamicDict:
            for kv in _gen_files:
                self.files.update(**kv)
            self.files = DynamicDict(self.files)
            return self.files
        
        _files = chain(((k,v) for k,v in self.files if k not in _gen_files), _gen_files)
        self.files = DynamicGen(_files)
        return self.files
    
    @classmethod
    @cache
    def load_file(cls, file_path, **__kwargs):
        _path = cls._validate_path(file_path, True)
        p_method = cls._ext_method(cls, _path)
        __kwargs = {} if __kwargs is None else __kwargs
        loaded_file = cls._load_file(cls, _path, p_method, __kwargs)
        return loaded_file
    
    @cached_property
    def _all_errors(self):
        return dict(zip((PermissionError, UnicodeDecodeError,
                        ParserError, DtypeWarning, OSError, 
                        EmptyDataError, JSONDecodeError, Exception),
                        (13, 100, 303, 400, 402, 607, 102, 500)))

    def _load_file(self, path, method, __kwargs):
        
        p_name = Path(path.parts[-1])
        p_contents = None
        _kwargs = __kwargs if (not hasattr(self, 'kwargs') or __kwargs) else self.kwargs
        _org_kwargs = deepcopy(_kwargs)
        _module = _kwargs.pop('module', False)
        method = open if _kwargs.get('no_method') else method
        self._rm_kwargs(__kwargs)
        FileInfo = namedtuple('FileInfo', ('path_', 'contents_'),
                            defaults=[None]*2,
                            module='FileLoader')
        
        try:
            p_contents = method(path, **__kwargs)
        except tuple((_errors:=self._all_errors)) as _error:
            __exception = type(_error)
            __error_code = _errors.get(__exception, 500)
            __placeholder_count = _ERRORS.get(__error_code).count('{}')
            __raise = partial(DLoaderException, __error_code, p_name, _log_method=logger.warning)
            
            if __exception == JSONDecodeError:
                __raise(_error.pos, _error.lineno, _error.colno)
            elif __placeholder_count == 2:
                __raise(_error)
            else:
                __raise()
            
            p_contents = 0
        
        if (isinstance(p_contents, int) and p_contents==0) \
            or (hasattr(p_contents, 'empty') and p_contents.empty):
            return FileInfo(path)
        
        _id = self.id
        if not isinstance(_id, str):
            if _module:
                _id = f'{_module}-added'
            _id = f'ID_{str(id(self))[:4]}'
            
        if _id not in (_hashed:=self._HASHED_FILES):
            _hashed[_id] = []
        _hashed[_id].append({p_name: self.calculate_hash(path)})
        
        _FI = partial(FileInfo, contents_=p_contents)
        if _org_kwargs.get('posix'):
            return _FI(path_=path)
        
        return _FI(path_=p_name)
    
    @cache
    def _execute_path(self):
        self._THREAD_EXECUTOR._initializer = _dl_initializer(Path(self.ext_path).parts[-2:])
        try:
            with self._THREAD_LOCK:
                _files = self._THREAD_EXECUTOR.map(self._check_ext, self._get_files)
        except tuple(self._all_errors) as _error:
            raise DLoaderException(0, message=f'{_error}')
        
        return ((file.path_, file.contents_) for file in _files if file.contents_ is not None)
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        
        _cache = DynamicDict(self.__files)
        try:
            next(iter(self.__files))
        except StopIteration:
            self.__files = _cache.items()
        
        return self.__files
    
    @staticmethod
    def calculate_hash(__file_path):
        sha256_hash = hashlib.sha256()
        with open(__file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    @property
    def hashed_files(cls):
        print(f'Current module [{cls.id}]')
        return DynamicDict(cls._HASHED_FILES)
    
    @cached_property
    def id(self):
        return self.kwargs.get('module', f'ID_{self.__ID}')
    
    def reset(cls):
        return super().reset()
    
    @classmethod
    def add_files(cls, *__files, **__kwargs):
        if not len(__files)>=1:
            raise DLoaderException(220)
        kwargs = deepcopy(__kwargs)
        with cls._THREAD_LOCK:
            loaded_files = cls._THREAD_EXECUTOR.map(partial(cls.load_file, **__kwargs),
                                                (cls._validate_path(path) for path in map(cls._validate_path, __files)))
            
        __files = ((file.path_, file.contents_) for file in loaded_files if file.contents_ is not None)
        if kwargs.get('dynamic') and not isinstance(cls, DynamicDict):
            return DynamicDict(__files)
        return DynamicGen(__files)
    
    @staticmethod
    def _rm_kwargs(__kwargs):
        for i in DataLoader._KWARG_KEYS:
            __kwargs.pop(i, False)
    
    @classmethod
    def add_dirs(cls, *__dirs, **__kwargs):
        if not len(__dirs)>=1:
            raise DLoaderException(220)
        
        __directories = map(cls._validate_path, __dirs)
        __dynamic = __kwargs.pop('dynamic', False)
        __merge = __kwargs.pop('merge', False)
        if __merge:
            with cls._THREAD_LOCK:
                loaded_directories = (cls.load_file(j, **__kwargs) \
                                    for i in cls._THREAD_EXECUTOR.map(cls.get_files, __directories) \
                                    for j in i)
            
            __files = ((p.path_, p.contents_) for p in loaded_directories if p.contents_ is not None)
            if __dynamic or isinstance(cls, DynamicDict):
                return DynamicDict(__files)
            return DynamicGen(__files)
        
        with cls._THREAD_LOCK:
            loaded_directories = cls._THREAD_EXECUTOR.map(partial(cls, **__kwargs), __directories)
            __files = ((__cls.ext_path, __cls.files) for __cls in loaded_directories)
        __type = cls._get_type(__files)
        
        if __dynamic and __type==DynamicDict:
            return DynamicDict(__files)
        return DynamicGen(__files)
    
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
                        default_factory=lambda: [f'Section{i}' for i in range(1,4)])
                        
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
                                default_factory=lambda: _NEW_CONFIG())
    
    def __post_init__(self):
        self.config_ini = self._validate_config()
        self.config = self._get_config()
    
    def _map_ini_suffix(self, __path):
        self.config_ini = Path(__path).with_suffix('.ini')
    
    def _validate_config(self):
        _ini = self.config_ini
        if _ini is None:
            raise DLoaderException(880, _ini)
        
        return DataLoader._validate_path(_ini, True)
    
    @staticmethod
    def _sql_config_sections():
        return ConfigManager.create_sql_config(sections_only=True)
    
    @staticmethod
    def create_sql_config(__ini_name='sql_config', sections_only=False):
        _ini_name = Path(__ini_name).with_suffix('.ini')
        _config_parser = _NEW_CONFIG()
        
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
            DLoaderException(7, _ini_name, _log_method=logger.info)
            DLoaderException(0, message=f'`{_ini_name}`: {sections}', _log_method=logger.info)
            return
        __input = f'The file `{_ini_name}` already exists. Overwriting will simply empty it out with null values. Proceed (N/y)? '
        if _ini_name.is_file():
            if compiler(['yes', 'y', '1'], input(__input)):
                return _write_file()
            DLoaderException(0, message=f'[TERMINATED] {_ini_name} has not been overwritten.', _log_method=logger.info)
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
            if not len(_config):
                raise DLoaderException(899, __name.name)
            
            __section_defaults = list(*(i.default_factory() for i in fields(self) if i.name=='sections'))
            if self.sections!=__section_defaults:
                try:
                    for _ in self.sections:
                        CC.items(_)
                except (NoSectionError, MissingSectionHeaderError):
                    if isinstance(self.sections, str):
                        self.sections = [self.sections]
                    _db_pat = partial(compiler, self.sections)
                    _possible_keys = list(filter(_db_pat, _ini_sections))
                    if _possible_keys:
                        good = {ini_key: _config.get(ini_key) for ini_key in _possible_keys if _has_nulls(ini_key)}
                        good_has_nulls = list(filterfalse(_has_nulls, _possible_keys))
                        __none = lambda i: all((compiler(good, i), compiler(good_has_nulls, i)))
                        __bad = list(filter(__none, self.sections))
                        if not good:
                            raise DLoaderException(1003, self.sections,  _possible_keys)
                        
                        if __bad:
                            DLoaderException(0, message=f'Invalid section{_s(__bad)}, skipping: {__bad}', _log_method=logger.warning)
                        
                        if good_has_nulls:
                            DLoaderException(1002, good_has_nulls, _log_method=logger.warning)
                        
                        self._update_attrs(__name.absolute(), list(good), __name.stem)
                        return good
                    
                    if not len(_possible_keys):
                        raise DLoaderException(890, self.sections, _ini_sections)
                    
            self._update_attrs(__name.absolute(), list(filter(_has_nulls, _ini_sections)),__name.stem)
            return _config
        return _clean_config()

    def _update_attrs(self, config_ini=None, sections=None, _ini_name=None):
        __config_params = DataLoader._get_params(self._update_attrs)
        self.__dict__.update(**dict(zip(__config_params,
                                        (config_ini,
                                        sections,
                                        _ini_name))))
    
    def _update_config(self, __section=None, __sources=None, encrypt=True):
        __sources = {} if __sources is None else __sources
        __config = dict((k, self._config_parser.encrypt_text(v, self._ini_name) \
                        if all((compiler(_PASS, k), encrypt)) else v) \
                        for k,v in __sources.items())
        if __section:
            self.config[__section].update(**__config)
        return self.config.update(**__config)
