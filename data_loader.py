import os
import re
import sys
import json
import hashlib
import logging
import threading
import mimetypes
import numpy as np
import pandas as pd
from pathlib import Path
from copy import deepcopy
from pdfminer.high_level import extract_pages
from collections import OrderedDict, namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache, wraps
from itertools import filterfalse, chain, count, zip_longest
from configparser import ConfigParser, NoSectionError, MissingSectionHeaderError
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView, Iterable,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from json.decoder import JSONDecodeError
from pandas.errors import ParserError, DtypeWarning, EmptyDataError
from constants import _PASS, _ERRORS
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr as _recursive_repr
from abc import ABC, abstractmethod, ABCMeta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='[%(asctime)s][LOG %(levelname)s]:%(message)s', 
                            datefmt='%Y-%m-%d %I:%M:%S %p')
handler = logging.FileHandler(f'{Path(__file__).stem}.log', 'a')
handler.setFormatter(formatter)
logger.addHandler(handler)

#** Outside of ThreadPool to ensure it prints only once for each execution(path)
def _dl_initializer(*__path, executor_only=False, max_workers_only=False):
    dl_executor = ThreadPoolExecutor(max_workers=(_max_workers:= \
                                                min(32, (os.cpu_count() or 1) + 4)))
    if executor_only:
        return dl_executor
    if max_workers_only:
        return _max_workers
    
    dl_executor._thread_name_prefix = __thread_prefix = 'DLExecutor'
    __thread_count = threading.active_count()
    __main_thread = threading.current_thread().name
    _repr = '\n\033[34m[{}]\033[0m Successfully initialized {} {} worker{} for \033[1;32m`{}`\033[0m\n'
    print(_repr.format(__thread_prefix, __thread_count,
                        __main_thread, _Generic._s_plural(__thread_count),
                        Path('/'.join(*__path))))

class _Generic(metaclass=ABCMeta):
    @abstractmethod
    def __missing__(self, *args, **kwargs):
        raise DLoaderException(*args, **kwargs)
    
    def _repr(cls, __iter):
        _cls_name = cls._cap_cls_name(cls)
        _place_holder = '\n{}([{}])'
        try:
            _items = list((k, cls._too_large(v), 0 if not isinstance(k, Path) else os.stat(k).st_size) for k, v in __iter)
            _total_bytes, _total = _Generic._bytes_converter(sum(i[-1] for i in _items))
            _string = _place_holder.format(
                                _cls_name,
                                f',\n{' ':>{len(_cls_name)+2}}'.join(
                                (f'({k}, {v}{', '+cls._bytes_converter(_b, True)[0] if isinstance(k, Path) else ''})' \
                                for k, v, _b in _items)
                                )
                            )
        except tuple(cls._all_errors()) as _errors:
            DLoaderException(0, message=_errors)
            return _place_holder.format(_cls_name, '')
        
        return _string if not _total else f'[{_total_bytes}] {_string}'
    
    @staticmethod
    def _cap_cls_name(__cls):
        return (lambda _cls: _cls.capitalize() if not _cls[0].isupper() else _cls)(__cls.__class__.__name__)
    
    @staticmethod
    def _bytes_converter(__num, __symbol_only=False, num_only=False):
        #XXX (KB)-1024, (MB)-1048576, (GB)-1073741824, (TB)-1099511627776
        _base = 1024
        _exp = np.arange(1,5)
        _conversions = dict(zip(('Kilobytes (KB)', 'Megabytes (MB)',
                                'Gigabytes (GB)', 'Terabytes (TB)'),
                                np.power(_base, _exp)))
        
        for k,v in _conversions.items():
            if (__num/_base)<v:
                if __symbol_only:
                    k = re.search(r'\((.*?)\)', k).group(1)
                results = f'{(_total:=__num/v):.2f} {k}', _total
                if num_only:
                    return _total
                return results
    
    @property
    def hashed_files(cls):
        return DataLoader._HASHED_FILES
    
    @staticmethod
    def _all_errors():
        return {
                PermissionError: 13,
                UnicodeDecodeError: 100,
                ParserError: 303,
                DtypeWarning: 400,
                OSError: 402,
                EmptyDataError: 607,
                JSONDecodeError: 102,
                Exception: 500
                }
    
    @staticmethod
    def compiler(__defaults, __k):
        try:
            __k = __k if isinstance(__k, str) \
                else '|'.join(map(re.escape, __k))
            __defaults = map(str, __defaults)
            return re.compile('|'.join(map(re.escape, __defaults)), re.IGNORECASE).match(__k)
        except TypeError as t_error:
            raise DLoaderException(0, message=t_error)
    
    @staticmethod
    def _s_plural(__word):
        return '{}'.format('s' if (hasattr(__word, '__len__') \
                and len(__word)>1 or __word>1) else '')
    
    @staticmethod
    def _validate_path(__path, __raise=False):
        def _raise(__exception):
            if __raise:
                raise __exception
        
        try:
            path = Path(__path)
        except tuple(_Generic._all_errors()) as _errors:
            _raise(DLoaderException(230, __path, _errors))
            return
        
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
        
        elif _Generic.compiler(r'^[._]', path.stem):
            _raise(DLoaderException(0, message=f'Skipping {path.name}', _log_method=logger.warning))
            return
        
        return path
    
    def reset(cls):
        try:
            if isinstance(cls, DynamicDict):
                return OrderedDict(cls.items())
            elif isinstance(cls, DynamicGen):
                return ((*kv,) for kv in cls)
        except: return cls
    
    def _too_large(self, __value, __max_length=75):
        _cls = self._cap_cls_name(__value)
        ellipsis = f'<{_cls}>'
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
    
    @staticmethod
    def _rm_period(__path):
        try:
            _path = str(__path).lstrip('.').lower()
        except tuple(_Generic._all_errors()) as _errors:
            raise DLoaderException(0, message=_errors)
        return _path
    
    @staticmethod
    def _get_type(__cls_files):
        for i,j in __cls_files:
            yield type(j)

class DynamicDict(OrderedDict, _Generic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __missing__(self, *args, **kwargs):
        super().__missing__(*args, **kwargs)
    
    @_recursive_repr()
    def __repr__(self):
        return self._repr(self.items())
    
    __str__ = __repr__
    
    def __dir__(self):
        return set(super().__dir__() + [str(i) for i in self])
    
    def __getattr__(self, __item):
        return self[__item]
    
    def __getitem__(self, __item):
        if not self.__contains__(__item):
            if (_right_key:=self._possible_key(__item)):
                return self.get(_right_key)
        
        return self.get(__item)
    
    def __setattr__(self, __attr, __value):
        self[__attr] = __value
    
    def __contains__(self, __key):
        return __key in self._posix_converter(self.keys())
    
    def _posix_converter(self, __items):
        if not all(isinstance(i, (tuple, list)) for i in __items):
            return list(map(lambda p: Path(p).name, __items))
        return list(map(lambda p: (Path(p[0]).name, p[1]), __items))
    
    def _possible_key(self, __key):
        _key = __key if not hasattr(__key, 'stem') else __key.stem
        try:
            return self.compiler(self.keys(), _key).group()
        except AttributeError:
            return
    
    def get(self, __key=None, __default=None):
        if (__key in ('', None)):
            return __default
        elif all((not __key, not __default)):
            return self.fromkeys((__key,))
        
        if self.__contains__(__key):
            for i,j in self._posix_converter(self.items()):
                if i==__key:
                    return j
        
        if (_possible_key:=self._possible_key(__key)):
            print(DLoaderException(221, __key, _possible_key, _log_method=logger.info))
        return __default

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
            holders = (Path(obj).name if isinstance(obj, (Path, str)) \
                        else obj for obj in __obj)
            return str_code.format(*holders)
    
    def _log_error(self, *args):
        self._log_method(f'{self.match_error(*args)}')

@dataclass(slots=True, weakref_slot=True)
class Extensions:
    
    _defaults: Dict = field(init=False, default_factory=lambda: Extensions.__defaults__)
    _ALL: Dict = field(init=False, default=None)
    
    def __post_init__(self):
        rm_p = _Generic._rm_period
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
    
    def __missing__(self, *args, **kwargs):
        super().__missing__(*args, **kwargs)
    
    def __repr__(self):
        return self._repr(self.__dict_gen)
    
    __str__ = __repr__
    
    def __dir__(self):
        return set(super().__dir__() + [str(i) for i,_j in self.__dict_gen])
    
    def __iter__(self):
        return iter(self.__dict_gen)
        
    def __len__(self):
        return 0 if not self.__dict_gen else sum(1 for __kv in self)
    
    def __bool__(self):
        return bool(self.__len__())
    
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
    _HASHED_FILES = DynamicDict()
    _THREAD_LOCK = threading.Lock()
    _THREAD_EXECUTOR = _dl_initializer(executor_only=True)
    _KWARG_KEYS = frozenset(('all_', 'dynamic', 'no_method',
                            'posix', 'module', 'generator'))
    
    __MAXW = False
    
    def __init__(self, ext_path=None, ext_defaults=None, all_=False, module=None, **kwargs):
        self.ext_path = ext_path
        self.ext_defaults = ext_defaults
        self.all_ = all_
        self.module = module
        self.kwargs = kwargs
        self.__post_init__()
    
    def __post_init__(self):
        _kwg = deepcopy(self.kwargs).get
        
        _dynamic, _generator, \
        _posix, _file_stats = \
                            (_kwg('dynamic'),
                            _kwg('generator'),
                            _kwg('posix'),
                            _kwg('file_stats')
                            )
        
        if not self.ext_path:
            self.__missing__(200, self.__class__.__name__, self.ext_path)
        
        elif all((self.ext_defaults, self.all_)):
            self.__missing__(202, self.__class__.__name__, self._DEFAULTS)
        
        elif all((_dynamic, _generator)):
            self.__missing__(201, DynamicGen.__name__)
        
        elif all((not _posix, _file_stats)):
            DLoaderException(370, FileStats.__name__,
                            f'For optimal compatibility, "posix" attr must be passed in when providing the "file_stats" attribute.\n Defaulting to POSIX.')
            _posix = True
        
        self.__files = None
        self.__posix = _posix
        self.ext_path = self._validate_path(self.ext_path, True)
        self.ext_defaults = self._validate_exts(self.ext_defaults)
    
    def __missing__(self, *args, **kwargs):
        _super = partial(super().__missing__)
        if args:
            _super(*args, **kwargs)
        
        _super(1, self.__class__.__name__,
                *map(lambda __cls: getattr(__cls[1], '__name__'),
                enumerate((DynamicGen, DynamicDict, DataLoader, DynamicGen, DataLoader)))
                )
    
    def __repr__(self):
        return self._repr(self.files)
    
    __str__ = __repr__
    
    def __call__(self):
        if self.kwargs.get('dynamic') \
            and not isinstance(self.files, DynamicDict) \
            and not self.kwargs.get('generator'):
            
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
    def get_files(cls, __path, defaults=None):
        _defaults = cls._DEFAULTS if defaults is None else defaults
        no_dirs = lambda _p: _p.is_file() and not _p.is_dir()
        _ext_pat = partial(cls.compiler, _defaults)
        return (_p for _p in __path.iterdir() \
                if _ext_pat(cls._rm_period(_p.suffix)) \
                and cls._validate_path(_p) \
                and no_dirs(_p))
    
    def _validate_exts(self, __exts):
        if __exts is None:
            return
        try:
            _valid_exts = [self._rm_period(ext) \
                            for ext in __exts \
                            if self._rm_period(ext) in self._ALL]
            
            _failed = list(filterfalse(lambda ext: ext in _valid_exts, __exts))
        except tuple(self._all_errors()) as _errors:
            raise type(_errors)(f'{_errors}\nExtension argument provided: {__exts!r}')
        
        if len(_failed)==len(__exts):
            raise DLoaderException(210, self._s_plural(len(_failed)), _failed, list(self._ALL))
        
        if _failed:
            DLoaderException(215, _failed, _log_method=logger.warning)
        return _valid_exts
    
    @staticmethod
    def _get_params(__method):
        import inspect
        try: 
            _sig = inspect.signature(__method)
        except TypeError:
            raise DLoaderException(800, __method)
        return _sig.parameters.keys()
    
    def _ext_method(self, __path):
        _suffix = self._rm_period(__path.suffix)
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
    
    def inject_files(self, *__dirs, **__kwargs):
        if not len(__dirs)>=1:
            raise DLoaderException(150)
        
        with self._THREAD_LOCK:
            _all_files = self._THREAD_EXECUTOR.map(partial(DataLoader,
                                                            module=__kwargs.get('module', self.id),
                                                            all_=self.all_, **self.kwargs),
                                                            __dirs)
        
        _gen_files = ((i.ext_path, i()) for i in _all_files)
        _type = next(self._get_type(_gen_files))
        if self.kwargs.get('dynamic') and _type==DynamicDict:
            self.files = DynamicDict(self.files)
            self.files.update(**DynamicDict(_gen_files))
            return self.files
        
        _files = chain(((k,v) for k,v in self.files if k not in _gen_files), _gen_files)
        self.files = DynamicGen(_files)
        return self.files
    
    @classmethod
    @cache
    def load_file(cls, file_path, **__kwargs):
        _path = cls._validate_path(file_path, True)
        p_method = cls._ext_method(cls, _path)
        __kwargs = {} or __kwargs
        loaded_file = cls._load_file(cls, _path, p_method, __kwargs)
        return loaded_file

    def _load_file(self, __path, __method, __kwargs):
        p_name = __path.parts[-1]
        p_contents = None
        _kwargs = __kwargs if not hasattr(self, 'kwargs') else self.kwargs
        method = open if _kwargs.get('no_method') else __method
        self._rm_kwargs(_kwargs)
        
        FileInfo = namedtuple('FileInfo', ('path_', 'contents_'),
                            defaults=(None,)*2,
                            module='FileLoader')
        
        try:
            p_contents = method(__path, **{})
        except tuple((_errors:=self._all_errors())) as _error:
            _exception = type(_error)
            _error_code = _errors.get(_exception, 500)
            _placeholder_count = _ERRORS.get(_error_code).count('{}')
            __raise = partial(self.__missing__, _error_code, p_name, _log_method=logger.warning)
            
            if _exception == JSONDecodeError:
                __raise(_error.pos, _error.lineno, _error.colno)
            elif _placeholder_count == 2:
                __raise(_error)
            elif _exception == Exception:
                raise _exception
            else:
                __raise()
            
            p_contents = 0
        
        if (isinstance(p_contents, int) and p_contents==0) \
            or (hasattr(p_contents, 'empty') and p_contents.empty):
            return FileInfo(__path)
        
        try:
            if not isinstance((_id:=self.id), property) \
                and _id not in (_hashed:=self._HASHED_FILES):
                
                _hashed[_id] = []
            _hashed[_id].append({__path: self.calculate_hash(__path)})
        
        except TypeError:
            pass
        
        return FileInfo(path_=__path, contents_=p_contents)
    
    @cache
    def _execute_path(self):
        _max_workers = _dl_initializer(max_workers_only=True)
        if not DataLoader.__MAXW:
            DataLoader.__MAXW = True
            print(f'\033[1;32m((DL)DataLoader MAXWORKERS={_max_workers})\033[0m')
        
        self._THREAD_EXECUTOR._initializer = _dl_initializer(Path(self.ext_path).parts[-2:])
        try:
            with self._THREAD_LOCK:
                _files = self._THREAD_EXECUTOR.map(self._check_ext, self._get_files)
        except tuple(self._all_errors()) as _error:
            raise DLoaderException(0, message=f'{_error}')
        
        return ((*file,) for file in _files if file.contents_ is not None)
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        
        while self.check_hash():
            _cache = DynamicDict(self.__files)
            try:
                next(iter(self.__files))
            except StopIteration:
                self.__files = _cache.items()
            
            if not self.__posix:
                self.__files = self.__cache((k.name, v) for k,v in self.__files)
            
            return self.__files
        
        raise DLoaderException(0, message=f'The integrity checker has failed during the loading process. Possible data tampering detected.')
    
    @staticmethod
    def __cache(__files):
        _files = __files
        _cache = DynamicDict(__files)
        try:
            next(iter(__files))
        except StopIteration:
            return _cache.items()
        return _files
    
    @staticmethod
    def calculate_hash(__file_path):
        sha256_hash = hashlib.sha256()
        with open(__file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def check_hash(cls, __path=None):
        _all_hashed = {k: hash_ for _k,v in cls.hashed_files.items() \
                                for j in v \
                                for (k,hash_) in j.items()}
        
        if __path is None:
            return all(cls.calculate_hash(k)==v for k,v in _all_hashed.items())
        
        _hashed = cls.calculate_hash((_path:=cls._validate_path(__path, True)))
        if (_result:=_all_hashed.get(_path)):
            return _hashed == _result
        
        raise DLoaderException(0, message=f'{__path} has no matches to compare too.\nOriginal hash value:\n{_hashed}')
    
    @property
    def hashed_files(cls):
        return cls._HASHED_FILES
    
    @cached_property
    def id(self):
        return f'ID_{next(DataLoader._ID)}' if not self.module else self.module
    
    @staticmethod
    def _posix_converter(__path, __kwargs):
        return __path if __kwargs.get('posix') else getattr(__path, 'name')
    
    @classmethod
    def add_files(cls, *__files, **__kwargs):
        if not len(__files)>=1:
            raise DLoaderException(220)
        _posix = cls._posix_converter
        kwargs = deepcopy(__kwargs)
        _gen_only = __kwargs.pop('generator', False)
        
        cls._rm_kwargs(__kwargs)
        with cls._THREAD_LOCK:
            loaded_files = cls._THREAD_EXECUTOR.map(partial(cls.load_file, **__kwargs),
                                                (path for path in map(cls._validate_path, __files)))
            
        __files = ((_posix(file.path_, kwargs), file.contents_) for file in loaded_files if file.contents_ is not None)
        if _gen_only:
            return __files
        
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
        
        _directories = map(cls._validate_path, __dirs)
        _dynamic = __kwargs.pop('dynamic', False)
        _merge = __kwargs.pop('merge', False)
        _defaults = __kwargs.pop('defaults', None)
        _all = __kwargs.pop('all_', False)
        _gen_only = __kwargs.pop('generator', False)
        _posix = cls._posix_converter
        
        if _merge:
            with cls._THREAD_LOCK:
                loaded_directories = (cls.load_file(j, **__kwargs) \
                                    for i in cls._THREAD_EXECUTOR.map(partial(cls.get_files, defaults=_defaults), _directories) \
                                    for j in i)
            __files = ((_posix(p.path_, __kwargs), p.contents_) for p in loaded_directories if p.contents_ is not None)
            if _gen_only:
                return __files
            
            if _dynamic and not isinstance(cls, DynamicDict):
                return DynamicDict(__files)
            return DynamicGen(__files)
        
        
        with cls._THREAD_LOCK:
            loaded_directories = cls._THREAD_EXECUTOR.map(partial(cls, ext_defaults=_defaults, all_=_all, **__kwargs), _directories)
        
        __files = ((_posix(__cls.ext_path, __kwargs), __cls.files) for __cls in loaded_directories)
        if _gen_only:
            return __files
        
        __type = next(cls._get_type(__files))
        
        if _dynamic and not __type==DynamicDict:
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

class FileStats(_Generic):
    def __init__(self, __paths, __posix=False):
        self.paths = __paths
        self.posix = __posix
        self._all_stats = None
        self.__post_init__()
    
    def __post_init__(self):
        if isinstance(self.paths, DynamicGen):
            self.paths = [k if not isinstance(k, (DynamicGen, str)) 
                        else k if not isinstance(self._get_type(k), (int, str)) 
                        else k[0] for k,_v in self.paths]
        elif isinstance(self.paths, DynamicDict):
            self.paths = self.paths.keys()
        
        try:
            for _p in self.paths:
                self._validate_path(_p, True)
        except DLoaderException as dl_error:
            self.__missing__(370, self.__class__.__name__, f'ERROR: {dl_error}')
    
    def __missing__(self, *args, **kwargs):
        return super().__missing__(*args, **kwargs)

    def __repr__(self):
        return self._repr(zip_longest(self.paths, ['N/A']))
    
    __str__ = __repr__
    
    def __call__(self):
        return self.all_stats
    
    @staticmethod
    def _bytes_converter(*args, **kwargs):
        return _Generic._bytes_converter(*args, **kwargs)
    
    def _get_stats(self):
        return DynamicDict({i.name if self.posix else i: self._os_stats(i) for i in self.paths})
    
    @staticmethod
    def _os_stats(__path):
        FStats = namedtuple('FullSize', ('sym_size', 'num_size'))
        _stats = os.stat_result(os.stat(__path))
        _os_stats = DynamicDict({attr: getattr(_stats, attr) for attr in dir(_stats) if attr.startswith('st')})
        _os_stats.update({'st_fsize': \
                        FStats(*FileStats._bytes_converter(os.stat(__path).st_size, True))})
        return _os_stats
    
    @property
    def all_stats(self):
        if self._all_stats is None:
            self._all_stats = self._get_stats()
        return self._all_stats

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
            if _Generic.compiler(['yes', 'y', '1'], input(__input)):
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
                            {k: CC.encrypt_text(v, __name.stem, True) if all((_Generic.compiler(_PASS, k), self.encrypt, CC.convert_value(v) is not None)) \
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
                    _db_pat = partial(_Generic.compiler, self.sections)
                    _possible_keys = list(filter(_db_pat, _ini_sections))
                    if _possible_keys:
                        good = {ini_key: _config.get(ini_key) for ini_key in _possible_keys if _has_nulls(ini_key)}
                        good_has_nulls = list(filterfalse(_has_nulls, _possible_keys))
                        __none = lambda i: all((_Generic.compiler(good, i), _Generic.compiler(good_has_nulls, i)))
                        __bad = list(filter(__none, self.sections))
                        if not good:
                            raise DLoaderException(1003, self.sections,  _possible_keys)
                        
                        if __bad:
                            DLoaderException(0, message=f'Invalid section{self._s_plural(__bad)}, skipping: {__bad}', _log_method=logger.warning)
                        
                        if good_has_nulls:
                            DLoaderException(1002, good_has_nulls, _log_method=logger.warning)
                        
                        self._update_attrs(__name.absolute(), list(good), __name.stem)
                        return good
                    
                    if not len(_possible_keys):
                        raise DLoaderException(890, self.sections, _ini_sections)
                    
            self._update_attrs(__name.absolute(), list(filter(_has_nulls, _ini_sections)), __name.stem)
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
                        if all((_Generic.compiler(_PASS, k), encrypt)) else v) \
                        for k,v in __sources.items())
        if __section:
            self.config[__section].update(**__config)
        return self.config.update(**__config)

