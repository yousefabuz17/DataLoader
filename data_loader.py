import os
import re
import sys
import json
import shutil
import hashlib
import logging
import argparse
import threading
import traceback
import mimetypes
import numpy as np
import pandas as pd
from time import time
from pathlib import Path
from copy import deepcopy
from datetime import datetime
from cryptography.fernet import Fernet
from pdfminer.high_level import extract_pages
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict, namedtuple, defaultdict
from functools import cached_property, partial, cache, wraps
from itertools import filterfalse, chain, count, zip_longest
from configparser import ConfigParser, NoSectionError, MissingSectionHeaderError
from typing import Any, AnyStr, Dict, Generator, IO, Iterable
from json.decoder import JSONDecodeError
from pandas.errors import DtypeWarning, EmptyDataError, ParserError
from constants import *
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr as _recursive_repr
from abc import ABCMeta, abstractmethod
from dataclasses_json import dataclass_json, LetterCase

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='[%(asctime)s][LOG %(levelname)s]:%(message)s', 
                            datefmt='%Y-%m-%d %I:%M:%S %p')
handler = logging.FileHandler(f'{Path(__file__).stem}.log', 'a')
handler.setFormatter(formatter)
logger.addHandler(handler)

#** Outside of ThreadPool to ensure it prints only once for each execution(path)
def _dl_initializer(*__paths, executor_only=False, max_workers_only=False):
    _max_workers = min(32, (os.cpu_count() or 1) + 4)
    dl_executor = ThreadPoolExecutor(max_workers=_max_workers)
    if any((executor_only, max_workers_only)):
        return [dl_executor, _max_workers][int(max_workers_only)]
    
    dl_executor._thread_name_prefix = __thread_prefix = 'DLExecutor'
    __thread_count = threading.active_count()
    __main_thread = threading.current_thread().name
    _repr = '\n\033[34m[{}]\033[0m Successfully initialized {} {} worker{} for \033[1;32m{!r}\033[0m\n'
    print(_repr.format(__thread_prefix, __thread_count,
                        __main_thread, _Generic._s_plural(__thread_count),
                        '/'.join(*__paths)))

class _Generic(metaclass=ABCMeta):
    _THREAD_LOCK = threading.Lock()
    _THREAD_EXECUTOR = _dl_initializer(executor_only=True)
    
    @abstractmethod
    def __missing__(self, *args, **kwargs):
        raise DLoaderException(*args, **kwargs)
    
    @abstractmethod
    def _repr(cls, __iter):
        _cls_name = cls._cap_cls_name(cls)
        _place_holder = '\n{}([{}])'
        if type(cls)==DataLoader:
            return f'<{DataLoader.__name__}.files object at {hex(id(cls))}>'
        try:
            _items = list((k, cls._too_large(v),
                            0 if not isinstance(k, Path) else os.stat(k).st_size) 
                            for k, v in __iter)
            _total_bytes, _total = _Generic._bytes_converter(sum(i[-1] for i in _items))
            _string = _place_holder.format(
                                _cls_name,
                                f',\n{' ':>{len(_cls_name)+2}}'.join(
                                (f'({k}, {v}{', '+cls._bytes_converter(_b, True)[0] if isinstance(k, Path) else ''})' \
                                for k, v, _b in _items)
                                )
                            )
        except tuple(_Generic._all_errors()) as _errors:
            _Generic._dl_raise(_errors)
            return _place_holder.format(_cls_name, '')
        
        return _string if not _total else f'[{_total_bytes}] {_string}'
    
    @staticmethod
    def _terminal_size():
        return shutil.get_terminal_size()
    
    @staticmethod
    def _dl_raise(__errors, /, message_only=False, *, pre='', post='', verbose=False):
        _dle = DLoaderException
        _error = __errors if not callable(__errors) else type(__errors)(__errors)
        _args = (None, f'{pre}{_error}{post}')
        if not message_only:
            raise _dle(*_args)
        if verbose:
            _dle(*_args)
    
    @staticmethod
    @abstractmethod
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
    @abstractmethod
    def _validate_path(__path, __raise=False, /, verbose=False):
        dle = DLoaderException
        
        try:
            path = Path(__path)
        except tuple(_Generic._all_errors()) as _errors:
            raise dle(230, __path, _errors)
        
        if not path:
            raise dle(800, path)
        
        elif not path.exists():
            raise dle(404, path)
        
        elif (not path.is_file()) \
            and (not path.is_dir()) \
            and (not path.is_absolute()):
            
            if verbose:
                dle(707, path)
            return
        
        elif _Generic.compiler(r'^[._]', path.stem):
            if verbose:
                dle(None, message=f'Skipping {path.name!r}', _log_method=logger.warning)
            return
        
        return path
    
    @staticmethod
    def _cap_cls_name(__cls):
        return (lambda _cls: _cls.capitalize() if not _cls[0].isupper() else _cls)(__cls.__class__.__name__)
    
    @staticmethod
    def _bytes_converter(__num, /, symbol_only=False, total_only=False):
        #XXX (KB)-1024, (MB)-1048576, (GB)-1073741824, (TB)-1099511627776
        _base = 1024
        _exp = np.arange(1,5)
        _conversions = dict(zip(('Kilobytes (KB)', 'Megabytes (MB)',
                                'Gigabytes (GB)', 'Terabytes (TB)'),
                                np.power(_base, _exp)))
        results = next((f'{(_total:=__num/v):.2f} {re.search(r'\((.*?)\)', k).group(1) \
                        if symbol_only else k}', _total)
                        for k,v in _conversions.items() if (__num/_base)<v)
        
        if total_only:
            return _total
        return results
    
    @staticmethod
    def _cls_tuple(__tuple, defaults=None):
        _defaults = defaults or (None,)*2
        _tuples = (namedtuple('Encrypter', ('text', 'key'), defaults=_defaults, module='__Encrypt'),
                    namedtuple('ExtInfo', ('suffix_', 'loader_'), defaults=_defaults, module='__Ext'),
                    namedtuple('PathInfo', ('path_', 'contents_'), defaults=_defaults, module='__Path'),
                    namedtuple('FStats', ('sym_size', 'num_size'), defaults=_defaults, module='__FStats'))
        try:
            return next((_tup for _tup in _tuples if _tup.__module__==__tuple))
        except StopIteration:
            raise DLoaderException(0, message=f'Invalid module name when fetching namedtuple type: {__tuple}')
    
    @property
    def hashed_files(cls):
        return DataLoader._HASHED_FILES
    
    @staticmethod
    def compiler(__defaults, __k):
        if __k is None:
            return
        try:
            _defaults = map(re.escape, map(str, __defaults))
            _k = __k if isinstance(__k, str) \
                    else '|'.join(map(re.escape, __k))
            _compiled = re.compile('|'.join(_defaults), re.IGNORECASE).match(_k)
        except TypeError as t_error:
            raise DLoaderException(0, message=f'{t_error}')
        
        return _compiled
    
    @staticmethod
    def _s_plural(__word):
        _base = '{}'.format
        _hasattr = partial(hasattr, __word)
        try:
            if (_hasattr('__len__') and len(__word)>1) \
                or (_hasattr('__gt__') and __word>1):
                
                return _base('s')
        
        except tuple(_Generic._all_errors()):
            pass
        
        return _base('')
    
    def reset(cls):
        try:
            return \
                [OrderedDict(cls.items()), ((*kv,) for kv in cls)] \
                [isinstance(cls, DynamicGen)]
        except tuple(cls._all_errors()):
            pass
        return cls
    
    @staticmethod
    def _too_large(value, max_length=None):
        _max_length = max_length if isinstance(max_length, int) \
                        else _Generic._terminal_size().columns
        _cls = _Generic._cap_cls_name(value)
        _cls_tag = f'<{_cls}>'
        try:
            _length = len(str(value))
        except TypeError:
            _length = None
        
        if any((
                (_length is not None) and (_length >= _max_length),
                (hasattr(value, '__str__')) and (len(str(value)) >= _max_length),
                isinstance(value, (Generator, Iterable))
                )):
            return _cls_tag
        return value
    
    @staticmethod
    def _rm_period(__path):
        try:
            _path = __path.lstrip('.').lower()
        except tuple(_Generic._all_errors()):
            pass
        return _path
    
    @staticmethod
    def _get_type(__cls_files):
        for _i,j in __cls_files:
            yield type(j)

@dataclass_json
class DynamicDict(OrderedDict, _Generic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __missing__(self, *args, **kwargs):
        super(_Generic, DynamicDict).__missing__(*args, **kwargs)
    
    @_recursive_repr()
    def __repr__(self):
        return self._repr(self.items())
    
    __str__ = __repr__
    
    def __dir__(self):
        return list(set(self.keys() + \
                        super(OrderedDict, DynamicDict).__dir__()))
    
    def __getattr__(self, __item):
        return self[__item]
    
    def __getitem__(self, __item):
        if not self.__contains__(__item):
            if (_right_key:=self._possible_key(__item)):
                return self.get(_right_key)
        
        return self.get(__item)
    
    def __setattr__(self, __attr, __value):
        self[__attr] = __value
    
    def _repr(cls, __iter):
        return super()._repr(__iter)
    
    @staticmethod
    def _all_errors():
        return _Generic._all_errors()
    
    @staticmethod
    def _validate_path(*args):
        return _Generic._validate_path(*args)
    
    def _posix_converter(self, __items):
        _posix = lambda _p, _slice=False: (_p[0], _p[1]) \
                                            if _slice else _p
        try:
            if all((isinstance(i, Iterable) and len(i)>1) for i in __items):
                
                return list(map(lambda _i: _posix(_i, _slice=True), __items))
            return list(map(_posix, __items))
        
        except tuple(self._all_errors()) as _errors:
            self._dl_raise(_errors)
    
    def _possible_key(self, __key):
        if hasattr(__key, 'stem') or isinstance(__key, Path):
            __key = __key.stem
        try:
            return self.compiler(self.keys(), __key).group()
        except AttributeError:
            pass
    
    def get(self, __key=None, __default=None):
        if any((__key in ('', None),
                _nothing:=all((not __key, not __default)))):
            
            return [__default, self.fromkeys((__key,))][_nothing]
        
        if self.__contains__(__key):
            _results = next((j for i,j in self.items() if i==__key))
            return _results
        
        if (_possible_key:=self._possible_key(__key)):
            DLoaderException(221, __key, _possible_key, _log_method=logger.info)
        return __default
    
    def to_json(self, *args, **kwargs):
        return self.to_json(*args, **kwargs)

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
    def encrypt_text(cls, text, /, ini_name='config', *, export=False):
        Encrypter = _Generic._cls_tuple('__Encrypt')
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        encrypted_bytes = cipher_suite.encrypt(text.encode())
        encrypted_text = encrypted_bytes.hex()
        encrypted_data = Encrypter(encrypted_text, key)
        if export:
            cls._exporter(text, encrypted_data, ini_name)
            # return encrypted_data
        
        return encrypted_data

    @classmethod
    def decrypt_text(cls, encrypted_text, key):
        _Fernet = Fernet
        cipher_suite = _Fernet(key)
        encrypted_bytes = bytes.fromhex(encrypted_text)
        decrypted_message = cipher_suite.decrypt(encrypted_bytes).decode()
        return decrypted_message
    
    @classmethod
    def _exporter(cls, org_text, encrypted, /, ini_name='config', *, refresh=False, ext_path=None):
        _config_parser = globals()['_NEW_CONFIG']()
        _items = {'ENCRYPTED_DATA': 
                dict(zip(('ORIGINAL_TEXT', 'ENCRYPTED_TEXT', 'DECRYPTER_KEY'),
                        (org_text, encrypted.text, encrypted.key)))
                }
        
        _config_parser.update(**_items)
        _path = Path(f'encrypted_{ini_name}.ini') if not ext_path else _Generic._validate_path(ext_path)
        if not _path.is_file() or refresh:
            with open(_path, mode='w') as c_file:
                _config_parser.write(c_file)
            DLoaderException(None, f'{_path!r} has been successfully created.', _log_method=logger.info)
        return _config_parser

class DLoaderException(BaseException):
    __slots__ = ('__weakref__', 'args', '__message',
                '__error_message', '_log_method')
    
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
        try:
            str_code = _ERRORS[__code]
            holders = (Path(obj).name if isinstance(obj, (Path, str)) \
                        else obj for obj in __obj)
        except AttributeError as attr_error:
            raise attr_error(_human_error)
        
        return str_code.format(*holders)
    
    def _log_error(self, *args):
        # traceback_ = traceback.format_exc(limit=1)
        self._log_method(f'{self.match_error(*args)}')

@dataclass(slots=True, weakref_slot=True, order=True)
class Timer:
    message: AnyStr = field(init=True, default='', repr=False)
    verbose: bool = field(init=True, default=False, kw_only=True)
    
    _start_time: float = field(init=False, default_factory=time, repr=False)
    _end_time: float = field(init=False, default=None, repr=False)
    
    def __enter__(self):
        return self._start_time

    def __exit__(self, *args, **kwargs):
        self._end_time = time()
        elapsed_time = self._end_time - self._start_time
        minutes, seconds = divmod(elapsed_time, 60)
        if self.verbose:
            if self.message:
                print(f'\033[33m{self.message!r}\033[0m')
            print(f'\033[32mExecution Time:\033[0m {minutes:.0f} minutes and {seconds:.5f} seconds.')


@dataclass(slots=True, weakref_slot=True)
class Extensions:
    _defaults: Dict = field(init=False, default=None)
    _ALL: Dict = field(init=False, default=None)
    
    def __post_init__(self):
        rm_p = _Generic._rm_period
        _defaults = self.__defaults__()
        _mimetypes = mimetypes.types_map
        _mimetypes['xlsx'] = None
        ExtInfo = self.__subclass__()
        _all_exts = set(ExtInfo(rm_p(i), pd.read_excel if rm_p(i) in ('xls','xlsx') else open) \
                        for i in _mimetypes \
                        if rm_p(i) not in _defaults)
        self._defaults = [ext for ext in _defaults if ext!='empty']
        self._ALL = {**{ext.suffix_: ext for ext in _all_exts},
                     **_defaults}
    
    def __defaults__(self):
        ExtInfo = self.__subclass__()
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
    
    def __subclass__(self):
        return _Generic._cls_tuple('__Ext')

EXTENSIONS = Extensions()
_NEW_CONFIG = lambda: CConfigParser(allow_no_value=True,
                                    delimiters='=',
                                    dict_type=DynamicDict,
                                    converters={'*': CConfigParser.convert_value}
                                    )

class DynamicGen(Iterable, _Generic):
    __slots__ = ('__weakref__', '__dict_gen')
    
    def __init__(self, __dict_gen):
        self.__dict_gen = __dict_gen
    
    def __missing__(self, *args, **kwargs):
        _super = partial(super().__missing__, *args, **kwargs)
        if all((args, kwargs)):
            _super(*args, **kwargs)
        
        _super(1, self.__class__.__name__,
                *map(lambda __cls: getattr(__cls[1], '__name__'),
                enumerate((DynamicGen, DynamicDict, DataLoader, DynamicGen, DataLoader)))
                )
    
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
    
    def _repr(cls, __iter):
        return super()._repr(__iter)
    
    @staticmethod
    def _all_errors():
        return _Generic._all_errors()
    
    @staticmethod
    def _validate_path(*args):
        return _Generic._validate_path(*args)
    
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
    __slots__ = ('ext_path', 'defaults',
                'all_exts', 'module', '__files',
                '__posix', '__ID')
    
    _ID = count()
    _TIMER = Timer
    _DEFAULTS = EXTENSIONS._defaults
    _ALL = EXTENSIONS._ALL
    _HASHED_FILES = {}
    _KWARG_KEYS = frozenset(('dynamic', 'no_method',
                            'module', 'generator',
                            'verbose', 'data_manager',
                            'allow_empty_files'))
    
    __MAXW = False
    
    def __init__(self, ext_path, defaults=None,  
                        all_exts=False, module=None,
                        posix=True, **kwargs):
        
        self.ext_path = ext_path
        self.defaults = defaults
        self.all_exts = all_exts
        self.module = module
        self.__posix = posix
        self.kwargs = kwargs
        self.__post_init__()
    
    def __post_init__(self):
        _kwg = deepcopy(self.kwargs).get
        
        _dynamic, \
        _generator, \
        _data_manager = (_kwg('dynamic'),
                        _kwg('generator'),
                        _kwg('data_manager')
                        )
        _missing = self.__missing__
        
        if not self.ext_path:
            _missing(200, self.__class__.__name__, self.ext_path)
        
        elif all((self.defaults, self.all_exts)):
            _missing(202, self.__class__.__name__, self._DEFAULTS)
        
        elif all((_dynamic, _generator)):
            _missing(201, DynamicGen.__name__)
        
        elif all((not self.__posix, _data_manager)):
            DLoaderException(370, DataManager.__name__,
                            f'For optimal compatibility, "posix" attr must be passed in when providing the "data_manager" attribute.\n>>Provided paths will default to POSIX for you.')
            self.__posix = True
        
        self.__files = None
        self.ext_path = self._validate_path(self.ext_path, True)
        self.defaults = self._validate_exts(self.defaults)
    
    def __missing__(self, *args, **kwargs):
        super().__missing__(*args, **kwargs)
    
    def __repr__(self):
        return self._repr(self.files)
    
    __str__ = __repr__
    
    def __call__(self, *args, **kwargs):
        _kwg = self.kwargs.get
        if _kwg('data_manager'):
            return DataManager(self.files, *args, **kwargs)
        elif all((_kwg('dynamic'),
                not isinstance(self.files, DynamicDict),
                not _kwg('generator'))):
            return DynamicDict(self.files)
        elif _kwg('generator'):
            return self.files
        return DynamicGen(self.files)
    
    def _repr(cls, __iter):
        return super()._repr(__iter)
    
    @staticmethod
    def _all_errors():
        return _Generic._all_errors()
    
    @staticmethod
    def _validate_path(*args):
        return _Generic._validate_path(*args)
    
    @cached_property
    def _get_files(self):
        _defaults = self._DEFAULTS
        
        if self.defaults:
            _defaults = self._validate_exts(self.defaults)
        elif self.all_exts:
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
            _valid_exts = list(
                            self._rm_period(ext) \
                            for ext in __exts \
                            if self._rm_period(ext) in self._ALL
                            )
            
            _failed = list(filterfalse(lambda ext: ext in _valid_exts, __exts))
        except tuple(self._all_errors()) as _errors:
            self._dl_raise(_errors, post=f'\nExtension argument provided: {__exts!r}')
        
        if len(_failed)==len(__exts):
            raise DLoaderException(210, self._s_plural(len(_failed)), _failed, list(self._ALL))
        
        if _failed and self.kwargs.get('verbose'):
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
        else:
            if self.kwargs.get('verbose'):
                DLoaderException(702,
                            __path.name,
                            _method,
                            _method:=open)
            
        return _method
    
    def _check_ext(self, __path):
        _method = self._ext_method(__path)
        _kwargs = {param: value for param, value in self.kwargs.items() \
                    if param in self._get_params(_method) \
                    and value is not None}
        _verbose = self.kwargs.get('verbose')
        if _kwargs and _verbose:
            DLoaderException(-1, _log_method=logger.info)
        
        return self._load_file(__path, _method, _kwargs)
    
    def inject_files(self, *__dirs, **__kwargs):
        if all((hasattr(__dirs, '__len__'),
                not len(__dirs)>=1)):
            
            raise DLoaderException(150)
        
        with self._THREAD_LOCK:
            _all_files = self._THREAD_EXECUTOR.map(partial(DataLoader,
                                                            module=self.id,
                                                            all_exts=self.all_exts, **self.kwargs),
                                                            __dirs)
        
        _gen_files = ((_cls.ext_path, _cls.__call__()) for _cls in _all_files)
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
    
    def _load_file(cls, __path, __method, __kwargs):
        _p_name = '/'.join(__path.parts[-2:])
        p_contents = None
        _org_kwargs = deepcopy(__kwargs)
        _kwargs = cls.kwargs if hasattr(cls, 'kwargs') else _org_kwargs
        _org_get = _kwargs.get
        _verbose = _org_get('verbose')
        _allow_empty = _org_get('allow_empty_files')
        _method = open if _org_get('no_method') else __method
        cls._rm_kwargs(_org_kwargs)
        
        with cls._TIMER(message=f'Executing {_p_name!r}', verbose=_verbose):
            try:
                p_contents = _method(__path, **{})
            except tuple((_errors:=cls._all_errors())) as _error:
                _exception = type(_error)
                _error_code = _errors.get(_exception, 500)
                _placeholder_count = _ERRORS.get(_error_code).count('{}')
                __raise = partial(DLoaderException, _error_code, _p_name, _log_method=logger.warning)
                
                if _exception == JSONDecodeError:
                    __raise(_error.pos, _error.lineno, _error.colno)
                elif _placeholder_count == 2:
                    __raise(_error)
                elif _exception == Exception:
                    raise _exception
                else:
                    __raise()
                
                p_contents = 0
            
            _path = cls._posix_converter(__path, posix=cls.__posix)
            try:
                if not isinstance((_id:=cls.id), property) \
                    and _id not in (_hashed:=cls._HASHED_FILES):
                    
                    _hashed[_id] = []
                _hashed[_id].append({__path: cls.calculate_hash(__path)})
            
            except TypeError:
                pass
            
            return cls._check_empty(_path, p_contents, _allow_empty=_allow_empty)
    
    @staticmethod
    def _check_empty(__path, __contents, _allow_empty=False):
        _p, _contents = __path, __contents
        PathInfo = _Generic._cls_tuple('__Path')
        
        _PI = partial(PathInfo, path_=_p)
        _hattr = lambda __name: hasattr(_contents, __name)
        
        _is_empty = (
                    (_hattr('empty') and _contents.empty) or
                    (_hattr('read') and not _contents.read().strip()) or
                    (_hattr('strip') and _hattr('__str__') and not _contents.strip()) or
                    (_hattr('__int__') and _contents == 0) or
                    (_hattr('__len__') and not len(_contents)) or
                    (_hattr('__bool__') and not _hattr('empty') and not bool(_contents)) or
                    (_hattr('getvalue') and not _contents.getvalue()) or
                    (_hattr('decode') and not _contents.decode()) or
                    (_hattr('isatty') and _contents.isatty()) or
                    (_hattr('tell') and _contents.tell() == 0) or
                    (_hattr('seek') and (_contents.seek(0, os.SEEK_END), _contents.tell() == 0)) or
                    (_hattr('geturl') and not _contents.geturl())
                )
        
        if _is_empty and not _allow_empty:
            return _PI()
        
        return _PI(contents_=_contents)
    
    @cache
    def _execute_path(self):
        _max_workers = _dl_initializer(max_workers_only=True)
        _string = f'(DL)DataLoader MAXWORKERS={_max_workers}'
        _verbose = self.kwargs.get('verbose')
        if _verbose:
            _ts = self._terminal_size().columns
            _prefix = _string.center(_ts, '-')
            if not DataLoader.__MAXW:
                DataLoader.__MAXW = True
                print(f'\033[1;32m{_prefix}\033[0m')
            
            self._THREAD_EXECUTOR._initializer = _dl_initializer(self.ext_path.parts[-2:])
        
        try:
            with self._THREAD_LOCK:
                _files = self._THREAD_EXECUTOR.map(self._check_ext, self._get_files)
        except tuple(self._all_errors()) as _errors:
            self._dl_raise(_errors, verbose=_verbose)
        
        return ((*file,) for file in _files if file.contents_ is not None)
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
            return self.__files
        
        while self.check_hash():
            _cache = self.__cache(self.__files)
            try:
                next(iter(self.__files))
            except StopIteration:
                self.__files = _cache.items()
            
            return self.__files
        
        raise DLoaderException(270)
    
    def __cache(self, __files):
        _files = ((k.name, v) for k,v in __files) if not self.__posix and self.compare_posix(__files) else __files
        _cache = DynamicDict(_files)
        try:
            next(iter(_files))
        except StopIteration:
            _files = _cache
        return _files
    
    @staticmethod
    def compare_posix(*args):
        _dl_error = partial(DLoaderException, 170)
        _check_diffs = lambda _set1, _set2: bool(_set1-_set2) \
                                        and bool(_set2-_set1)

        _stem_converter = lambda _p: Path(str(_p)).stem
        _set_converter = lambda _args: set(map(_stem_converter, _args))
        try:
            l_args = len(args)
            
            if not 1<=l_args<=2:
                raise _dl_error(f'The number of arguments passed as sets must be 1<=x<=2')
            
            _compared = map(_set_converter, args)
            
            if l_args==1:
                _compared = (_set_arg_copy:=_set_converter(*args)), _set_arg_copy
            
            _first_set, _second_set = _compared
            
        except tuple(_Generic._all_errors()) as _errors:
            raise _dl_error(f'{_errors}')
        return not _check_diffs(_first_set, _second_set)
    
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
        raise DLoaderException(None, message=f'{__path} has no matches to compare too.\nOriginal hash value:\n{_hashed}')
    
    def manage_data(self):
        return DataManager(self.files)
    
    @property
    def hashed_files(cls):
        return DynamicDict(cls._HASHED_FILES)
    
    @cached_property
    def id(self):
        return f'ID_{next(DataLoader._ID)}' if not self.module else self.module
    
    @staticmethod
    def _posix_converter(__path, __kwargs=None, posix: bool=False):
        if __kwargs and not posix:
            return __path if __kwargs.get('posix') else getattr(__path, 'name')
        return __path.name if not posix else __path
    
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
    
    @classmethod
    def _rm_kwargs(cls, __kwargs):
        for i in cls._KWARG_KEYS:
            __kwargs.pop(i, False)
    
    @classmethod
    def add_dirs(cls, *__dirs, **__kwargs):
        if not len(__dirs)>=1:
            raise DLoaderException(220)
        
        _directories = map(cls._validate_path, __dirs)
        _dynamic = __kwargs.pop('dynamic', False)
        _merge = __kwargs.pop('merge', False)
        _defaults = __kwargs.pop('defaults', None)
        _all = __kwargs.pop('all_exts', False)
        _gen_only = __kwargs.pop('generator', False)
        _posix = cls._posix_converter
        
        if _merge:
            with cls._THREAD_LOCK:
                loaded_directories = (cls.load_file(j, **__kwargs) \
                                    for i in cls._THREAD_EXECUTOR.map(partial(cls.get_files, defaults=_defaults), _directories) \
                                    for j in i)
            __files = ((_posix(p.path_, __kwargs), p.contents_) for p in loaded_directories if p.contents_ is not None)
            
            if any((_dynamic and not isinstance(cls, DynamicDict),
                    _gen_only,)):
                
                return [DynamicDict(__files), __files][int(_gen_only)]
            
            return DynamicGen(__files)
        
        
        with cls._THREAD_LOCK:
            loaded_directories = cls._THREAD_EXECUTOR.map(partial(cls, defaults=_defaults, all_=_all, **__kwargs), _directories)
        
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

class DataManager(Iterable, _Generic):
    _ID = count()
    __slots__ = ('_paths', '_module', '_serializer',
                '_posix', '_all_stats', '__ID')
    
    def __init__(self, __paths, module=None, serializer=None, posix=True):
        self._paths = __paths
        self._module = module
        self._serializer = serializer
        self._posix = posix
        self.__post_init__()
    
    def __post_init__(self):
        self._all_stats = None
        self.__ID = next(self._ID)
        try:
            self._paths = list(self._paths)
            for _p in self._paths:
                self._validate_path(_p, True)
        
        except DLoaderException as dl_error:
            self.__missing__(370, self.__class__.__name__, f'ERROR: {dl_error}')
    
    def __missing__(self, *args, **kwargs):
        return super().__missing__(*args, **kwargs)

    def __repr__(self):
        return self._repr(zip_longest(self._paths, [None]))
    
    __str__ = __repr__
    
    def __iter__(self):
        return iter(self._paths)
    
    def __call__(self):
        return self.all_stats
    
    def _repr(cls, __iter):
        return super()._repr(__iter)
    
    @staticmethod
    def _all_errors():
        return _Generic._all_errors()
    
    @staticmethod
    def _validate_path(*args):
        return _Generic._validate_path(*args)
    
    def _get_stats(self):
        return DynamicDict({_path.name if not self._posix else _path: \
                            self._os_stats(_path) for _path in self._paths})
    
    @staticmethod
    def _os_stats(__path):
        FStats = _Generic._cls_tuple('__FStats')
        _stats = os.stat_result(os.stat(__path))
        _os_stats = DynamicDict({attr: getattr(_stats, attr) for attr in dir(_stats) if attr.startswith('st')})
        _os_stats.update({'st_fsize': \
                        FStats(*DataManager._bytes_converter(_stats.st_size, True))})
        return _os_stats
    
    @staticmethod
    def _get_time():
        return datetime.now().strftime('%Y%m%dT%I-%M-%S%p')
    
    def _format_file(self, __file='file', *, with_id=True):
        return f'{self._get_time()}_{__file}_metadata{f'_{self.__ID}' if with_id else ''}.json'
    
    @cached_property
    def all_stats(self):
        if self._all_stats is None:
            self._all_stats = self._get_stats()
        return self._all_stats
    
    @property
    def module(self):
        _module = self._module
        _formatter = self._format_file
        if _module is None:
            _module = _formatter()
            return _module
        
        try:
            _module = _formatter(Path(_module).stem, with_id=False)
        except tuple(self._all_errors()):
            _module = _formatter()
        return _module
    
    @property
    def serializer(self):
        _defaults = ('dataclass', 'dataclass_json', True)
        return self.compiler(_defaults, self._serializer)
    
    def _exporter(self, __data: DynamicDict):
        _data = __data.to_json() if all((hasattr(__data, 'to_json'),
                                            isinstance(__data, DynamicDict),
                                            self.serializer)) \
                                        else __data
        _file = self.module
        with open(_file, 'w', encoding='utf-8') as stats_file:
            json.dump(_data, stats_file, indent=4)
    
    def export_stats(self, __other=None):
        _stats = __other or self.all_stats
        try:
            self._exporter(_stats)
        except:
            _no_posix = DynamicDict({posix.stem: _v for posix, _v in _stats.items()})
            self.export_stats(_no_posix)
        return f'\033[34m{self.module!r}\033[0m has been successfully exported.' + \
                (lambda _serial: '' if not _serial else f' (Serialized as {dataclass_json.__name__!r})')(self.serializer) 

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


def main():
    dl = DataLoader
    dm = DataManager
    cii = ConfigManager

    nltk = dl(f'{Path.home()}/nltk_data/corpora/stopwords', dynamic=True, all_exts=True, no_method=False, posix=True, verbose=True, data_manager=False, module='NLTK', generator=False)()
    print(nltk)

if (__main:=__name__) == '__main__':
    _dl_name = DataLoader.__class__.__name__
    parser = argparse.ArgumentParser(description=f'{_dl_name}: A powerful data loading utility.')
    parser.add_argument('message', nargs='?', default=f'Executing {__main!r}', help=f'Title message for the execution of {__main}.')
    parser.add_argument('--verbose', action='store_true', help=f'Enable verbose mode for {_dl_name!r}.')
    args = parser.parse_args()
    _message = args.message
    _verbose = args.verbose or True
    
    with Timer(message=_message, verbose=_verbose):
        main()