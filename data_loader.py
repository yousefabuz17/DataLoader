import os
import re
import sys
import json
import shutil
import hashlib
import inspect
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
from benedict import benedict
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict, namedtuple, defaultdict
from functools import cached_property, partial, cache, wraps
from itertools import filterfalse, chain, count, permutations, tee, zip_longest
from configparser import ConfigParser, NoSectionError, MissingSectionHeaderError
from typing import Any, AnyStr, Dict, Generator, IO, \
                    Iterable, NamedTuple, List, Tuple, LiteralString
from json.decoder import JSONDecodeError
from pandas.errors import DtypeWarning, EmptyDataError, ParserError
from constants import _ERRORS, _PASS
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr
from abc import ABCMeta, abstractmethod
from dataclasses_json import dataclass_json
import asyncio
import aiofiles


__all__ = ('DataLoader', 'DataManager', 'DynamicDict', 'DynamicGen')

def create_logger(level=logging.DEBUG,
                    formatter_kwgs=None,
                    handler_kwgs=None,
                    write_log=True):

    _logger = logging.getLogger(__name__)
    _levels = np.arange(1, 6) * 10
    
    if level in _levels:
        _logger.setLevel(level)
    
    _formatter_kwgs = {**{'fmt': '[%(asctime)s][LOG %(levelname)s]:%(message)s',
                            'datefmt': '%Y-%m-%d %I:%M:%S %p'},
                       **(formatter_kwgs or {})}
    _handler_kwgs = {**{'filename': f'{Path(__file__).stem}.log', 'mode': 'a'},
                    **(handler_kwgs or {})}
    
    formatter = logging.Formatter(**_formatter_kwgs)
    handler = logging.FileHandler(**_handler_kwgs)
    handler.setFormatter(formatter)
    
    if write_log:
        _logger.addHandler(handler)
    
    return _logger

logger = create_logger(write_log=False)

class DynamicThread:
    _lock = None
    _executor = None
    
    def __repr__(self):
        return f'{self.__class__.__name__}(_lock={self.THREAD_LOCK}, _executor={self.THREAD_EXECUTOR})'
    
    __str__ = __repr__
    
    def __iter__(self):
        return iter(self.get_threads())
    
    def __enter__(self):
        return self.get_threads()
    
    def __exit__(self, *args):
        self._lock = self._executor = None
    
    #** Outside of ThreadPool to ensure it prints only once for each execution(__path)
    @classmethod
    def _dl_initializer(cls, __path=repr(__file__),
                            executor_only=False,
                            max_workers_only=False,
                            thread_kwargs=None):
        
        _max_workers = min(32, (os.cpu_count() or 1) + 4)

        _thread_kwargs = thread_kwargs or {'max_workers': _max_workers,
                                            'thread_name_prefix': 'DLExecutor'}
        
        dl_executor = ThreadPoolExecutor(**_thread_kwargs)
        if any((executor_only, max_workers_only)):
            return [dl_executor, _max_workers][int(max_workers_only)]
        
        _thread_prefix = dl_executor._thread_name_prefix
        _thread_count = threading.active_count()
        _main_thread = threading.current_thread().name
        _repr = '\n\033[34m[{}]\033[0m Successfully initialized {} {} worker{} for \033[1;32m{!r}\033[0m\n'
        print(_repr.format(_thread_prefix, _thread_count,
                            _main_thread, _Generic._s_plural(_thread_count),
                            __path))
    
    def get_threads(cls):
        return (cls.THREAD_LOCK, cls.THREAD_EXECUTOR)
    
    @property
    def THREAD_EXECUTOR(self):
        if self._executor is None:
            self._executor = self._dl_initializer(executor_only=True)
        return self._executor
    
    @property
    def THREAD_LOCK(self):
        if self._lock is None:
            self._lock = threading.Lock()
        return self._lock
    
    def _print_header(self):
        _max_workers = self._dl_initializer(max_workers_only=True)
        _terminal_size = self._terminal_size().columns
        _prefix = (lambda _self: f'({''.join(filter(str.isupper, _self))}){_self} MAXWORKERS={_max_workers}')(self.__class__.__name__)
        _divider = _prefix.center(_terminal_size, '-')
        print(f'\033[1;32m{_divider}\033[0m')

class _Generic(DynamicThread, metaclass=ABCMeta):
    _NAMEDTUPLES = {}
    _CLASSES = {}
    
    def __init_subclass__(cls):
        cls._CLASSES[cls.__name__] = cls
    
    @abstractmethod
    def __missing__(cls, *args, **kwargs):
        raise DLoaderException(*args, **kwargs)
    
    @classmethod
    @abstractmethod
    def _repr(cls, __obj, module=None, display_all=False):
        get_inherited_cls = cls.get_inherited_cls
        _gen_types = (get_inherited_cls('DataLoader'), Generator, get_inherited_cls('DynamicGen'))
        _dict_types = (dict, get_inherited_cls('DynamicDict'), DynamicDict._get_benedict(), benedict)
        _obj = (lambda _x: _x.items() if isinstance(_x, _dict_types) else _x)(__obj)
        _cls_name = cls._cap_cls_name(cls) if not module else module
        _place_holder = '{}({})'
        if type(__obj) == _gen_types[0] \
            or _cls_name == _gen_types[0].__name__ \
            or isinstance(__obj, _gen_types[:2]):
            
            return f'<generator object {_cls_name}.files.<key-value> at {hex(id(cls))}>'
        try:
            _items, _gen_items = tee((k, cls._too_large(v, display_all=display_all),
                                        os.stat(k).st_size if isinstance(k, Path) else 0) 
                                        for k, v in _obj)
            _total_bytes = cls._bytes_converter(sum(i[-1] for i in _items))
            if not _total_bytes:
                _total_bytes = None
            
            _string = _place_holder.format(
                                _cls_name,
                                f',\n{' ':>{len(_cls_name)+1}}'.join(
                                (f'({k}{f', {v}' if hasattr(v, '__str__') else ''}{', '+cls._bytes_converter(_b, symbol_only=True).symbolic_size if isinstance(k, Path) else ''})' \
                                for k, v, _b in _gen_items)
                                )
                            )
        
        except tuple(cls._all_errors()) as _errors:
            raise _errors
        
        return _string if not _total_bytes else f'[{_total_bytes.symbolic_size}]\n{_string}'
    
    @abstractmethod
    def __sizeof__(self):
        _gen_contents = self.items() if hasattr(self, 'items') or isinstance(self, dict) else self
        _total = sum(os.stat(k).st_size if Path(k).is_file() else 0 for k,_v in _gen_contents)
        _bytes_conveter = _Generic._bytes_converter
        _not_posix = not _total and not any((isinstance(_path, Path) for _path,_v in _gen_contents))
        _bytes_stats = _bytes_conveter(_total, not_posix=_not_posix)
        if not _bytes_stats:
            #XXX itertools.tee instances
            _error_message = self.get_inherited_cls('DataLoader').__name__, \
                            f'{self.__class__.__name__ +'.'+self.__sizeof__.__name__!r}'
            DLoaderException(50, *_error_message)
            return
        return _bytes_stats
    
    @staticmethod
    def _terminal_size():
        return shutil.get_terminal_size()
    
    @staticmethod
    def _get_params(__method):
        try: 
            _sig = inspect.signature(__method)
        except TypeError:
            raise DLoaderException(800, __method)
        return dict.fromkeys(_sig.parameters)
    
    @staticmethod
    def _dl_raise(__errors, /, _raise=False, *, pre='', post='', verbose=False):
        _error = type(__errors)(__errors) if callable(__errors) else __errors
        _args = (None, _string:=f'{pre}{_error}{post}')
        _raise_dle = partial(DLoaderException, *_args)
        if _raise:
            raise _raise_dle()
        elif verbose:
            _raise_dle()
        return _string
    
    @classmethod
    @abstractmethod
    def _all_errors(cls):
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
    
    @classmethod
    @abstractmethod
    def _validate_path(cls, __path, verbose=False):
        dle = DLoaderException
        
        try:
            path = Path(__path)
        except tuple(cls._all_errors()) as _errors:
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
        
        elif cls.compiler(r'^[._]', path.stem):
            if verbose:
                dle(None, message=f'Skipping {path.name!r}', _log_method=logger.warning)
            return
        
        return path
    
    @classmethod
    def _cap_cls_name(cls, __cls):
        return (lambda _cls: _cls.capitalize() if not _cls[0].isupper() else _cls)(__cls.__class__.__name__)
    
    @classmethod
    def _bytes_converter(cls, __num, /, symbol_only=False, total_only=False, *, not_posix=False, verbose=False):
        #XXX (KB)-1024, (MB)-1048576, (GB)-1073741824, (TB)-1099511627776
        if not __num:
            return
        
        Stats = cls.create_subclass('Stats', ('symbolic_size', 'calculated_size', 'bytes_size'))
        _base = 1024
        _exp = np.arange(1,5)
        _conversions = dict(zip(('KB (Kilobytes)', 'MB (Megabytes)',
                                'GB (Gigabytes)', 'TB (Terabytes)'),
                                np.power(_base, _exp)))
        results = next((f'{(_total:=__num/v):.2f} {k[:2] if symbol_only else k}', _total, __num)
                        for k,v in _conversions.items() if (__num/_base)<v)
        
        if not _total:
            _error = _ERRORS.get(25, -1000).format('DataLoader')
            cls._dl_raise(_error, _raise=False, post=f'\n>>Value passed: {__num}', verbose=verbose)
            if not_posix:
                return (*results, f'<ERROR {repr('CODE 25')}>')
        
        if total_only:
            return _total
        
        return Stats(*results)
    
    @classmethod
    @cache
    def create_subclass(cls, typename='FieldTuple', /,
                            field_names=None, *,
                            rename=False, module=None,
                            defaults=None, **kwargs):
        """
        Create a dynamically generated namedtuple subclass.

        Parameters:
        - typename (str): Name of the named tuple subclass.
        - field_names (List[str]): List of field names.
        - rename (bool): Whether to rename invalid field names.
        - module (str): Module name for the namedtuple subclass.
        - defaults (Tuple): Default values for fields.
        - num_attrs (int): Number of default attributes if field_names is not provided.
        - **kwargs: Additional parameters.
            - num_attrs (int): The number of default attributes assigned to the object when no specific field names are provided.
            - field_docs (str): List of documentation strings for each field.

        Returns:
        - Named tuple subclass.
        """
        _tuples = cls._NAMEDTUPLES
        try:
            return _tuples.get(module) or cls.get_subclass(typename)
        except KeyError:
            _tuples[typename] = {}
        
        num_attrs = kwargs.pop('num_attrs', 5)
        if not isinstance(num_attrs, int) or num_attrs <= 0:
            raise ValueError(f"{num_attrs!r} is not a positive integer.")

        _field_names = field_names or np.core.defchararray.add('attr', np.arange(1, num_attrs+1).astype(str))
        _none_generator = lambda _type=None: (_type,) * len(_field_names)
        _defaults = defaults or _none_generator()
        _field_docs = kwargs.pop('field_docs', _none_generator(''))
        _module = module or typename
        _new_tuple = namedtuple(typename=typename,
                                field_names=_field_names,
                                rename=rename,
                                defaults=_defaults,
                                module=_module)
        setattr(_new_tuple, '__doc__', _field_docs)
        _tuples[_module] = _new_tuple
        return _new_tuple
    
    @classmethod
    def get_subclass(cls, __subclass):
        try:
            return cls._NAMEDTUPLES[__subclass]
        except KeyError:
            cls.__missing__(None, f'{NamedTuple.__name__} {__subclass!r} has not been created yet')
    
    @classmethod
    def get_inherited_cls(cls, __cls):
        return cls._CLASSES.get(__cls, __cls)
    
    @property
    def hashed_files(cls):
        return cls._HASHED_FILES
    
    @classmethod
    def compiler(cls, __defaults, __k):
        if any((not __k, not isinstance(__k, str))) and hasattr(__k, '__str__'):
            __k = str(__k)
        
        _defaults = map(re.escape, map(str, __defaults))
        _k = __k if isinstance(__k, str) \
                else '|'.join(map(re.escape, __k))
                
        _compiled = re.compile('|'.join(_defaults), re.IGNORECASE).match(_k)
        return bool(_compiled)
    
    @classmethod
    def _s_plural(cls, __word):
        _base = '{}'.format
        _hasattr = partial(hasattr, __word)
        if (_hasattr('__len__') and len(__word)>1) \
            or (_hasattr('__gt__') and __word>1):
            
            return _base('s')
        
        return _base('')
    
    @classmethod
    def _new_config(cls):
        return CConfigParser(allow_no_value=True,
                            delimiters='=',
                            dict_type=DynamicDict,
                            converters={'*': CConfigParser.convert_value}
                            )
    
    def reset(cls):
        try:
            return \
                [OrderedDict(cls.items()), ((*kv,) for kv in cls)] \
                [isinstance(cls, (DataLoader, Generator))]
        except:
            pass
        return cls
    
    @classmethod
    def _too_large(cls, value, max_length=None, display_all=False):
        if display_all:
            return value
        
        _max_length = max_length if isinstance(max_length, int) \
                        else cls._terminal_size().columns
        
        _cls = cls._cap_cls_name(value)
        _cls_tag = f'<{_cls}>'
        try:
            _length = len(str(value))
        except TypeError:
            _length = None
        
        if any((
                (_length is not None) and (_length >= _max_length),
                (hasattr(value, '__str__') and len(str(value)) >= _max_length),
                isinstance(value, Generator),
                isinstance(value, (pd.DataFrame, pd.Series)),
                type(value)==type
                )):
            return _cls_tag
        return value
    
    @staticmethod
    def _rm_period(__path):
        return __path.lstrip('.').lower()
    
    @staticmethod
    def _get_type(__cls_files):
        for _i,j in __cls_files:
            yield type(j)

@dataclass_json
class DynamicDict(OrderedDict, _Generic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    class Benedict(benedict):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
        
        def __repr__(self):
            return _Generic._repr(self, module='Benedict')
        
        __str__ = __repr__
        
        def __sizeof__(self):
            raise DLoaderException(None, f'{DynamicDict.__name__!r} instances only.')
    
    def __missing__(self, *args, **kwargs):
        get_inherited_cls = lambda __key: self.get_inherited_cls(__key).__name__
        _super = _Generic.__missing__
        if all((args, kwargs)):
            _super(self, *args, **kwargs)
        
        _super(self, 1, self.__class__.__name__, *map(get_inherited_cls, (DynamicDict, benedict, 'DataLoader', 'DataLoader')))
    
    @recursive_repr()
    def __repr__(self):
        return self._repr(self, module=DynamicDict.__name__)
    
    __str__ = __repr__
    
    def __sizeof__(self):
        return _Generic.__sizeof__(self)
    
    def __getattr__(self, __item):
        return self[__item]
    
    def __getitem__(self, __item):
        if not self.__contains__(__item):
            if (_right_key:=self._possible_key(__item)):
                return self.get(_right_key)
        
        return self.get(__item)
    
    def __setattr__(self, __attr, __value):
        if __attr!='_full_repr':
            self[__attr] = __value
    
    @classmethod
    def _repr(cls, *args, **kwargs):
        return _Generic._repr(*args, **kwargs)
    
    @classmethod
    def _all_errors(cls):
        return _Generic._all_errors()
    
    @classmethod
    def _validate_path(cls, *args, **kwargs):
        return _Generic._validate_path(*args, **kwargs)
    
    def _posix_converter(self, __items):
        _posix = lambda _p, _slice=False: (_p[0], _p[1]) \
                                            if _slice else _p
        try:
            if all((isinstance(i, Iterable) and len(i)>1) for i in __items):
                
                return list(map(lambda _i: _posix(_i, _slice=True), __items))
            return list(map(_posix, __items))
        
        except tuple(self._all_errors()) as _errors:
            self._dl_raise(_errors, verbose=self._verbose)
    
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
    
    def to_benedict(self):
        return self._get_benedict(self)
    
    @staticmethod
    def _get_benedict():
        return DynamicDict.Benedict

class CConfigParser(ConfigParser):
    def __init__(self, *args, dict_type=DynamicDict, default_section='CC-DEFAULT', allow_no_value=True, **kwargs):
        _kwargs = {
                    'dict_type': dict_type,
                    'allow_no_value': allow_no_value,
                    'default_section': default_section,
                    'converters': {'*': CConfigParser.convert_value},
                    **kwargs
                }
        super().__init__(*args, **_kwargs)

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
        Encrypter = _Generic.create_subclass('Encrypter', ('text', 'key'))
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        encrypted_bytes = cipher_suite.encrypt(text.encode())
        encrypted_text = encrypted_bytes.hex()
        encrypted_data = Encrypter(encrypted_text, key)
        if export:
            cls._exporter(text, encrypted_data, ini_name)
        
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
        _config_parser = _Generic._new_config()
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
            try:
                return str_code.format(*holders)
            except:
                return args
        except AttributeError as attr_error:
            raise attr_error(_human_error)
    
    def _log_error(self, *args):
        # traceback_ = traceback.format_exc(limit=1)
        self._log_method(f'{self.match_error(*args)}')

@dataclass(slots=True, weakref_slot=True, order=True)
class Timer:
    message: AnyStr = field(default='')
    verbose: bool = field(default=False, kw_only=True)
    
    _start_time: float = field(init=False, default_factory=lambda: time, repr=False)
    _end_time: float = field(init=False, default_factory=lambda: time, repr=False)
    
    def __enter__(self):
        self._start_time = self._start_time()
        return self._start_time

    def __exit__(self, *args, **kwargs):
        elapsed_time = self._end_time() - self._start_time
        minutes, seconds = divmod(elapsed_time, 60)
        if self.verbose:
            if self.message:
                print(f'\033[33m{self.message!r}\033[0m')
            print(f'\033[32mExecution Time:\033[0m {minutes:.0f} minutes and {seconds:.5f} seconds.')

@dataclass(slots=True, weakref_slot=True)
class Extensions:
    DEFAULTS: Dict = field(init=False)
    ALL: Dict = field(init=False)
    
    def __post_init__(self):
        rm_p = _Generic._rm_period
        _defaults = self.__defaults__()
        _mimetypes = mimetypes.types_map
        _mimetypes['xlsx'] = None
        ExtInfo = self.__subclass__()
        _all_exts = set(ExtInfo(rm_p(i), pd.read_excel if rm_p(i) in ('xls','xlsx') else open) \
                        for i in _mimetypes \
                        if rm_p(i) not in _defaults)
        self.DEFAULTS = {ext: _v for ext,_v in _defaults.items() if ext!='empty'}
        self.ALL = {**{ext.suffix_: ext for ext in _all_exts},
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
        return f'{self.__class__.__name__}({', '.join('{}={}'.format(*i) for i in self.__getstate__().items())})'
    
    __str__ = __repr__
    
    def __iter__(self):
        return iter(self.get_attrs())
    
    def __getstate__(self):
        return {i.name: list(getattr(self, i.name)) for i in fields(self)}
    
    def __subclass__(self):
        return _Generic.create_subclass('ExtInfo', ('suffix_', 'loader_'))
    
    def get_attrs(self):
        return (self.DEFAULTS, self.ALL)
    
    def to_json(self, __type=DynamicDict):
        #XXX __type: DynamicDict | benedict
        try:
            return __type(self.ALL).to_json()
        except:
            raise DLoaderException(None, f'{__type} has no attribute {repr('to_json')}')


class DynamicGen(Iterable, _Generic):
    __slots__ = ('__weakref__', '__keyvalue_gen', '__full_repr')
    
    def __init__(self, __keyvalue_gen, full_repr=False):
        self.__keyvalue_gen = __keyvalue_gen
        self.__full_repr = full_repr
    
    def __missing__(self, *args, **kwargs):
        _Generic.__missing__(self, *args, **kwargs)
    
    def __repr__(self):
        return self._repr(self.__keyvalue_gen, module=DynamicGen.__name__, display_all=self.__full_repr)
    
    __str__ = __repr__
    
    def __sizeof__(self):
        return _Generic.__sizeof__(self.__keyvalue_gen)

    def __iter__(self):
        try:
            next(iter(self.__keyvalue_gen))
        except StopIteration:
            return iter([f'{self.__class__.__name__} Exhausted'])
        return iter(self.__keyvalue_gen)
        
    def __len__(self):
        return 0 if not self.__keyvalue_gen else sum(1 for __kv in self.__keyvalue_gen)
    
    def __bool__(self):
        return bool(self.__len__())
    
    @classmethod
    def _repr(cls, *args, **kwargs):
        return _Generic._repr(*args, **kwargs)
    
    @classmethod
    def _all_errors(cls):
        return _Generic._all_errors()
    
    @classmethod
    def _validate_path(cls, *args, **kwargs):
        return _Generic._validate_path(*args, **kwargs)
    
    def _missing(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.__missing__(self, None)
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
                '__files', '_posix', '__ID')
    
    _ID = count(1)
    _TIMER = Timer
    _DEFAULTS, _ALL = Extensions()
    _HASHED_FILES = DynamicDict()
    _KWARG_KEYS = frozenset(('dynamic', 'no_method',
                            'module', 'generator',
                            'verbose', 'allow_empty_files',
                            'manage_data', 'dynamic_with_benedict',
                            'all_exts', 'full_repr'))
    
    __MAXW = False
    
    def __init__(self, __path, defaults=None, posix=True, **kwargs):
        self.ext_path = __path
        self.defaults = defaults
        self.__files = None
        self._posix = posix
        self.kwargs = kwargs
        self.__post_init__(**self.kwargs)
    
    def __post_init__(self, **kwargs):
        _missing = self.__missing__
        _not_boolean = ('module')
        for _key in self._KWARG_KEYS:
            _setattr = partial(self.__setattr__, f'_{_key}')
            _attr_key = kwargs.pop(_key, False) or False
            if _key not in _not_boolean:
                _setattr(_attr_key) if isinstance(_attr_key, bool) else \
                        _missing(None, f'{_key!r} attribute must be boolean')
            else:
                _setattr(None)
        
        _not_allowed_together = ('dynamic', 'generator',
                                'dynamic_with_benedict',
            _needs_posix_enabled:=('manage_data'))
        
        _data_formatters = permutations((
                                    self._dynamic,
                                    self._dynamic_with_benedict,
                                    self._generator,
                                    self._manage_data
                                    ))
        
        _not_allowed = next(map(sum, _data_formatters))>=2
        
        if not self.ext_path:
            _missing(200, self.__class__.__name__, self.ext_path)
        
        elif all((self.defaults, self._all_exts)):
            _missing(202, self.__class__.__name__, list(self._DEFAULTS))
        
        elif _not_allowed:
            _missing(201, _not_allowed_together, DataLoader.__name__)
        
        elif all((not self._posix,
                self._manage_data)):
            
            DLoaderException(371, _needs_posix_enabled)
            self._posix = True
        
        _ext_path = self.ext_path = self._validate_path(self.ext_path, verbose=self._verbose)
        
        if _ext_path.is_file() and not _ext_path.is_dir():
            self.files =  self.load_file(_ext_path, **kwargs)
            return self.__call__(self.files)
        
        self.defaults = self._validate_exts(self.defaults)
    
    def __missing__(self, *args, **kwargs):
        _missing = super(DynamicGen, self).__missing__
        if any((args, kwargs)):
            _missing(*args, **kwargs)
        _missing(None)
    
    def __repr__(self):
        return self._repr(self.__call__(), module=DataLoader.__name__, display_all=self._full_repr)
    
    __str__ = __repr__
    
    def __call__(self, __other=None):
        _files = __other or self.files
        if not _files:
            self.__missing__(0, '')
        
        if all((self._dynamic,
                not isinstance(_files, DynamicDict))):
            
            return DynamicDict(_files, full_repr=self._full_repr)
        
        elif self._manage_data:
            return self.manage_data()
        
        elif self._dynamic_with_benedict:
            return DynamicDict.Benedict(_files)
        
        elif self._generator \
            and (hasattr(_files, '__next__') 
                and callable(getattr(_files, '__next__'))):
            
            return DynamicGen(_files, self._full_repr)
        
        return self
    
    def __sizeof__(self):
        return _Generic.__sizeof__(self.files)
    
    @classmethod
    def _repr(cls, *args, **kwargs):
        return super()._repr(*args, **kwargs)
    
    @classmethod
    def _all_errors(cls):
        return super()._all_errors()
    
    @classmethod
    def _validate_path(cls, *args, **kwargs):
        return super()._validate_path(*args, **kwargs)
    
    @cached_property
    def _get_files(self):
        _defaults = self._DEFAULTS
        
        if self.defaults:
            _defaults = self._validate_exts(self.defaults)
        elif self._all_exts:
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
            _exts = set(__exts)
            _valid_exts = set(
                            _ext \
                            for _ext in _exts \
                            if _ext in map(self._rm_period, self._ALL)
                            )
            
            _failed = set(filterfalse(lambda ext: ext in _valid_exts, __exts))
        except tuple(self._all_errors()) as _errors:
            self._dl_raise(_errors, post=f'\n>>Extension argument provided: {__exts!r}', verbose=self._verbose)
        
        if _failed==_exts:
            raise DLoaderException(210, self._s_plural(len(_failed)), _failed, list(self._ALL))
        
        if _failed and self._verbose:
            DLoaderException(215, _failed, _log_method=logger.warning)
        
        return _valid_exts
    
    def _ext_method(self, __path):
        _suffix = self._rm_period(__path.suffix)
        _method = None
        _all = self._ALL
        
        if _suffix==_all['empty'].suffix_:
            _method = _all['empty'].loader_
        elif _suffix in _all:
            _method = _all[_suffix].loader_
        else:
            if self._verbose:
                DLoaderException(702,
                            __path.name,
                            _method,
                            _method:=open)
            
        return _method
    
    def _check_ext(self, __path):
        _method = self._ext_method(__path)
        _kwargs = {param: value for param, value in self.kwargs.items()
                    if param in self._get_params(_method)
                    and value is not None}
        
        _verbose = self._verbose
        if _kwargs and _verbose:
            DLoaderException(-1, _log_method=logger.info)
        
        return self._load_file(__path, _method, _kwargs)
    
    def inject_files(self, *__dirs, **__kwargs):
        if all((hasattr(__dirs, '__len__'),
                not len(__dirs)>=1)):
            
            raise DLoaderException(150)
        
        _kwargs = {
                    **self.kwargs,
                    'module': self.id,
                    'all_exts': self._all_exts,
                    **__kwargs
                }
        
        with self.THREAD_LOCK:
            _all_files = self.THREAD_EXECUTOR.map(partial(DataLoader,
                                                            **_kwargs),
                                                            __dirs)
        
        _gen_files = ((_cls.ext_path, _cls.__call__()) for _cls in _all_files)
        _type = next(self._get_type(_gen_files))
        
        if self._dynamic and _type==DynamicDict:
            self.files = DynamicDict(self.files, full_repr=self._full_repr)
            self.files.update(DynamicDict(_gen_files, full_repr=self._full_repr))
            return self.__call__(self.files)
        
        self.files = chain(((k,v) for k,v in self.files), _gen_files)
        return self.__call__(self.files)
    
    @classmethod
    @cache
    def load_file(cls, file_path, **__kwargs):
        _kwargs = {} or __kwargs
        _path = cls._validate_path(file_path, verbose=_kwargs.get('verbose'))
        p_method = cls._ext_method(cls, _path)
        loaded_file = cls._load_file(cls, _path, p_method, _kwargs)
        return loaded_file
    
    def _load_file(cls, __path, __method, __kwargs):
        _p_name = '/'.join(__path.parts[-2:])
        p_contents = None
        _kwargs = cls.kwargs if hasattr(cls, 'kwargs') else __kwargs
        _get = partial(_kwargs.get, False)
        _verbose = cls._verbose if hasattr(cls, '_verbose') else _get('verbose')
        _allow_empty = cls._allow_empty_files if hasattr(cls, '_allow_empty_files') else _get('allow_empty_files')
        _method = open if (hasattr(cls, '_no_method') and getattr(cls, '_no_method') is True) and not _get('no_method') else __method
        cls._rm_cls_kwargs(_kwargs)
        with cls._TIMER(message=f'Executing {_p_name!r}', verbose=_verbose):
            try:
                p_contents = _method(__path, **{})
            except tuple((_errors:=cls._all_errors())) as _error:
                if _verbose:
                    _exception = type(_error)
                    _error_code = _errors.get(_exception, 500)
                    _placeholder_count = _ERRORS.get(_error_code).count('{}')
                    _raise = partial(DLoaderException, _error_code, _p_name, _log_method=logger.warning)
                    if _exception == JSONDecodeError:
                        _raise(_error.pos, _error.lineno, _error.colno)
                    elif _placeholder_count == 2:
                        _raise(_error)
                    elif _exception == Exception:
                        raise _exception
                    else:
                        _raise()
                
                p_contents = 0
            
            _path = cls._posix_converter(__path, posix=cls._posix)
            _hashed_value = cls.calculate_hash(__path)
            try:
                if not isinstance((_id:=cls.id), property) \
                    and _id not in (_hashed:=cls._HASHED_FILES):
                    
                    _hashed[_id] = []
                
                _hashed[_id].append({__path: _hashed_value})
            
            except TypeError as t_error:
                if not cls.check_hash(__path):
                    raise DLoaderException(270, f'{t_error}')
            
            return cls._check_empty(_path, p_contents, allow_empty=_allow_empty, verbose=_verbose)
    
    @classmethod
    def _check_empty(cls, *args, **kwargs):
        _p, _contents = args
        _allow_empty = kwargs.setdefault('allow_empty', False)
        _verbose = kwargs.setdefault('verbose', False)
        
        PathInfo = _Generic.create_subclass('PosInfo', ('path_', 'contents_'))
        _PI = partial(PathInfo, path_=_p)
        _hattr = lambda __name: hasattr(_contents, __name)
        _is_empty = False
        try:
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
        except tuple(cls._all_errors()) as dl_error:
            if _verbose:
                DLoaderException(None,
                                f'>>ERROR on {DataLoader.__name__}.{DataLoader._check_empty.__name__!r}:\n{dl_error}')
            _is_empty = True
        
        if _is_empty and not _allow_empty:
            return _PI()
        
        return _PI(contents_=_contents)
    
    @cache
    def _execute_path(self):
        _TLOCK, _EXECUTOR = self.get_threads()
        
        if self._verbose:
            _verbose_path = '/'.join(self.ext_path.parts[-2:])
            logger.write_log = True
            if not DataLoader.__MAXW:
                DataLoader.__MAXW = True
                self._print_header()
            
            _EXECUTOR._initializer = self._dl_initializer(_verbose_path)
        
        _files = None
        try:
            with _TLOCK:
                _files = _EXECUTOR.map(self._check_ext, self._get_files)
        
        except tuple(self._all_errors()) as _errors:
            self._dl_raise(_errors, verbose=_verbose)
        
        return ((*file,) for file in _files if file.contents_ is not None)
    
    @cached_property
    def files(self):
        _files = self.__files
        
        if _files is None:
            _files, _gen = tee(self._execute_path())
        #     _stems_allowed = all((not self._posix, not self._manage_data, self.compare_posix(_files)))
        # if _stems_allowed:
        #     return ((Path(k).stem, v) for k,v in _gen)
        return _gen
    
    @classmethod
    def _stems_allowed(cls, __paths):
        _stem_converter = lambda _p: Path(_p).stem
        _check_diffs = lambda _set1, _set2: len(_set1)==len(_set2)
        try:
            _keys = list(zip(*__paths))[0]
            _args = list(map(_stem_converter, _keys))
        except:
            raise DLoaderException(170, f'{DataLoader._stems_allowed.__name__!r}')
        
        return _check_diffs(_args, set(_args))
    
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
        
        _hashed = cls.calculate_hash((_path:=cls._validate_path(__path)))
        if (_result:=_all_hashed.get(_path)):
            return _hashed == _result
        raise DLoaderException(None, message=f'{__path} has no matches to compare too.\nOriginal hash value:\n{_hashed}')
    
    @property
    def hashed_files(cls):
        return cls._HASHED_FILES
    
    @cached_property
    def id(self):
        return f'ID_{next(DataLoader._ID)}' if not self._module else self._module
    
    @cached_property
    def total_size(self):
        return self.__sizeof__()
    
    @staticmethod
    def _posix_converter(__path, __kwargs=None, posix: bool=False):
        if __kwargs and not posix:
            return __path if __kwargs.get('posix') else getattr(Path(__path), 'name')
        return Path(__path).stem if not posix else __path
    
    @classmethod
    def add_files(cls, *__files, **__kwargs):
        if not len(__files)>=1:
            raise DLoaderException(220)
        
        _posix = cls._posix_converter
        kwargs = deepcopy(__kwargs)
        _gen_only = __kwargs.get('generator')
        _verbose = __kwargs.get('verbose')
        cls._rm_cls_kwargs(__kwargs)
        
        with DynamicThread() as _threading:
            loaded_files = _threading[1].map(partial(cls.load_file, **__kwargs),
                                                (path for path in map(partial(cls._validate_path, verbose=_verbose), __files)))
            
        __files = ((_posix(file.path_, kwargs), file.contents_) for file in loaded_files if file.contents_ is not None)
        
        if kwargs.get('dynamic') and not isinstance(cls, DynamicDict) and not _gen_only:
            return DynamicDict(__files)
        return __files
    
    @classmethod
    def _rm_cls_kwargs(cls, __kwargs):
        for i in cls._KWARG_KEYS:
            __kwargs.pop(i, False)
    
    @classmethod
    def add_dirs(cls, *__dirs, **__kwargs):
        if not len(__dirs)>=1:
            raise DLoaderException(220)
        
        _org_kwargs = deepcopy(__kwargs)
        _verbose = __kwargs.pop('verbose', False)
        _directories = map(partial(cls._validate_path, verbose=_verbose), __dirs)
        _dynamic = __kwargs.pop('dynamic', False)
        _merge = __kwargs.pop('merge', False)
        _defaults = __kwargs.pop('defaults', None)
        _all = __kwargs.pop('all_exts', False)
        _gen_only = __kwargs.pop('generator', False)
        
        _posix = cls._posix_converter
        _THREAD_AND_LOCK = DynamicThread()
        if _merge:
            with _THREAD_AND_LOCK as _threading:
                loaded_directories = (cls.load_file(j, **__kwargs) \
                                    for i in _threading[1].map(partial(cls.get_files, defaults=_defaults), _directories) \
                                    for j in i)
            __files = ((_posix(p.path_, __kwargs), p.contents_) for p in loaded_directories if p.contents_ is not None)
            
            if any((_dynamic and not isinstance(cls, DynamicDict),
                    not _gen_only)):
                
                return [DynamicDict(__files), __files][int(_gen_only)]
            
            return __files
        
        
        with _THREAD_AND_LOCK as _threading:
            loaded_directories = _threading[1].map(partial(cls, defaults=_defaults, all_exts=_all, verbose=_verbose, **_org_kwargs), _directories)
        
        __files = ((_posix(__cls.ext_path, __kwargs), __cls.files) for __cls in loaded_directories)
        
        __type = next(cls._get_type(__files))
        
        if _dynamic and not __type==DynamicDict and not _gen_only:
            return DynamicDict(__files)
        return __files
    
    def manage_data(self):
        if not isinstance(self.files, (dict, DynamicDict)) and self._manage_data:
            return DataManager(DynamicDict(self.files))
        raise DLoaderException(None, f'Object must be a dictionary instance not {type(self)}. Not suitable for {DataManager.__name__}')
    
    @staticmethod
    def load_sql(__database=''):
        class DataSQL: pass
        return DataSQL
    
    @staticmethod
    def load_config(**kwargs):
        if kwargs.pop('instance_only', False):
            return ConfigManager
        return ConfigManager(**kwargs)

class DataManager(_Generic):
    __slots__ = ('_paths', '_module', '_serializer',
                '_posix', '_all_stats', '_ID')
    
    __ID = count(1)
    
    def __init__(self, __paths, serializer=None, posix=True, module=None):
        self._paths = __paths
        self._module = module
        self._serializer = serializer
        self._posix = posix
        self._all_stats = None
        self._ID = next(self.__ID)
        self.__post_init__()
    
    def __post_init__(self):
        _paths = self._paths
        try:
            if isinstance(_paths, (Generator, DynamicGen)):
                self._type_error()
            _paths = _paths.keys() if isinstance(_paths, dict) else list(_paths)
            for _p in _paths:
                self._validate_path(_p, True)

        except tuple(self._all_errors()) as dl_error:
            self._type_error()+f'\n>>ERROR: {dl_error}'
    
    def __missing__(self, *args, **kwargs):
        return _Generic.__missing__(self, *args, **kwargs)

    def __repr__(self):
        return super()._repr(zip_longest(self._paths, [None]), module=DataManager.__name__)
    
    __str__ = __repr__
    
    def __iter__(self):
        return iter(self._paths)
    
    def __call__(self):
        return self.all_stats
    
    def __sizeof__(self):
        return _Generic.__sizeof__(self.all_stats)
    
    def _type_error(self):
        self.__missing__(50, DataLoader.__name__,
                        f'{type(self._paths)!r} must be a dictionary type not Generator.')
    
    @classmethod
    def _repr(cls, *args, **kwargs):
        return super()._repr(*args, **kwargs)
    
    @classmethod
    def _all_errors(cls):
        return super()._all_errors()
    
    @classmethod
    def _validate_path(cls, *args, **kwargs):
        return super()._validate_path(*args, **kwargs)
    
    def _get_stats(self):
        return DynamicDict({_path.name if not self._posix and DataLoader.compare_posix(self._paths) else _path: \
                            self._os_stats(_path) for _path in self._paths})
    
    @classmethod
    def _os_stats(cls, __path):
        _bytes_converter = DataManager._bytes_converter
        _stats = os.stat_result(os.stat(__path))
        _volume_stats = {k: _bytes_converter(v) for k,v in shutil.disk_usage(__path)._asdict().items()}
        _os_stats = DynamicDict({attr: _bytes_converter(getattr(_stats, attr)) for attr in dir(_stats) if attr.startswith('st')})
        Stats = _Generic.get_subclass('Stats')
        _os_stats.update({'st_fsize': \
                        Stats(*_bytes_converter(_stats.st_size, symbol_only=True)),
                        'st_vsize': DynamicDict(_volume_stats)})
        return _os_stats
    
    @staticmethod
    def _get_time():
        return datetime.now().strftime('%Y%m%dT%I-%M-%S%p')
    
    def _format_file(self, __file='file', *, with_id=True):
        return f'{self._get_time()}_{__file}_metadata{f'_{self._ID}' if with_id else ''}.json'
    
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
        _exporter = self._exporter
        try:
            _exporter(_stats)
        except:
            _no_posix = DynamicDict({posix.stem: _v for posix, _v in _stats.items()})
            _exporter(_no_posix)
        finally:
            return f'\033[34m{self.module!r}\033[0m has been successfully exported.' + \
                (f' (Serialized as {dataclass_json.__name__!r})' if self._serializer else '')

@dataclass(order=True)
class ConfigManager:
    config_ini: Any = None
    sections: Any = field(init=True,
                        default_factory=lambda: np.core.defchararray.add('Section', np.arange(1, 4).astype(str)))
                        
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
                                default_factory=lambda: _Generic._new_config())
    
    def __post_init__(self):
        self.config_ini = self._validate_config()
        self.config = self._get_config()
    
    def _map_ini_suffix(self, __path):
        self.config_ini = Path(__path).with_suffix('.ini')
    
    def _validate_config(self):
        _ini = self.config_ini
        if _ini is None:
            raise DLoaderException(880, _ini)
        
        return _Generic._validate_path(_ini, verbose=True)
    
    @staticmethod
    def _sql_config_sections():
        return ConfigManager.create_sql_config(sections_only=True)
    
    @staticmethod
    def create_sql_config(__ini_name='sql_config', sections_only=False):
        _ini_name = Path(__ini_name).with_suffix('.ini')
        _config_parser = _Generic._new_config()
        
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
        CP = self._config_parser
        CP.read(self.config_ini)
        _ini_sections = list(filterfalse(lambda __key: __key.lower() in ('cc-default', 'default'), CP))
        _p_name = Path(self.config_ini)
        _has_nulls = lambda __key, __method=all: __method(CP.convert_value(val) for val in \
                                                        dict(CP.items(__key)).values())
        def _clean_config():
            _config = {
                        key: 
                            {k: CP.encrypt_text(v, _p_name.stem, export=True) if all((_Generic.compiler(_PASS, k), self.encrypt, CP.convert_value(v) is not None)) \
                            else v for k, v in CP.items(key)}
                            for key in _ini_sections
                    }
            if not len(_config):
                raise DLoaderException(899, _p_name.name)
            
            _section_defaults = np.array(*(i.default_factory() for i in fields(self) if i.name=='sections'))
            if not np.array_equal(self.sections, _section_defaults):
                try:
                    for _ in self.sections:
                        CP.items(_)
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
                        
                        self._update_attrs(_p_name.absolute(), list(good), _p_name.stem)
                        return good
                    
                    if not len(_possible_keys):
                        raise DLoaderException(890, self.sections, _ini_sections)
                    
            self._update_attrs(_p_name.absolute(), list(filter(_has_nulls, _ini_sections)), _p_name.stem)
            return _config
        return _clean_config()

    def _update_attrs(self, config_ini=None, sections=None, _ini_name=None):
        __config_params = _Generic._get_params(self._update_attrs)
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


def main(verbose=False):
    dl = DataLoader
    dm = DataManager
    ci = dl.load_config
    cii = ConfigManager
    
    nltk = dl(f'{Path.home()}/nltk_data/corpora/stopwords', dynamic=True, all_exts=True, manage_data=False,  no_method=False, posix=False, verbose=True, module='NLTK', generator=False, dynamic_with_benedict=False, full_repr=True)()
    test1 = dl('/Users/yousefabuzahrieh/Desktop/test', dynamic=False, encoding='utf-8', all_exts=True, module='test1', posix=True, generator=True, verbose=False).inject_files(f'{Path.home()}/nltk_data/corpora/stopwords', module='NLTK')
    test2 = dl('/Users/yousefabuzahrieh/Desktop/test', ['csv', 'xls', 'xlsx', 'json', 'pdf', 'txt'], dynamic=True, module='test2', posix=False, allow_empty_files=True, verbose=False, manage_data=False)
    test5 = dl('/Users/yousefabuzahrieh/Desktop/test', ['csv', 'xls', 'xlsx', 'json', 'pdf', 'txt'], dynamic=False, module='test2', posix=False, allow_empty_files=True, verbose=False, manage_data=True)()
    test3 = dl('/Users/yousefabuzahrieh/Desktop/test', ['csv', 'xls', 'xlsx'], dynamic=True, module='test3', posix=False, verbose=False, allow_empty_files=True, dynamic_with_benedict=False)()
    # test4 = dl('/Users/yousefabuzahrieh/Downloads/archive', ['csv'], dynamic=True, posix=True, verbose=False)()
    # print(test4)
    # print(dm(test4).export_stats())
    # print(dm(nltk, posix=False)().dutch)
    # print(dm(nltk, posix=True)().__sizeof__())
    # print(dm(nltk, posix=False).all_stats)
    # print(nltk.dutch)
    # print(DynamicThread())
    # print(test2['islamic-facts'])
    # print(test5)
    # print(test3)
    # print(nltk.total_size)
    # print(dl.compare_sets(nltk, nltk))
    # print(test4.hashed_files)
    # print(nltk.english)
    # print(len(nltk))
    # print(nltk.__sizeof__())
    # print(benedict(nltk))
    # print(bool(nltk))
    # print(test2.hashed_files)
    # print([i for i in nltk])
    # print(test2.arabic_numbers)
    # print(test3['islamic_facts.dcsv'])
    # print(test2['islamic_facts.csv'])
    # print(test2.get('islamic_facts.csvv'))
    # print(test3)
    # print(len(test3))
    # print(nltk.english)
    # print(nltk.files.get('dutch'))
    # print(nltk['dutch'])
    # print([i for i in nltk.reset()])
    # print(nltk.file_stats)
    # print(FileStats(test4))
    # print(FileStats(nltk, serializer='dataclass_json').export_stats())
    # print(FileStats(nltk).export_stats())
    # print(nltk(posix=False).all_stats)
    # print(nltk.get('englissh'))
    # print(FileStats(nltk))
    # print(FileStats(nltk).all_stats)
    # print([i for i,j in nltk])
    # print(nltk.hashed_files)
    # print(nltk.check_hash('/Users/yousefabuzahrieh/Desktop/test/islamic_facts.csv'))
    # print(test1.hashed_files)
    # print(dl('/Users/yousefabuzahrieh/Desktop/test/islamic_facts.csv'))
    # print([j for i,j in nltk.hashed_files.items()])
    # print(nltk.hashed_files.ID_1)
    # print(nltk.files['dutchh'])
    # print(nltk.get('dutch'))
    # def func():
    #     print(nltk.english)
        # print(len(nltk))
        # print(nltk.get('dutch'))
    # func()
    # print(nltk.get('','englishh'))
    # print(nltk.get())
    # print(nltk.files['englishh'])
    # print([i for i,j in test1])
    # print(test1)
    # print(test1.hashed_files)
    # print(test4.__sizeof__())
    print(nltk, test1, test2, test3, test5, sep='\n\n\n')
    # print(_Generic.get_subclass('hello'))
    # print(nltk)
    # print(nltk.english)
    # print(nltk.__sizeof__(), test1.__sizeof__(), test2.__sizeof__(), test3.__sizeof__(), sep='\n\n\n')
    # print(dm(nltk, module='NLTK').export_stats(), dm(test1, serializer='dataclass_json').export_stats(), dm(test2).export_stats(), dm(test3).export_stats(), sep='\n\n\n')
    # print(dl('/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/islamic_data/jsons'))
    # print(DataManager(nltk))
    # print(test1['Book1'])
    # print(test1.get('Book1'))
    # print(test2['islamic_terms.csv'])
    # print([k for i,j in test3 for k in j if i=='stopwords'])
    # print(test3)
    # print([os.path.getsize(i) for i,j in nltk])
    # print(['dutch' in nltk])
    # print(len(test2))
    # print(test3)
    # print(test3['islamic_facts.csv'])
    # print(test3['allahs_names.csv'])
    # print(test2['all-duas'])
    # print(test2['salah-guide'])
    # print(test2['islamic_timeline.csv'])
    # print(test2.get('quran_stats'))
    # print(test1)
    # print(cii(config_ini='db_config.ini'))
    # print(cii(config_ini='db_config'))
    # print(cii(config_ini='g.py'))
    # print(cii(config_ini='db_config.ini', sections=['mysql', 'postgresql']))
    # print(cii(config_ini='db_config.ini', sections=['ffrfr', 'mysql','postgresql', 'mysql']))
    # print(cii(config_ini='db_config.ini', sections=['ffrfr', 'postgresql', 'mysql']))
    # print(cii(config_ini='db_config.ini'))
    # text = cii(config_ini='db_config.ini', _encrypt=True).config.PostgreSQL['password']
    # print(text)
    # a = dl.config_info(instance_only=True)
    # print(a(config_ini='db_config.ini').config)
    # print(cii(config_ini='sql', _sql_keys=None))
    # print(cii(config_ini='sql_config.ini'))
    # a = cii(config_ini='/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/sources.ini')
    # print(a.config.Sources)
    dirs = '/Users/yousefabuzahrieh/Desktop/test', '/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/islamic_data/jsons', \
        f'{Path.home()}/nltk_data/corpora/stopwords', '/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/islamic_data/jsons/arabic', '/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/islamic_data/jsons/hadiths', \
        '/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/islamic_data/jsons/salah'
    # for i in dirs:
    #     print(shutil.disk_usage(i)._asdict())
    # print(_Generic.create_subclass('Extractor', module='NLTK', num_args=10, rename=True)().__module__)
    # print(shutil.disk_usage('/Volumes/USBC')._asdict())
    # print(a)
    # print(dm([Path(dirs[1])], posix=False).all_stats)
    # print(shutil.disk_usage('/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive')._asdict())

    # CP = ConfigManager(config_ini='/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/sources.ini')
    # print(CP)
    # print(dir(nltk))
    # print(dir(test2))
    # print(dl.add_dirs(*dirs, defaults=['json'], all_exts=False, merge=True, dynamic=True, no_method=False, posix=False, generator=False, verbose=False))
    # print(dl.add_dirs(*dirs, all_=True, merge=True, dynamic=False, no_method=False, posix=True, verbose=False))
    # print(dl.add_files('/Users/yousefabuzahrieh/Desktop/test/islamic_facts.csv','/Users/yousefabuzahrieh/Desktop/test/islamic_terms.csv', dynamic=True, generator=False, verbose=False))
    # print(ci(instance_only=True)('db_config.ini', encrypt=True).config)
    # print(ci(config_ini='db_config.ini').config)
    # print(cii.create_sql_config('db_config'))
    # print(cii(config_ini='db_config.ini', encrypt=True)._update_config('MySQL', {'host': 'localhost', 'password': 'me'}))
    # print(dl('/Users/yousefabuzahrieh/Library/CloudStorage/GoogleDrive-yousef.abuzahrieh@gmail.com/My Drive/Python/Projects/IslamAI/'))
    # print([i for i in dl._get_params(ConfigManager._update_attrs)])
    # print(cii(config_ini='db_config.ini', encrypt=True))
    # print(nltk.hashed_files)
    # print(DynamicDict({Path('ggoth.csv'): 1}).get('ggoth.csv'))
    # print(_Generic._rm_period(frozenset(('a'))))
    # print(_Generic._too_large(chain(i for i in [])))
    # import constants
    # print(constants.__repr__())
    # print(constants.__getitem__('_PASS'))
    # print(DataLoader._terminal_size())
    # print(DynamicDict({'1':1}).get('1'))
    # from datetime import datetime
    # print(datetime.today())
    # print(datetime.now().strftime('%Y-%m-%dT%H-%M-%S%p'))
    # print(_Generic._cls_tuple('__Ext'))
    # print(DataLoader.compare_posix((('corpora/stopwords.csv','1'), ('corpora/stopwords','1')), (('corpora/stopwords.csd', '2'),('corpora/stopwords', '2'))))
    #<generator object DataLoader._execute_path.<locals>.<genexpr> at 0x11ce013c0>
    # print(chain((1),(2)))
    # print(DynamicDict({'a':{'b': DynamicDict({'3': {'4':'5'}})}}))
    # print(dl('/Users/yousefabuzahrieh/Desktop/test/islamic_facts.csv'))
    
if (__main:=__name__) == '__main__':
    _dl_name = DataLoader.__class__.__name__
    parser = argparse.ArgumentParser(description=f'{_dl_name}: A powerful data loading utility.')
    parser.add_argument('message', nargs='?', default=f'Executing {__main!r}.py', help=f'Title message for the execution of {__main}.')
    parser.add_argument('--verbose', action='store_true', help=f'Enable verbose mode for {_dl_name!r}.')
    _args = parser.parse_args()
    _message = _args.message
    _verbose = _args.verbose or True
    
    with Timer(message=_message, verbose=_verbose):
        main(_verbose)