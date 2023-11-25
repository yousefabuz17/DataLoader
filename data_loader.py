import re
import sys
import logging
import threading
from os import cpu_count
from copy import deepcopy
from pathlib import Path
from collections import OrderedDict, namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache, wraps
from itertools import filterfalse, chain
from configparser import ConfigParser, NoSectionError, MissingSectionHeaderError
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView, Iterable,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from constants import _PASS, _ERRORS
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr as _recursive_repr

logging.basicConfig(level=logging.INFO, format='[LOG]%(levelname)s:%(message)s')

rm_p = lambda __i: __i.lstrip('.').lower()
_s = lambda __i: '{}'.format('s' if (hasattr(__i, '__len__') and len(__i)>1 \
                                or __i>1) else '')
compiler = lambda __defaults, __k: \
            re.compile('|'.join(map(re.escape, __defaults)), re.IGNORECASE) \
            .match(__k if isinstance(__k, str) \
            else '|'.join(map(re.escape, __k)))

dl_executor = ThreadPoolExecutor(max_workers=min(32, (cpu_count() or 1) + 4))

#** Outside of ThreadPool to ensure it prints only once for each execution(path)
def _dl_initializer(*__path):
    __thread_prefix = dl_executor._thread_name_prefix = '(DL)DataLoaderExecutor'
    __thread_count = threading.active_count()
    __main_thread = threading.current_thread().name
    _repr = '\n{} initialized {} {} worker{} for \033[1;32m`{}`\033[0m\n'
    print(_repr.format(__thread_prefix, __thread_count,
                    __main_thread, _s(__thread_count),
                    Path('/'.join(*__path))))

class DynamicDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    
    @staticmethod
    def _repr(__cls, __items):
        items = ((k, DynamicDict._too_large(v)) for k, v in __items)
        return '{}([{}])'.format(
                                __cls,
                                f',\n{' ':>{len(__cls)+2}}'.join(
                                (f'({k}, {v})' for k, v in items)
                                )
                            )
    
    def __missing__(self,*args):
        raise DLoaderException(*args)
    
    def __getattr__(self, __item):
        if __item not in self:
            if hasattr(__item, '__str__'):
                __files = {Path(p).stem: v for p,v in self}
                __key = Path(__item).stem
                _right_key = compiler(__files.keys(), __key)
                if len(__files)!=len(self):
                    #** Incase if any files have the same name but different extension
                    self.__missing__(227)
                elif _right_key:
                    return __files.get(_right_key.group())
                self.__missing__(225, __item, self.__possible_key(__key))
        return self[__item]
    
    def __getitem__(self, __item):
        if __item not in self:
            try:
                return self.__getattr__(__item) 
            except:
                if not (_right_key:=self.__possible_key(__item)):
                    raise self.__missing__(223, __item, _right_key)
                return self.__getattr__(_right_key) 
        
        return self.get(__item)
    
    def __setattr__(self, __attr, __value):
        self[__attr] = __value
    
    @_recursive_repr()
    def __repr__(self):
        __cls = (lambda x: x.capitalize() if not x[0].isupper() else x)(self.__class__.__name__)
        
        if not self:
            return DynamicDict._repr(__cls, '')
        
        return DynamicDict._repr(__cls, self.items())
    
    __str__ = __repr__
    
    def __possible_key(self, __key):
        try:
            __defaults = list(map(lambda p: Path(p).stem, self.keys()))
            return compiler(__defaults, Path(__key).stem).group()
        except AttributeError:
            return None
    
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
    
    def get(self, __key=None, __default=None):
        result = super().get(__key, __default)
        
        if (__key in ('', None)):
            return __default
        elif all((not __key, not __default)):
            raise DLoaderException(222)
        
        if self.__contains__(__key):
            return result
        
        if (_possible_key:=self.__possible_key(__key)):
            DLoaderException(221, __key, _possible_key, _log_method=logging.warning)
        
        return __default

    @staticmethod
    def reset(__dict_gen, dict_type=None):
        __gen = ((k,v) for k,v in __dict_gen)
        return DynamicDict(__gen) if dict_type is None else dict_type(__gen)
    

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
            DLoaderException(0, message=f'`{_path}` has been successfully created.', _log_method=logging.info)
        return _config_parser

class DLoaderException(BaseException):
    def __init__(self, *args, message=None, _log_method=logging.error) -> None:
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
    
    _defaults: Any = field(init=False, default_factory=lambda: Extensions.__defaults__)
    _ALL: List = field(init=False, default=None)
    
    def __post_init__(self):
        _defaults = self.__defaults__
        _mimetypes = mimetypes.types_map
        _mimetypes['xlsx'] = None
        _all_exts = set(ExtInfo(rm_p(i), pd.read_excel if rm_p(i) in ['xls','xlsx'] else open) \
                        for i in _mimetypes \
                        if rm_p(i) not in _defaults)
        self._defaults = [ext for ext in _defaults if ext!='empty']
        self._ALL = {**{ext.suffix_: ext for ext in _all_exts},
                     **_defaults}
    
    @property
    def __defaults__(self):
        global pd, mimetypes, ExtInfo
        import json
        import mimetypes
        import pandas as pd
        from pdfminer.high_level import extract_pages
        
        ExtInfo = namedtuple('ExtInfo', ('suffix_', 'loader_'), module='Extensions')
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
        return f'{self.__class__.__name__}([{', '.join(f'{i.name}={list(getattr(self, i.name))}' for i in fields(self))}])'
    
    __str__ = __repr__

EXTENSIONS = Extensions()
_NEW_CONFIG = lambda: CConfigParser(allow_no_value=True,
                                    delimiters='=',
                                    dict_type=DynamicDict,
                                    converters={'*': CConfigParser.convert_value}
                                    )

class DynamicGen(Iterable):
    __dict_gen = None
    
    def __init__(self, dict_gen):
        self.__dict_gen = DynamicGen.__dict_gen = dict_gen
    
    def __repr__(self):
        __cls = self.__class__.__name__
        __string = '{}([{}])'.format(
                                __cls,
                                f',\n{' ':>{len(__cls)+2}}'.join(
                                (f'({k}, {DynamicDict._too_large(v)})' for k, v in self.__dict_gen)
                                ))
        return f'[{sys.getsizeof(self.__dict_gen)} BYTES] GenMemID: {id(self.__dict_gen)}\n{__string})'
    
    def __missing__(self):
        raise DLoaderException(1, self.__class__.__name__)
    
    def __iter__(self):
        if not isinstance(self.__dict_gen, Generator):
            raise DLoaderException(0)
        return iter(self.__dict_gen)
        
    def __len__(self):
        return 0 if not self.__dict_gen else sum(1 for _kv in self.__dict_gen)
    
    def _missing(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.__missing__()
        return wrapper
    
    def __bool__(self):
        return bool(self.__len__())
    
    @_missing
    def get(self):
        pass
    
class DataLoader(DynamicGen):
    _DEFAULTS = EXTENSIONS._defaults
    _ALL = EXTENSIONS._ALL
    _THREAD_LOCK = threading.Lock()
    _THREAD_EXECUTOR = dl_executor
    _ALREADY_LOGGED = defaultdict(bool)
    _KWARG_KEYS = set(('all_', 'dynamic', 'no_method'))
    _DYNAMIC = lambda x=None, __type=DynamicGen: __type if not x else __type(x)
    
    def __init__(self,
                ext_path=None,
                ext_defaults=None,
                all_=False,
                **kwargs):
        
        self.ext_path = ext_path
        self.ext_defaults = ext_defaults
        self.all_ = all_
        self._DYNAMIC = DataLoader._DYNAMIC
        self.kwargs = kwargs
        self._validate_args()

    def __repr__(self):
        return super().__repr__()
    
    __str__ = __repr__
    
    def __call__(self):
        if self.kwargs.get('dynamic'):
            return DataLoader._DYNAMIC(self.files, DynamicDict)
        return DataLoader._DYNAMIC(self.files, DynamicGen)
    
    def _validate_args(self):
        if not self.ext_path:
            raise DLoaderException(200, self.__class__.__name__, self.ext_path)
        
        elif all((self.ext_defaults, self.all_)):
            raise DLoaderException(202, self.__class__.__name__, self._DEFAULTS)
        
        self.__files = None
        self.ext_path = self._validate_path(self.ext_path, True)
        self.ext_defaults = self._validate_exts(self.ext_defaults)
    
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
        __defaults = cls._DEFAULTS if __defaults is None else __defaults
        __no_dirs = lambda _p: _p.is_file() and not _p.is_dir()
        __ext_pat = partial(compiler, __defaults)
        
        return (__p for __p in __path.glob('*') \
                if __ext_pat(rm_p(__p.suffix)) \
                and DataLoader._validate_path(__p) \
                and __no_dirs(__p))
    
    def _validate_exts(self, __exts):
        if __exts is None:
            return
        
        __valid_exts = [rm_p(ext) \
                        for ext in __exts \
                        if rm_p(ext) in self._ALL]
        
        __failed = list(filterfalse(lambda ext: ext in __valid_exts, __exts))
        if len(__failed)==len(__exts):
            raise DLoaderException(210, _s(len(__failed)), __failed, list(self._ALL))
        
        vld_ext_name = self._validate_exts.__func__.__name__
        if __failed and vld_ext_name not in self._ALREADY_LOGGED:
            DLoaderException(215, __failed, _log_method=logging.warning)
            self.__logged(vld_ext_name)
        return __valid_exts
    
    @staticmethod
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
            vld_pth_name = DataLoader._validate_path.__name__
            __logged = DataLoader._ALREADY_LOGGED
            if any(filterfalse(lambda x: x in __logged, (vld_pth_name, path.stem))):
                _raise(DLoaderException(0, message=f'Skipping {path.stem}', _log_method=logging.warning))
                DataLoader._ALREADY_LOGGED.update({__k: True for __k in (vld_pth_name, path.stem)})
            return
        
        return path

    @staticmethod
    def _get_params(__method):
        import inspect
        try: 
            __sig = inspect.signature(__method)
        except TypeError:
            raise DLoaderException(800, __method)
        return __sig.parameters.keys()
    
    def _ext_method(self, __path):
        __suffix = rm_p(__path.suffix)
        __method = None
        __all = self._ALL
        if __suffix==__all['empty'].suffix_:
            __method = __all['empty'].loader_
        elif __suffix in __all:
            __method = __all[__suffix].loader_
        else: raise DLoaderException(702, __method)
        yield __method
    
    def _check_ext(self, __path):
        __method = next(self._ext_method(__path))
        __kwargs = {param: value for param, value in self.kwargs.items() \
                    if param in self._get_params(__method) \
                    and value is not None}
        _chk_ext_name = self._check_ext.__func__.__name__
        if __kwargs and _chk_ext_name not in self._ALREADY_LOGGED:
            DLoaderException(-1, _log_method=logging.info)
            self.__logged(_chk_ext_name)
        
        return self._load_file(__path, __method, __kwargs)
    
    def inject_files(self, *__dirs):
        if not len(__dirs)>=1:
            raise DLoaderException(150)
        
        with self._THREAD_LOCK:
            __all_files = self._THREAD_EXECUTOR.map(partial(DataLoader, all_=True, **self.kwargs), __dirs)
        __gen_files = ((i.ext_path.name, i.files) for i in __all_files)
        if isinstance(self.files, DynamicDict):
            for i,j in __gen_files:
                self.files.update(**{i:j})
            self.files = DynamicDict(self.files)
            return self.files
        # raise DLoaderException(0, message=f'{DynamicGen.__name__!r} is not compatible with {DataLoader.inject_files.__name__!r} yet.')
        
        __files = chain(((k,v) for k,v in self.files), __gen_files)
        self.files = DynamicGen(__files)
        return self.files
    
    @classmethod
    @cache
    def load_file(cls, file_path, **__kwargs):
        __path = cls._validate_path(file_path, True)
        p_method = next(cls._ext_method(cls, __path))
        __kwargs = {} if __kwargs is None else __kwargs
        loaded_file = cls._load_file(cls, __path, p_method, __kwargs)
        return loaded_file

    def _load_file(self, path, method, __kwargs):
        from json.decoder import JSONDecodeError
        from pandas.errors import ParserError, DtypeWarning, EmptyDataError
        
        p_name = Path(path.parts[-1])
        p_contents = None
        method = (lambda x: open if x else method)(__kwargs.pop('no_method', False))
        self._rm_kwargs(__kwargs, True)
        FileInfo = namedtuple('FileInfo', ('path_', 'contents_'), module='DataLoader')
        __errors = dict(zip((PermissionError, UnicodeDecodeError,
                            ParserError, DtypeWarning, OSError, 
                            EmptyDataError, JSONDecodeError, Exception),
                            (13, 100, 303, 400, 402, 607, 102, 500)))
        try:
            p_contents = method(path, **__kwargs)
        except tuple(__errors) as _error:
            __exception = type(_error)
            __error_code = __errors.get(__exception, 500)
            __placeholder_count = _ERRORS.get(__error_code).count('{}')
            __raise = partial(DLoaderException, __error_code, p_name, _log_method=logging.warning)
            _ld_dt_name = self.load_file.__func__.__name__
            
            if any(__item not in DataLoader._ALREADY_LOGGED \
                    for __item in (_ld_dt_name, str(__exception))):
                
                if __exception == JSONDecodeError:
                    __raise(_error.pos, _error.lineno, _error.colno)
                elif __placeholder_count == 2:
                    __raise(_error)
                else:
                    __raise()
                
                self.__logged(_ld_dt_name, str(__exception))
            p_contents = 0
        
        if (isinstance(p_contents, int) and p_contents==0) \
            or (hasattr(p_contents, 'empty') and p_contents.empty):
            return FileInfo(p_name, None)
        
        return FileInfo(p_name, p_contents)
    
    @cache
    def _execute_path(self):
        self._THREAD_EXECUTOR._initializer = _dl_initializer(Path(self.ext_path).parts[-2:])
        with self._THREAD_LOCK:
            _files = self._THREAD_EXECUTOR.map(self._check_ext, self._get_files)
        
        return ((Path(file.path_).name, file.contents_) for file in _files if file.contents_ is not None)
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        return self.__to_dynamic(self.__files, self.kwargs)
    
    @classmethod
    def __logged(cls, *__func):
        cls._ALREADY_LOGGED.update({_func: True for _func in __func})
    
    @classmethod
    def add_files(cls, *__files, **__kwargs):
        if not len(__files)>=1:
            raise DLoaderException(220)
        kwargs = deepcopy(__kwargs)
        with cls._THREAD_LOCK:
            loaded_files = cls._THREAD_EXECUTOR.map(partial(cls.load_file, **__kwargs), (cls._validate_path(path) \
                        for path in map(cls._validate_path, __files)))
            
        __files = ((file.path_.name, file.contents_) for file in loaded_files if file.contents_ is not None)
        cls.__files = __files
        
        __dynamic = kwargs.pop('dynamic', False)
        if __dynamic:
            return DynamicDict(cls.__files)
        
        return DynamicGen(cls.__files)
    
    @staticmethod
    def _rm_kwargs(__kwargs, all_=False):
        __defaults = DataLoader._KWARG_KEYS if all_ \
                    else (k for k in DataLoader._KWARG_KEYS if k!='dynamic')
        [__kwargs.pop(i, False) for i in __defaults]
    
    @classmethod
    def add_dirs(cls, *__dirs, **__kwargs):
        if not len(__dirs)>=1:
            raise DLoaderException(220)
        
        __directories = map(cls._validate_path, __dirs)
        __merge = __kwargs.pop('merge', False)
        if __merge:
            with cls._THREAD_LOCK:
                loaded_directories = (cls.load_file(j, **__kwargs) \
                                    for i in cls._THREAD_EXECUTOR.map(cls.get_files, __directories) \
                                    for j in i)
            
            __files = ((p.path_.name, p.contents_) for p in loaded_directories if p.contents_ is not None)
            cls.files = cls.__to_dynamic(__files, __kwargs)
            return cls.files
        
        with cls._THREAD_LOCK:
            loaded_directories = cls._THREAD_EXECUTOR.map(partial(cls, **__kwargs), __directories)
            __files = ((__cls.ext_path.name, __cls()) for __cls in loaded_directories)
        cls.files = cls.__to_dynamic(__files, __kwargs)
        return cls.files
    
    @staticmethod
    def __to_dynamic(__dict_gen, kwargs):
        __dynamic = kwargs.pop('dynamic', False)
        
        if __dynamic:
            return DataLoader._DYNAMIC(__dict_gen, DynamicDict)
        
        return __dict_gen
    
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
            DLoaderException(7, _ini_name, _log_method=logging.info)
            DLoaderException(0, message=f'`{_ini_name}`: {sections}', _log_method=logging.info)
            return
        __input = f'The file `{_ini_name}` already exists. Overwriting will simply empty it out with null values. Proceed (N/y)? '
        if _ini_name.is_file():
            if compiler(['yes', 'y', '1'], input(__input)):
                return _write_file()
            DLoaderException(0, message=f'[TERMINATED] {_ini_name} has not been overwritten.', _log_method=logging.info)
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
                            DLoaderException(0, message=f'Invalid section{_s(__bad)}, skipping: {__bad}', _log_method=logging.warning)
                        
                        if good_has_nulls:
                            DLoaderException(1002, good_has_nulls, _log_method=logging.warning)
                        
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



