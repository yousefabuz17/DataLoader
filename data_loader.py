import sys
import re
import json
import inspect
import logging
import mimetypes
from pathlib import Path
from itertools import chain
from multiprocessing import cpu_count
from configparser import ConfigParser, NoSectionError
from collections import OrderedDict, namedtuple
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)

import pandas as pd
from pandas.errors import ParserError, DtypeWarning

from dataclasses import dataclass, field, fields
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, cached_property, partial
from reprlib import recursive_repr as _recursive_repr

class DataDict(OrderedDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def __getattr__(self, __attr):
        if __attr in self:
            return self[__attr]
        else:
            raise AttributeError(f"'DataDict' object has no attribute '{__attr}'")

    def __setattr__(self, __attr, __value):
        self[__attr] = __value
    
    def __str__(self):
        __str = f',\n{' '*10}'.join([f'({__key}, ...)' for __key in self])
        return f'{self.__class__.__name__}([{__str}])'
    
    @_recursive_repr()
    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, [(i, '...') for i in self.keys()])
@dataclass(slots=True)
class Extensions:
    csv: Any='csv'
    txt: Any='txt'
    json: Any='json'
    empty: Any=''
    excel: Any=field(default_factory=lambda: ['xls', 'xlsx'])
    
    _ALL: List = None
    _extensions: Dict = None
    _methods: Dict = None
    
    def __post_init__(self):
        self._extensions = {
                            i.name: self.excel if i.name=='excel' else [i.default] \
                            for i in fields(self) \
                            if re.compile(r'([a-z].*[a-z]$|_all)', re.IGNORECASE).match(i.name) \
                            }
        
        self._ALL = [i.lstrip('.') for i in mimetypes.types_map.keys()] + [self.excel[1]]
        
        __any = partial(lambda path: open(Path(path)))
        __generic = partial(lambda path: open(Path(path)).read().splitlines())
        __loaders = [__any,
                    pd.read_csv,
                    __generic,
                    pd.read_excel, 
                    partial(lambda path: json.load(open(Path(path)))),
                    __generic]
        
        self._methods = DataDict(dict(zip(sorted(self._extensions), __loaders)))

EXTENSIONS = Extensions()
METHODS = EXTENSIONS._methods

class LoaderException(Exception):
    from errors import ERRORS
    
    def __init__(self, *args, _log_method=logging.error) -> None:
        self._log_method = _log_method
        self.error_message = self.match_error(*args)
        super().__init__(self.error_message)
        self._log_error(*args)

    def __str__(self):
        return self.error_message
    
    def match_error(self, __obj, __code):
        __obj = __obj if not isinstance(__obj, Path|str) else Path(__obj).name
        if __code in  self.ERRORS:
            str_code = self.ERRORS[__code]
            return str_code.format(' '.join(chain([__obj])))
        _human_error = self.ERRORS[-1].format(__code)
        raise AttributeError(_human_error)
    
    def _log_error(self, *args):
        self._log_method(self.match_error(*args))

class DataLoader:
    def __init__(self, ext_path=None, ext_defaults=None, **kwargs) -> None:
        self.ext_path = ext_path
        self.ext_defaults = ext_defaults
        self.kwargs = kwargs
        self.__files = None
        self.__index = 0
        self._validate_args()
    
    def __getitem__(self, __item):
        return self.get(__item)
    
    def __getattr__(self, __item):
        return self.get(__item)
    
    def __iter__(self):
        self.__files = iter(self.files)
        return self
    
    def __next__(self):
        if self.__index >= len(self):
            raise StopIteration
        item = dict(self.files.items())[self.__index]
        self.__index += 1
        return item 
    
    def __len__(self):
        return 0 if self.__files is None else len(self.__files)
    
    def __str__(self):
        formatted_files = f',\n{' '*10}'.join([f'({file}, ...)' for file in self.files])
        return f'{self.__class__.__name__}([{formatted_files}])'
    
    @_recursive_repr()
    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, [(i, '...') for i in self.files])
    
    def __contains__(self, __item):
        return __item in self.files
    
    def _validate_args(self):
        __all = self.kwargs.get('all_', False)
        __args = [self.ext_defaults, __all]
        
        if all(__args):
            raise LoaderException('', 810)
        
        self.__all = __all
        self.ext_path = self._validate_path(self.ext_path) if self.ext_path is not None else None
        self.ext_defaults = self._validate_exts(self.ext_defaults) if self.ext_defaults is not None else None

    def get(self, __key, __default=None):
        if __key in self.files:
            return self.files[__key]
        return __default
    
    @cached_property
    def _get_files(self):
        compiler = partial(lambda __defaults: re.compile('|'.join(__defaults), re.IGNORECASE))
        cls_exts = chain.from_iterable(EXTENSIONS._extensions.values())
        _defaults = [i for i in cls_exts if i]
        if self.ext_defaults:
            _defaults = [ext for ext in self._validate_exts(self.ext_defaults)]
        elif self.__all:
            _defaults = EXTENSIONS.empty
        
        _ext_pat = compiler(_defaults)
        return (i for i in self.ext_path.glob('*') \
                if (_ext_pat.search(i.suffix.lstrip('.')) \
                and self._validate_path(i)))
    
    @staticmethod
    def _validate_exts(__exts):
        __valid_exts = [ext.lstrip('.').lower() \
                        if ext.lstrip('.').lower() in EXTENSIONS._ALL \
                        else False for ext in __exts]
        __failed = [i for i in __exts if i not in __valid_exts]
        _s = '{}'.format('(s)' if len(__failed)>1 else '')
        
        if not all(__valid_exts):
            raise AttributeError(f'Invalid default argument{_s} provided for extension{_s}:\n{__failed}')
        
        return __valid_exts
    
    @staticmethod
    def _validate_path(__path):
        path = Path(__path)
        if not path:
            raise LoaderException(path, 800)
        elif not path.exists():
            raise LoaderException(path, 404)
        elif (not path.is_file()) \
            and (not path.is_dir()) \
            and (not path.is_absolute()):
            
            raise LoaderException(path, 707)
        
        return path
    
    @staticmethod
    def _get_params(__method):
        try: 
            __sig = inspect.signature(__method)
        except TypeError:
            raise LoaderException(__method, 800)
        return __sig.parameters.keys()
    
    @staticmethod
    def _ext_method(__path):
        __suffix = __path.suffix.lstrip('.').lower()
        __method = None
        
        if __suffix in [EXTENSIONS.csv, EXTENSIONS.txt]:
            __method = METHODS.csv
        elif __suffix in EXTENSIONS.excel:
            __method = METHODS.excel
        elif __suffix == EXTENSIONS.json:
            __method = METHODS.json
        elif __suffix==EXTENSIONS.empty:
            __method = METHODS.empty
        elif __suffix in EXTENSIONS._ALL:
            __method = METHODS._ALL
        else: raise LoaderException(__method, 702)
        
        return __method
    
    def _check_ext(self, __path):
        __method = self._ext_method(__path)
        __method_params = self._get_params(__method)
        __kwargs = {param: param for param, value in self.kwargs.items() \
                    if param in __method_params \
                    and value is not None}
        return self._load_data(__path, __method, __kwargs)
    
    @staticmethod
    @lru_cache(maxsize=None)
    def load_file(file_path, __kwargs=None):
        dl = DataLoader
        _path = dl._validate_path(file_path)
        p_method = dl._ext_method(_path)
        
        if __kwargs is None:
            __kwargs = {}
        
        loaded_file = dl._load_data(_path, p_method, __kwargs)
        return loaded_file.method

    @staticmethod
    def _load_data(path, method, __kwargs):
        p_name = path.parts[-1]
        p_method = None
        p_contents = namedtuple('_', ['name', 'method'])
        try:
            p_method = method(path, **__kwargs)
        except PermissionError: raise LoaderException(p_name, 13)
        except UnicodeDecodeError: raise LoaderException(p_name, 100)
        except ParserError: raise LoaderException(p_name, 303)
        except DtypeWarning: raise LoaderException(p_name, 400)
        except Exception as _e: raise LoaderException(_e, 500)
        
        if p_method is None:
            raise LoaderException(p_name, 530)
        
        #! Make separate class for cleaning purposes
        elif isinstance(p_method, pd.DataFrame|pd.Series) and p_method.empty:
            LoaderException(p_name, 607, _log_method=logging.warning)
        
        return p_contents(p_name, p_method)
    
    @lru_cache(maxsize=None)
    def _execute_path(self):
        with ThreadPoolExecutor(max_workers=max(1, cpu_count()-2)) as executor:
            _files = executor.map(self._check_ext, self._get_files)
        return {file.name: file.method for file in _files}
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        return self.__files
    
    @staticmethod
    def add(*__files):
        '''Add files only, not directories yet'''
        dl = DataLoader
        return DataDict({file.name: dl.load_file(file) for file in \
                        [dl._validate_path(path) for path in __files]})
    
    @cached_property
    def mapper(self):
        return DataDict(self.files)
    
    @staticmethod
    def load_sql(__database=''):
        class DataSQL: pass
        return DataSQL
    
    @staticmethod
    def load_config(*args, **kwargs):
        if kwargs.pop('instance', False):
            return ConfigInfo
        config_path, databases = args[0], args[1:]
        return ConfigInfo(config_path, *databases)

@dataclass
class ConfigInfo:
    config: Any = field(init=False, repr=False)
    _config_ini: Any = None
    _sections: Any = None
    _ini_name: Any = None
    
    def __post_init__(self):
        self._config_ini = self._validate_config()
        self.config = self._get_config
        
    def _validate_config(self):
        _ini = self._config_ini
        if _ini is None:
            raise LoaderException(_ini, 880)
        elif _ini and _ini.lower() == 'sql':
            return self._create_sql_config
        return DataLoader._validate_path(_ini)
    
    @staticmethod
    def convert_value(value):
        if str(value).lower() == 'true':
            return True
        elif str(value).lower() == 'false':
            return False
        elif str(value).lower()=='none':
            return None
        else:
            return value

    @cached_property
    def _create_sql_config(self):
        _ini_name = Path('sql_config.ini')
        __config = ConfigParser(allow_no_value=True, dict_type=DataDict, converters={'*': self.convert_value})
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
        
        _sections = {
                    'MySQL': _mysql,
                    'PostgreSQL': _generic, 'SQLAlchemy': _generic,
                    'Oracle': _generic, 'IBM DB2': _generic,
                    'Microsoft SQL Server': _generic,
                    'Amazon Redshift': _generic,
                    'SQLite': _other, 'MariaDB': _other
                    }
        
        for section, values in _sections.items():
            __config[section] = {key: str(value) for key, value in values.items()}
        
        if not _ini_name.is_file():
            with open(_ini_name, mode='w') as sql_file:
                __config.write(sql_file)
                sql_file.close()
        self._ini_name = _ini_name
        self._sections = list(_sections.keys())
        self._config_ini = self._ini_name.absolute()
        return __config
        
    @cached_property
    def _get_config(self):
        logging.basicConfig(level=logging.INFO)
        dd = DataDict
        config_parser = ConfigParser(allow_no_value=True, dict_type=DataDict, converters={'*': self.convert_value})
        config_parser.read(self._config_ini)
        _ini_keys = list(filter(lambda __key: __key!='DEFAULT', config_parser))
        __name = self._ini_name if self._ini_name else self._config_ini if not isinstance(self._config_ini, Path) \
                                                                                        else self._config_ini.stem
        def _clean_config():
            _has_nulls = lambda __key, __method=any: __method(self.convert_value(val) for val in \
                                                        dict(config_parser.items(__key)).values())
            _config = dd({key: dd(config_parser.items(key)) for key in _ini_keys if _has_nulls(key)})
            if not _config:
                raise LoaderException(__name, 1000)
            elif self._sections:
                try:
                    for _ in self._sections:
                        config_parser.items(_)
                except NoSectionError:
                    if isinstance(self._sections, str):
                        self._sections = [self._sections]
                    _db_pat = re.compile('|'.join(map(re.escape, self._sections)), re.IGNORECASE)
                    _possible_keys = list(filter(_db_pat.search, _ini_keys))
                    if _possible_keys:
                        good = dd({ini_key: _config.get(ini_key) for ini_key in _possible_keys if _has_nulls(ini_key, all)})
                        bad = list(filter(lambda _bad: not _has_nulls(_bad, all), _possible_keys))
                        if not any(good):
                            LoaderException(f'{self._sections} -> Available sections based on arguments provided ({_possible_keys}) ', 1001)
                        elif bad:
                            LoaderException(', '.join(bad), 1001, _log_method=logging.warning)
                        self._sections = list(good.keys())
                        self._config_ini = self._config_ini.absolute()
                        self._ini_name =  self._config_ini.name
                        return good
                    elif not _possible_keys:
                        LoaderException(f'{', '.join(self._sections)}`... -> All possible sections are found below:\n{_ini_keys}', 890)
                        sys.exit(0)
            self._sections = _ini_keys
            self._ini_name = __name
            return _config
        if self._ini_name and self._ini_name.stem=='sql_config':
            LoaderException(self._ini_name, 7, _log_method=logging.info)
            return self
        return _clean_config()

