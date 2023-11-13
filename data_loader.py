import sys
import re
import inspect
import logging
import mimetypes
from pathlib import Path
from itertools import chain
from multiprocessing import cpu_count
from configparser import ConfigParser, NoSectionError
from cryptography.fernet import Fernet
from collections import OrderedDict, namedtuple
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from constants import _PASS, _ERRORS
import pandas as pd
from pandas.errors import ParserError, DtypeWarning
from itertools import zip_longest

from dataclasses import dataclass, field, fields
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache
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
    
    _empty: Any = field(init=False, repr=False, default='')
    _other: Any = field(init=False, repr=False, default=None)
    _methods: Any = field(init=False, repr=False, default=None)
    _loaders: Any = field(init=False, default_factory=lambda: open)
    _ALL: List = field(init=False, default=None)
    
    def __post_init__(self):
        dd = DataDict
        self._other = {
                    i.name: [i.default] for i in fields(self) \
                    if i.name.lower() in ['_empty', '_all']
                    }
        
        self._ALL = set([i.lstrip('.') for i in mimetypes.types_map.keys()] + ['xlsx'])
        
        self._methods = dd(dict(zip_longest(self._other, [self._loaders], fillvalue=self._loaders)))

EXTENSIONS = Extensions()
METHODS = EXTENSIONS._methods

class LoaderException(Exception):
    def __init__(self, *args, _log_method=logging.error) -> None:
        self._log_method = _log_method
        self.error_message = self.match_error(*args)
        super().__init__(self.error_message)
        self._log_error(*args)

    def __str__(self):
        return self.error_message
    
    def match_error(self, __obj, __code):
        __obj = __obj if not isinstance(__obj, Path|str) else Path(__obj).name
        if __code in  _ERRORS:
            str_code = _ERRORS[__code]
            return str_code.format(' '.join(chain([str(__obj)])))
        _human_error = _ERRORS[-1].format(__code)
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
        _defaults = [i for i in chain.from_iterable(EXTENSIONS._other.values()) if i]
        if self.ext_defaults:
            _defaults = [ext for ext in self._validate_exts(self.ext_defaults)]
        elif self.__all:
            _defaults = EXTENSIONS._empty
        
        _ext_pat = re.compile('|'.join(map(re.escape, _defaults)), re.IGNORECASE)
        return (i for i in self.ext_path.glob('*') \
                if (_ext_pat.search(i.suffix.lstrip('.')) \
                and self._validate_path(i))
                )
    
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
    def _validate_path(__path, __skip=False):
        def __raise(_exception):
            if not __skip:
                raise _exception
            return
        
        path = Path(__path)
        if not path:
            __raise(LoaderException(path, 800))
            
        elif not path.exists():
            __raise(LoaderException(path, 404))
            
        elif (not path.is_file()) \
            and (not path.is_dir()) \
            and (not path.is_absolute()):
            __raise(LoaderException(path, 707))
            
        elif path.stem.startswith('.'):
            path = None

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
        
        if __suffix==EXTENSIONS._empty:
            __method = METHODS._empty
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
    @cache
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
            p_method = EXTENSIONS._loaders(path, **__kwargs)
        except PermissionError: raise LoaderException(p_name, 13)
        except UnicodeDecodeError: raise LoaderException(p_name, 100)
        except ParserError: raise LoaderException(p_name, 303)
        except DtypeWarning: raise LoaderException(p_name, 400)
        except OSError: raise LoaderException(p_name)
        except Exception as _e: raise LoaderException(_e, 500)
        
        if p_method is None:
            raise LoaderException(p_name, 530)
        
        #! Make separate class for cleaning purposes
        elif isinstance(p_method, pd.DataFrame|pd.Series) and p_method._empty:
            LoaderException(p_name, 607, _log_method=logging.warning)
        return p_contents(p_name, p_method)
    
    @cache
    def _execute_path(self):
        with ThreadPoolExecutor(max_workers=max(1, cpu_count()-2)) as executor:
            _files = executor.map(self._check_ext, self._get_files)
        return DataDict({Path(file.name).stem: file.method for file in _files})
    
    @property
    @cache
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
    
    @property
    def mapper(self):
        return DataDict(self.files)
    
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
    config: Any = field(init=False, repr=False)
    _sql_keys: Any = field(init=True, repr=False, default=None)
    _encrypt: Any = field(init=True,
                        default=False,
                        repr=False)
    
    _ini_name: Any = field(init=False, default=None)
    _config_parser: Any = field(init=False,
                                repr=False,
                                default_factory=lambda: ConfigParser(allow_no_value=True,
                                                                    dict_type=DataDict))
    
    def __post_init__(self):
        self._sql_keys = self._create_sql_config(True)
        self.config_ini = self._validate_config()
        self.config = self._get_config()
        self._config_parser._converters = {'*': self.convert_value}
    
    def _validate_config(self):
        _ini = self.config_ini
        if not _ini:
            raise LoaderException(_ini, 880)
        elif _ini and _ini.lower() == 'sql':
            return self._create_sql_config()
        return DataLoader._validate_path(_ini)

    @staticmethod
    def convert_value(value):
        value = str(value).lower()
        if value == 'true':
            return True
        elif value == 'false':
            return False
        elif value=='none':
            return None
        return value
    
    def _create_sql_config(self, __sections=False):
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
        if __sections:
            return list(sections.keys())
        for section, values in sections.items():
            __config[section] = {key: str(value) for key, value in values.items()}
        
        if not _ini_name.is_file():
            with open(_ini_name, mode='w') as sql_file:
                __config.write(sql_file)
                sql_file.close()
        self._ini_name = _ini_name.stem
        self.sections = list(sections.keys())
        self.config_ini = _ini_name.absolute()
        return __config
    
    @staticmethod
    def _pass_section(string, __method=None):
        __method = __method or re.search
        pass_pat = re.compile('|'.join(map(re.escape, _PASS)), re.IGNORECASE)
        return __method(pass_pat, string)

    def _get_config(self):
        logging.basicConfig(level=logging.INFO)
        dd = DataDict
        config_parser = self._config_parser
        config_parser.read(self.config_ini)
        _ini_sections = list(filter(lambda __key: __key!='DEFAULT', config_parser))
        __name = Path(self._ini_name) if self._ini_name else Path(self.config_ini)
        def _clean_config():
            _has_nulls = lambda __key, __method=any: __method(self.convert_value(val) for val in \
                                                        dict(config_parser.items(__key)).values())
            _config = dd({
                        key: dd({k: self.encrypt_string(v) if self._pass_section(k) and self._encrypt else v for k, v in config_parser.items(key)})
                        for key in _ini_sections if _has_nulls(key)
                    })
            if not _config:
                raise LoaderException(__name, 1000)
            elif self.sections:
                try:
                    for _ in self.sections:
                        config_parser.items(_)
                except NoSectionError:
                    if isinstance(self.sections, str):
                        self.sections = [self.sections]
                    _db_pat = re.compile('|'.join(map(re.escape, self.sections)), re.IGNORECASE)
                    _possible_keys = list(filter(_db_pat.search, _ini_sections))
                    if _possible_keys:
                        good = dd({ini_key: _config.get(ini_key) for ini_key in _possible_keys if _has_nulls(ini_key, all)})
                        bad = list(filter(lambda _bad: not _has_nulls(_bad, all), _possible_keys))
                        if not any(good):
                            LoaderException(f'{self.sections} -> Available sections based on arguments provided ({_possible_keys}) ', 1001)
                        elif bad:
                            LoaderException(', '.join(bad), 1001, _log_method=logging.warning)
                        return good
                    elif not _possible_keys:
                        LoaderException(f'{', '.join(self.sections)}`... -> All possible sections:\n{_ini_sections}', 890)
                        sys.exit(0)
            self.sections = _ini_sections
            self.config_ini = __name.absolute()
            self._ini_name =  __name.stem
            return _config
        if __name and __name.stem=='sql_config':
            LoaderException(__name, 7, _log_method=logging.info)
            self._ini_name = __name.stem
            self.sections = _ini_sections
            self.config_ini = __name.absolute()
            return self._update_sql_values()
        return _clean_config()
    
    def _update_sql_values(self):
        print(self._sql_keys)
        #! Depending on section key, loop through and give input() for each value depening on section
        return self.sections
    
    @staticmethod
    def encrypt_string(text):
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        encrypted_bytes = cipher_suite.encrypt(text.encode())
        encrypted_text = encrypted_bytes.hex()
        return encrypted_text, key

    @staticmethod
    def decrypt_string(encrypted_text, key):
        cipher_suite = Fernet(key)
        encrypted_bytes = bytes.fromhex(encrypted_text)
        decrypted_message = cipher_suite.decrypt(encrypted_bytes).decode()
        return decrypted_message




dl = DataLoader
ci = dl.config_info
cii = ConfigManager


nltk = dl(f'{Path.home()}/nltk_data/corpora/stopwords', all_=True)
test1 = dl('/Users/yousefabuzahrieh/Desktop/test')
test2 = dl('/Users/yousefabuzahrieh/Desktop/test', ['csv', 'xls', 'xlsx'])
test3 = dl('/Users/yousefabuzahrieh/Desktop/test', ['csv', 'pdf'])
print(nltk, test1, test2, test3, sep='\n\n\n')
print(test1.allahs_names)
print(test1.Book1)
print(cii(config_ini='db_config.ini'))
print(cii(config_ini='db_config.ini', sections=['mysql', 'postgresql']))
# text, key = cii(config_ini='db_config.ini', _encrypt=True).config.PostgreSQL.password
# # print(text)
a = dl.config_info(instance_only=True)
# print(a(config_ini='db_config.ini'))
print(cii(config_ini='sql', _sql_keys=None))
# print(cii(config_ini='sql_config.ini'))