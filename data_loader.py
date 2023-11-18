import logging
import re
from os import cpu_count
from collections import OrderedDict, namedtuple
from configparser import ConfigParser, NoSectionError
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, partial, cache
from itertools import filterfalse
from pathlib import Path
from typing import (Any, AnyStr, Dict, Generator, IO, ItemsView, Iterable,
                    KeysView, List, NamedTuple, Optional, Tuple, Union, ValuesView)
from constants import _PASS, _ERRORS
from pandas.errors import ParserError, DtypeWarning, EmptyDataError
from cryptography.fernet import Fernet
from dataclasses import dataclass, field, fields
from reprlib import recursive_repr as _recursive_repr
from json.decoder import JSONDecodeError


logging.basicConfig(level=logging.INFO)

rm_p = lambda __i: __i.lstrip('.').lower()
_s = lambda __i: '{}'.format('s' if len(__i)>1 else '')
compiler = lambda __defaults, __k: \
                re.compile('|'.join(map(re.escape, __defaults)), re.IGNORECASE).search(__k if isinstance(__k, str) \
                                                                                        else '|'.join(map(re.escape, __k)))

ExtInfo = lambda __typename='ExtInfo', \
                __field_names=['suffix_', 'loader_'], \
                __defaults=(None,)*2: namedtuple(__typename, __field_names, defaults=__defaults)

__max_workers = min(cpu_count() * 2, 32)
dl_executor = ThreadPoolExecutor(
                                thread_name_prefix='DLThread',
                                max_workers=__max_workers,
                                initializer=lambda *_: print(f'{__max_workers} DataLoader(DL) Threads in progress...'),
                                )

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
        return '{}([{}])'.format(
                        __cls,
                        f',\n{' ':>{len(__cls)+2}}'.join(
                        [f'({k}, {DynamicDict._too_large(v)})' for k, v in self.items()]
                        )
                    )
    
    __str__ = __repr__
    
    @staticmethod
    def _too_large(value, max_length=50):
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

    @staticmethod
    def convert_value(value):
        _value = str(value).lower()
        _vals = {'true': True, 'false': False, 'none': None}
        return _vals.get(_value, value)
    
    @staticmethod
    def encrypt_text(text, ini_name='config', __export=False):
        Encrypter = namedtuple('Encrypter', ['text', 'key'])
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        encrypted_bytes = cipher_suite.encrypt(text.encode())
        encrypted_text = encrypted_bytes.hex()
        encrypted_data = Encrypter(encrypted_text, key)
        if __export:
            return CConfigParser._exporter(text, encrypted_data, ini_name)
        
        return encrypted_data

    @staticmethod
    def decrypt_text(encrypted_text, key):
        cipher_suite = Fernet(key)
        encrypted_bytes = bytes.fromhex(encrypted_text)
        decrypted_message = cipher_suite.decrypt(encrypted_bytes).decode()
        return decrypted_message
    
    @staticmethod
    def _exporter(org_text, encrypted, /, ini_name='config', *, refresh=False, __path=None):
        _config_parser = _new_config()
        _items = {'ENCRYPTED_DATA': 
                dict(zip(('ORIGINAL_TEXT', 'ENCRYPTED_TEXT', 'DECRYPTER_KEY'),
                        (org_text, encrypted.text, encrypted.key)))
                }
        
        _config_parser.update(**_items)
        _path = Path(f'encrypted_{ini_name}.ini') if not __path else DataLoader._validate_path(__path, True)
        if not _path.is_file() or refresh:
            with open(_path, mode='w') as c_file:
                _config_parser.write(c_file)
            LoaderException(0, message=f'`{_path}` has been successfully created.', _log_method=logging.info)
        return _config_parser

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
                'csv': ExtInfo()('csv', pd.read_csv),
                'hdf': ExtInfo()('hdf', pd.read_hdf),
                'json': ExtInfo()('json', lambda path, **kwargs: json.load(open(path, **kwargs))),
                'pdf': ExtInfo()('pdf', extract_pages),
                'sql': ExtInfo()('sql', pd.read_sql),
                'txt': ExtInfo()('txt', open),
                'xml': ExtInfo()('xml', pd.read_xml),
                'empty': ExtInfo()('', lambda path, **kwargs: open(path, **kwargs).read().splitlines())
                }
    
    @_recursive_repr
    def __repr__(self):
        return DynamicDict.__repr__(self.__defaults__)
    
    __str__ = __repr__

EXTENSIONS = Extensions()
global _new_config
_new_config = lambda: CConfigParser(allow_no_value=True,
                                    delimiters='=',
                                    dict_type=DynamicDict,
                                    converters={'*': CConfigParser.convert_value}
                                    )

class DataLoader:
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
        assert len(self.files), LoaderException(0, '')
        return iter(self.files)
    
    def __next__(self):
        assert self.__index >= len(self), StopIteration
        file_name = self.files[self.__index]
        self.__index += 1
        return file_name
    
    def __bool__(self):
        assert len(self.files), LoaderException(0, '')
        return True if len(self.files)>=1 else False

    @_recursive_repr()
    def __repr__(self):
        return DynamicDict.__repr__(self.files)
    
    def __contains__(self, __item):
        return __item in self.files
    
    def add(self, *__files, **kwargs):
        return self.add_files(*__files, **kwargs)
    
    def mul(self, *__dirs, **kwargs):
        return self.add_dirs(*__dirs, **kwargs)
    
    def get(self, __key=None, __default=None):
        assert __key is not None, LoaderException(222, '')
        if __key in self.files:
            return self.files[__key]
        return __default
    
    def _validate_args(self):
        assert any((self.ext_path, self.ext_defaults)), \
                LoaderException(200, self.__class__.__name__)
        
        assert all((self.ext_defaults, self.all_)) is False, \
                LoaderException(202, self.__class__.__name__, ', '.join(EXTENSIONS._defaults))
        
        self.__files = None
        self.__index = 0
        self.ext_path = self._validate_path(self.ext_path, True) if self.ext_path is not None else None
        self.ext_defaults = self._validate_exts(self.ext_defaults) if self.ext_defaults is not None else None
    
    @cached_property
    def _get_files(self):
        _defaults = EXTENSIONS._defaults
        if self.ext_defaults:
            _defaults = [ext for ext in self._validate_exts(self.ext_defaults)]
        elif self.all_:
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
        assert len(__valid_exts), LoaderException(210, _s(__failed), __failed, EXTENSIONS._ALL.keys())
        
        if __failed:
            LoaderException(215, _log_method=logging.warning)
        return iter(__valid_exts)
    
    @staticmethod
    def _validate_path(__path, __raise=False):
        def _raise(_exception):
            assert not __raise, _exception
        
        try:
            path = Path(__path)
        except TypeError as t_error:
            raise (LoaderException(230, __path, t_error))
        
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
        import inspect
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
        __kwargs = {param: value for param, value in self.kwargs.items() \
                    if param in self._get_params(__method) \
                    and value is not None}
        return self._load_data(__path, __method, __kwargs)
    
    @staticmethod
    @cache
    def load_file(file_path, **__kwargs):
        dl = DataLoader
        _path = dl._validate_path(file_path, True)
        p_method = dl._ext_method(_path)
        __kwargs = {} if __kwargs is None else __kwargs
        loaded_file = dl._load_data(_path, p_method, __kwargs)
        return loaded_file

    @staticmethod
    def _load_data(path, method, __kwargs):
        p_name = Path(path.parts[-1])
        p_contents = None
        FileInfo = ExtInfo('FileInfo', ['name_', 'contents_'])
        try:
            p_contents = method(path, **__kwargs)
        except PermissionError: raise LoaderException(13, p_name)
        except UnicodeDecodeError: raise LoaderException(100, p_name)
        except ParserError: raise LoaderException(303, p_name)
        except DtypeWarning: raise LoaderException(400, p_name)
        except OSError: raise LoaderException(400 ,p_name)
        except EmptyDataError:
            p_contents = 0
        except JSONDecodeError as e:
            LoaderException(102, p_name, e.pos, e.lineno, e.colno, _log_method=logging.warning)
            p_contents = 0
        except Exception as _e: raise LoaderException(500, f'{p_name}: {_e}')
        
        if (isinstance(p_contents, int) and p_contents==0) \
            or (hasattr(p_contents, 'empty') and p_contents.empty):
            LoaderException(0, message=f'{_ERRORS[607].format(p_name)} File will be skipped.', _log_method=logging.warning)
            return FileInfo(p_name, None)
        return FileInfo(p_name, p_contents)
    
    @cache
    def _execute_path(self):
        _files = dl_executor.map(self._check_ext, self._get_files)
        return {Path(file.name_).stem: file.contents_ for file in _files if file.contents_ is not None}
    
    @cached_property
    def files(self):
        if self.__files is None:
            self.__files = self._execute_path()
        return self.__files
    
    @staticmethod
    def add_files(*__files, **kwargs):
        assert len(__files)>=1, LoaderException(220, '')
        dl = DataLoader
        files = map(Path, __files)
        loaded_files = dl_executor.map(partial(dl.load_file, **kwargs), (dl._validate_path(path, True) for path in files))
        return {Path(file.name_).stem: file.contents_ for file in loaded_files if file.contents_ is not None}
    
    @staticmethod
    def add_dirs(*__dirs, __merge=False, **kwargs):
        assert len(__dirs)>=1, LoaderException(220, '')
        dl = DataLoader
        directories = (dl._validate_path(_path, True) for _path in __dirs)
        
        if not __merge:
            loaded_directories = dl_executor.map(partial(dl, **kwargs), directories)
            return DynamicDict({i.ext_path.stem: i for i in loaded_directories})
        
        loaded_directories = (dl.load_file(j, **kwargs) for i in directories for j in dl.get_files(i))
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

@dataclass(kw_only=True)
class ConfigManager:
    config_ini: Any = field(init=True, default='db_config.ini')
    sections: Any = field(init=True, default_factory=lambda: [f'Section{i}' for i in range(1,4)])
    encrypt: Any = field(init=True,
                        default=True,
                        repr=False)
    config: Any = field(init=False, repr=False, default_factory=DynamicDict)
    
    _sql_keys: Any = field(init=False, repr=False, default_factory=lambda: ConfigManager._sql_config_sections())
    _ini_name: Any = field(init=False, default='db_config')
    _config_parser: Any = field(init=False, repr=False, default_factory=lambda: _new_config())
    
    def __post_init__(self):
        self.config_ini = self._validate_config()
        self.config = self._get_config()
    
    def __repr__(self):
        return f'{self.__class__.__name__}({[f'{i.name}={getattr(self, i.name)}'
                                            for i in fields(self) if i.repr is True]})'
    
    def _validate_config(self):
        _ini = self.config_ini
        assert _ini is not None, LoaderException(880, _ini)
        
        return DataLoader._validate_path(_ini, True)
    
    @staticmethod
    def _sql_config_sections():
        return ConfigManager.create_sql_config(sections_only=True)
    
    @staticmethod
    def create_sql_config(__name='sql_config.ini', sections_only=False):
        _config_parser = _new_config()
        _ini_name = DataLoader._validate_path(__name)
        
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
            LoaderException(0, message=f'\n{_ini_name!r}: {repr(sections)}', _log_method=logging.info)
            return
        __input = f'The file `{_ini_name}` already exists. Overwriting will simply empty it out with null values. Proceed (N/y)? '
        if _ini_name.is_file():
            if compiler(['yes', 'y', '1'], input(__input)):
                return _write_file()
            LoaderException(0, message=f'[TERMINATED] {_ini_name} has not been overwritten.', _log_method=logging.info)
            return
        
        return _write_file()

    def _get_config(self):
        CC = CConfigParser
        self._config_parser.read(self.config_ini)
        _ini_sections = list(filter(lambda __key: __key!='DEFAULT', self._config_parser))
        __name = Path(self._ini_name or self.config_ini)
        _has_nulls = lambda __key, __method=all: __method(CC.convert_value(val) for val in \
                                                        dict(self._config_parser.items(__key)).values())
        def _clean_config():
            _config = {
                        key: 
                            {k: CC.encrypt_text(v, __name.stem, True) if all((compiler(_PASS, k), self.encrypt, CC.convert_value(v) is not None)) \
                            else v for k, v in self._config_parser.items(key)}
                            for key in _ini_sections
                    }
            assert len(_config), LoaderException(899, __name.name)
            
            __sections_defaults = list(*(i.default_factory() for i in fields(self) if i.name=='sections'))
            if self.sections!=__sections_defaults:
                try:
                    for _ in self.sections:
                        self._config_parser.items(_)
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
                        assert good, LoaderException(1003, f'[{', '.join(self.sections)}]',  _possible_keys)
                        if __bad:
                            LoaderException(0, message=f'Invalid section{_s(__bad)}, skipping: {__bad}', _log_method=logging.warning)
                        
                        if good_has_nulls:
                            LoaderException(1002, f'[{', '.join(good_has_nulls)}]', _log_method=logging.warning)
                        
                        self._update_attrs(__name.absolute(), list(good), __name.stem)
                        return good
                    
                    assert len(_possible_keys), LoaderException(890, f'[{', '.join(self.sections)}]', _ini_sections)
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
        __config = dict((k, CConfigParser.encrypt_text(v) if all((compiler(_PASS, k), encrypt)) else v) for k,v in __sources.items())
        if __section:
            self.config[__section].update(**__config)
        return self.config.update(**__config)
