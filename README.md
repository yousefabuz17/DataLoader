<img src="logo/data-loader-logo.jpg" alt="DataLoader Logo" width="200"/>

# DataLoader
[![PyPI version](https://badge.fury.io/py/dynamic-loader.svg)](https://badge.fury.io/py/dynamic-loader)
[![Downloads](https://pepy.tech/badge/dynamic-loader)](https://pepy.tech/project/dynamic-loader)
[![License](https://img.shields.io/badge/license-Apache-blue.svg)](https://opensource.org/license/apache-2-0/)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://github.com/yousefabuz17/DataLoader/blob/main/README.md)
[![Code Style](https://img.shields.io/badge/code%20style-pep8-blue.svg)](https://www.python.org/dev/peps/pep-0008/)

# Table of Contents
- [Requirements](#requirements)
- [Getting Started](#getting-started)
  - [Installation Methods](#installation-methods)
    - [Cloning the Repository](#cloning-the-repository)
    - [Install via pip](#install-via-pip)
- [Overview](#overview)
- [Features](#features)
  - [DataLoader](#dataloader)
    - [Key Features](#key-features)
    - [Parameters](#parameters)
    - [Class & Property Methods](#class--property-methods)
  - [DataMetrics](#datametrics)
    - [Key Features](#key-features-1)
    - [Parameters](#parameters-1)
    - [Property Methods](#property-methods)
  - [Extensions](#extensions)
    - [Key Features](#key-features-2)
    - [Parameters](#parameters-2)
    - [Class Methods](#class-methods)
  - [GetLogger](#getlogger)
    - [Overview](#overview-1)
    - [Parameters](#parameters-3)
    - [Attributes](#attributes)
    - [Returns](#returns)
    - [Note](#note)
- [Usage](#usage)
  - [DataLoader Usage](#dataloader-usage-examples)
    - [Load Files from a Single Directory as a Generator](#load-files-from-a-single-directory-as-a-generator)
    - [Load Files from a Single Directory as a Dictionary (Custom-Repr)](#load-files-from-a-single-directory-as-a-dictionary-custom-repr)
    - [Load Files from Multiple Directories](#load-files-from-multiple-directories)
    - [Load Files with Default Extensions](#load-files-with-default-extensions)
    - [Retrieve Data for a Specific File](#retrieve-data-for-a-specific-file)
    - [Load Files with Custom Loader Methods](#load-files-with-custom-loader-methods)
    - [Specify a Custom Logger](#specify-a-custom-logger)
  - [DataMetrics Usage](#datametrics-usage)
  - [Extensions Usage](#extensions-usage)
  - [GetLogger Usage](#getlogger-usage)
- [Output Example](#output-example)
- [Future Updates](#future-updates)
- [Feedback](#feedback)
  - [Contact Information](#contact-information)


# Requirements
- #### **`Python`**: ~=3.10
- #### **`pytest`**: ~=7.4.3
- #### **`setuptools`**: ~=68.2.2
- #### **`pandas`**: ~=2.2.0
> *This project mandates the use of `Python 3.7` or later versions. Compatibility issues have been identified with the use for dataclasses in `Python 3.6` and earlier versions.*

# Getting Started

## Installation Methods
### Cloning the Repository
1. Clone the repository.
2. Install the required dependencies
```sh
pip install -r requirements.txt
```

### Install via pip
```sh
pip install dynamic-loader
pip install -r requirements.txt
```
---

# Overview.
The **DataLoader** project is a comprehensive utility that facilitates the efficient loading and processing of data from specified directories. This project is designed to be user-friendly and easy to integrate into your projects.

The **DataMetrics** class focuses on processing data paths and gathering statistics related to the file system and specified paths. Also allows the ability to export all statistics to a JSON file.

The **Extensions** class is a utility that provides a set of default file extensions for the `DataLoader` class. Its the back-bone for mapping all file extensions to its respective loading method.

---

# Features

## `DataLoader`

The `DataLoader` class is specifically designed for loading and processing data from directories. It provides the following key features:

### Key Features:
  - **Dynamic Loading**: Load files from a single directory or merge files from multiple directories.
  - **Flexible Configuration**: Set various parameters, such as default file extensions, full POSIX paths, method loader execution, and more.
  - **Parallel Execution**: Leverage parallel execution with the `total_workers` parameter to enhance performance.
  - **Verbose Output**: Display verbose output to track the loading process.
    - If enabled, the `verbose` parameter will display the loading process for each file.
    - If disabled, the `verbose` parameter will write the loading process for each file to a log file.
  - **Custom Loaders**: Implement custom loaders for specific file extensions.
    - Please note that at the moment, the loading methods kwargs will be uniformly applied to all files with the specified extension.
    - Additionally, the first parameter of the loader method is automatically passed and should be skipped. If passed, the loader will fail and return the contents of the file as `TextIOWrapper`.
    >***Future updates will include the ability to specify what loader method to use for a specific files efficiently.***

### Parameters:
  - `path` (str or Path): The path of the directory from which to load files.
  - `directories` (Iterable): An iterable of directories from which to all files.
  - `default_extensions` (Iterable): Default file extensions to be processed.
  - `full_posix` (bool): Indicates whether to display full POSIX paths.
  - `no_method` (bool): Indicates whether to skip loading method matching execution.
  - `verbose` (bool): Indicates whether to display verbose output.
  - `generator` (bool): Indicates whether to return the loaded files as a generator; otherwise, returns as a dictionary.
  - `total_workers` (int): Number of workers for parallel execution.
  - `log` (Logger): A configured logger instance for logging messages. (Refer to the [GetLogger](#getlogger) class for more information on how to create a logger instance using the `GetLogger` class.)
  - `ext_loaders` (dict[str, Any, dict[key-value]]): Dictionary containing extensions mapped to specified loaders. (Refer to the [Extensions](#extensions) class for more information)


### **Class & Property Methods**:
  - `load_file` (class_method): Load a specific file.
  - `get_files` (class_method): Retrieve files from a directory based on default extensions and filters unwanted files.
  - `dir_files` (property): Loaded files from specified directories.
  - `files` (property): Loaded files from a single directory
  - `all_exts` (property): Retrieve all supported file extensions with their respective loader methods being used.
  - `EXTENSIONS` (*Extensions* class instance): Retrieve all default supported file extensions with their respective loader methods.
---

## `DataMetrics`

The `DataMetrics` class focuses on processing data paths and gathering statistics related to the file system. Key features include:

### Key Features:
  - **OS Statistics**: Retrieve detailed statistics for each path, including symbolic link status, calculated size, and size in bytes.
  - **Export to JSON**: Export all statistics to a JSON file for further analysis and visualization.

### Parameters:
  - `paths` (Iterable): Paths for which to gather statistics.
  - `file_name` (str): The file name to be used when exporting all files metadata stats.
  - `full_posix` (bool): Indicates whether to display full POSIX paths.

### **Property Methods**:
  - `all_stats`: Retrieve statistics for all paths.
  - `total_size`: Calculate the total size of all paths.
  - `total_files`: Calculate the total number of files in all paths.
  - `export_stats()`: Export all statistics to a JSON file.

### **OS Stats Results**:
  - `os_stats_results`: OS statistics results for each path.
  - Custom Stats:
    - `st_fsize`: Full file size statistics.
    - `st_vsize`: Full volume size statistics.
---

## `Extensions`:

The `Extensions` class is a utility that provides a set of default file extensions for the `DataLoader` class. Its the back-bone for mapping all file extensions to its respective loading method. All extensions are stored in a dictionary (no period included), and the `Extensions` class provides the following key features:

### Key Features:
  - **File Extension Mapping**: Retrieve all supported file extensions with their respective loader methods.
  - **Loader Method Retrieval**: Retrieve the loader method for a specific file extension.
  - **Loader Method Check**: Check if a specific file extension has a loader method implemented that's not `open`.
  - **Supported Extension Check**: Check if a specific file extension is supported.
  - **Customization**: Customize the `Extensions` class with new files extensions and its respective loader methods.

### Parameters:
  - No parameters are required for the `Extensions` class.
  - `Extensions()`: Initializes the `Extensions` class with all implemented file extensions and their respective loader methods.
    - Acts as a dictionary for accessing supported file extensions and their loader methods via Extensions().ALL_EXTS.

### **Class Methods**:
  - `ALL_EXTS`: Retrieve all supported file extensions with their respective loader methods.
  - `get_loader`: Retrieve the loader method for a specific file extension.
  - `has_loader`: Checks if a specific file extension has a loader method implemented thats not `open`.
  - `is_supported`: Checks if a specific file extension is supported.
  - `customize`: Customize the `Extensions` class with new files extensions and its respective loader methods.
    - Specified loading method will be converted to a lambda function to support kwargs.
    - The first parameter of the loader method is automatically passed and should be skipped. If passed, the loader will fail and return the contents of the file as `TextIOWrapper`.
    - Future updates will include the ability to specify what loader method to use for a specific files efficiently.
    - The loader method kwargs will be uniformly applied to all files with the specified extension.
    - Example:
      ```py
      # Structure: {extension: {loader_method: {kwargs}}}
      ext_loaders = {"csv": {pd.read_csv: {"header": 10}}}
      ```
---

## `GetLogger`
## Overview
The `GetLogger` class is a utility that provides a method to get a configured logger instance for logging messages. It is designed to be user-friendly and easy to integrate into your projects.

## Parameters

- `name` (str, optional): The name of the logger. Defaults to the name of the calling module.
- `level` (int, optional): The logging level. Defaults to logging.DEBUG.
- `formatter_kwgs` (dict, optional): Additional keyword arguments for the log formatter.
- `handler_kwgs` (dict, optional): Additional keyword arguments for the log handler.
- `mode` (str, optional): The file mode for opening the log file. Defaults to "a" (append).

## Attributes
- `refresher` (callable): A method to refresh the log file.
- `set_verbose` (callable): A method to set the verbosity of the logger.

## Returns
- Logger: A configured logger instance.

## Notes
- This function sets up a logger with a file handler and an optional stream (console) handler for verbose logging.
- If `verbose` is True, log messages will be printed to the console instead of being written to a file.

---

# Usage:

## `DataLoader` Usage Examples

### Load Files from a Single Directory as a Generator

```python
from data_loader import DataLoader

# Load all files with a specified path (directory) as a Generator
dl_gen = DataLoader(path="path/to/directory")
dl_files_gen = dl_gen.files
print(dl_files_gen)
# Output:
# <generator object DataLoader.files.<key-value> at 0x1163f4ba0>
```

### Load Files from a Single Directory as a Dictionary (Custom-Repr)

```python
from data_loader import DataLoader

# Load all files with a specified path (directory) as a Dictionary (Custom-Repr)
# Disabling 'generator' and 'full_posix' for displaying purposes.
dl_dict = DataLoader(path="path/to/directory", generator=False, full_posix=False)
dl_files_dict = dl_dict.files
print(dl_files_dict)
# Output:
# DataLoader((LICENSE.md, <TextIOWrapper>),
#             (requirements.txt, <Str>),
#             (Makefile, <Str>),
#             ...
#             (space_4.txt, <Str>))
```

### Load Files from Multiple Directories

```python
from data_loader import DataLoader

# Load all files from multiple directories
# Disabling 'generator' and 'full_posix' for displaying purposes.
dl = DataLoader(directories=["path/to/dir1", "path/to/dir2"], generator=False, full_posix=False)
dl_dir_files = dl.dir_files
print(dl_dir_files)
# Output:
# DataLoader((file1.txt, <Str>),
#             (file2.txt, <Str>),
#             (file3.txt, <Str>),
#             ...
#             (fileN.txt, <Str>))
```

### Load Files with Default Extensions

```python
from data_loader import DataLoader

# Load all files with default extensions
dl_default = DataLoader(path="path/to/directory", default_extensions=["csv"], generator=False, full_posix=False)
dl_default_files = dl_default.files
print(dl_default_files)
# Output:
# DataLoader((file1.csv, <DataFrame>),
#             (file2.csv, <DataFrame>),
#             ...
#             (fileN.csv, <DataFrame>))
```

### Retrieve Data for a Specific File

```python
from data_loader import DataLoader

# Retrieve data for a specific file
dl_files = DataLoader(path="path/to/directory", generator=False, full_posix=False).files
dl_specific_file_data = dl_files["file1.csv"]
# Output:
# <DataFrame>
```

### Load Files with Custom Loader Methods

```python
from data_loader import DataLoader
import pandas as pd

# Specify your own custom loader methods
dl_custom = DataLoader(path="path/to/directory", ext_loaders={"csv": {pd.read_csv: {"nrows": 10}}}, generator=False, full_posix=False)
dl_custom_files = dl_custom.files
print(dl_custom_files)
# Output:
# DataLoader((file1.csv, <DataFrame>),
#             (file2.csv, <DataFrame>),
#             ...
#             (fileN.csv, <DataFrame>))
# Note: The 'nrows' will be dynamically passed to the 'pd.read_csv' method for each file.
```

### Specify a Custom Logger

```python
from data_loader import DataLoader
import logging

# Specify your own custom logger
custom_logger = logging.getLogger("DataLoader")
dl_with_logger = DataLoader(path="path/to/directory", log=custom_logger)
dl_logger_files = dl_with_logger.files
print(dl_logger_files)
# Output:
# <generator object DataLoader.files.<key-value> at 0x1163f4ba0>
# Note: The logger will be used to log or stream messages.
```
---

## `DataMetrics` Usage
  ```py
  from data_loader import DataMetrics
  # Retrieve statistics for all paths
  dm = DataMetrics(files=["path/to/directory1", "path/to/directory2"])
  print(dm.all_stats) # Retrieve statistics for all paths
  # Calculate the total size of all paths
  print(dm.total_size) # Calculate the total size of all paths
  # Calculate the total number of files in all paths
  print(dm.total_files) # Calculate the total number of files in all paths
  dm.export_stats() # Export all statistics to a JSON file
  ```
---

## `Extensions` Usage
  ```py
  from data_loader import Extensions
  ALL_EXTS = Extensions() # Initializes the Extensions class or use the default instance Extensions().ALL_EXTS
  print("csv" in ALL_EXTS) # True
  print(ALL_EXTS.get_loader("csv")) # <function read_csv at 0x7f8e3e3e3d30>
  # or
  print(ALL_EXTS.get_loader(".pickle")) # <function read_csv at 0x7f8e3e3e3d30>
  print(ALL_EXTS.has_loader("docx")) # False
  print(ALL_EXTS.is_supported("docx")) # True
  

  ALL_EXTS.customize({"docx": {open: {mode="rb"}},
                          "png": {PIL.Image.open: {}}}) # Customize the Extensions class with a new file extension and loader method
  
  print(ALL_EXTS.get_loader("docx")) # <function <lambda> at 0x7f8e3e3e3d30>
  ```
---

### `GetLogger` Usage:

```python
# Create a logger with default settings
from data_loader import GetLogger
logger = GetLogger().logger
logger.info("This is an info message")  # Writes to the log file

# Create a logger with custom settings
logger = GetLogger(name='custom_logger', level=logging.INFO, verbose=True).logger
logger.info("This is an info message")  # Prints to the console

# Initiate verbosity
logger = GetLogger().logger
logger.set_verbose(True)
CustomException("Error Message")  # Prints to the console

# Disable verbosity
logger.set_verbose(False).logger
CustomException("Error Message")  # Writes to the log file
```
---

## `DataMetrics` Usage Examples

```python
from data_metrics import DataMetrics

# Create a DataMetrics instance with paths and corresponding metadata
dm = DataMetrics(("path/to/directory1", <Dict>),
                 ("path/to/directory2", <Dict>))

# Access metadata for a specific path
metadata_directory1 = dm["path/to/directory1"]
print(metadata_directory1)
# Output:
# {'os_stats_results': <os_stats_results>,
#  'st_fsize': Stats(symbolic='6.20 KB', calculated_size=6.19921875, bytes_size=6348),
#  'st_vsize': {'total': Stats(symbolic='465.63 GB (Gigabytes)', calculated_size=465.62699127197266, bytes_size=499963174912),
#               'used': Stats(symbolic='131.60 GB (Gigabytes)', calculated_size=131.59552001953125, bytes_size=141299613696),
#               'free': Stats(symbolic='334.03 GB (Gigabytes)', calculated_size=334.0314712524414, bytes_size=358663561216)}}

# Export all statistics to a JSON file
dm.export_stats(file_path="all_metadata_stats.json")

# Calculate the total size of all paths
total_size = dm.total_size
print(total_size)
# Output:
# Stats(symbolic='471.76 GB (Gigabytes)', calculated_size=471.75720977783203, bytes_size=507012679260)

# Calculate the total number of files in all paths
total_files = dm.total_files
print(total_files)
# Output:
# 215
```
---

# Future Updates
- [ ] Include the ability to specify loader methods for individual files, providing greater flexibility.
- [ ] Intend to add an option for special representation of loaded files, displaying all contents rather than just the data type.
- [ ] Add more comprehensive tests covering all implemented features.
  - [ ] Include specific tests for the `ext_loaders` parameter.
- [ ] Add loading method keyword argument support for the `load_file` class method.
  - [ ] Implement a more efficient method for specifying loader methods kwargs for specific files rather than applying them uniformly.

---
# Feedback

Feedback is crucial for the improvement of the `DataLoader` project. If you encounter any issues, have suggestions, or want to share your experience, please consider the following channels:

1. **GitHub Issues**: Open an issue on the [GitHub repository](https://github.com/yousefabuz17/dataloader) to report bugs or suggest enhancements.

2. **Contact**: Reach out to the project maintainer via the following:

### Contact Information
- [Discord](https://discord.com/users/581590351165259793)
- [Gmail](yousefzahrieh17@gmail.com)

> *Your feedback and contributions play a significant role in making the `DataLoader` project more robust and valuable for the community. Thank you for being part of this endeavor!*


