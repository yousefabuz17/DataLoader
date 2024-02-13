<img src="logo/data-loader-logo.jpg" alt="DataLoader Logo" width="200"/>

# DataLoader
[![PyPI version](https://badge.fury.io/py/data-loader.svg)](https://badge.fury.io/py/data-loader)
[![Downloads](https://pepy.tech/badge/data-loader)](https://pepy.tech/project/data-loader)
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
- [Usage](#usage)
  - [DataLoader Usage](#dataloader-usage)
  - [DataMetrics Usage](#datametrics-usage)
  - [Extensions Usage](#extensions-usage)
- [Feedback](#feedback)
  - [Contact Information](#contact-information)


# Requirements
- #### **`Python`**: ~=3.10
- #### **`numpy`**: ~=1.26.4
- #### **`pytest`**: ~=7.4.3
- #### **`setuptools`**: ~=68.2.2
- #### **`pandas`**: ~=2.2.0
- #### **`pdfminer.six`**: ~=20221105
> *This project mandates the use of `Python 3.10` or later versions. Compatibility issues have been identified with `Python 3.9` due to the utilization of the `kw_only` parameter in dataclasses, a critical component for the project. It is important to note that the project may undergo a more stringent backward compatibility structure in the near future.*

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
pip install data-loader
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

### Parameters:
  - `path` (str or Path): The path of the directory from which to load files.
  - `directories` (Iterable): Additional directories to load files from, merging them with the specified path.
  - `default_extensions` (Iterable): Default file extensions to be processed.
  - `full_posix` (bool): Indicates whether to display full POSIX paths.
  - `no_method` (bool): Indicates whether to skip loading method matching execution.
  - `verbose` (bool): Indicates whether to display verbose output.
  - `generator` (bool): Indicates whether to return the loaded files as a generator; otherwise, returns as a dictionary.
  - `total_workers` (int): Number of workers for parallel execution.


### **Class & Property Methods**:
  - `load_file` (class_method): Load a specific file.
  - `get_files` (class_method): Class method to retrieve files from a directory based on default extensions and filters unwanted files.
  - `dir_files` (property): Loaded files from specified directories.
  - `files` (property): Loaded files from a single directory

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


## `Extensions`:

The `Extensions` class is a utility that provides a set of default file extensions for the `DataLoader` class. Its the back-bone for mapping all file extensions to its respective loading method. All extensions are stored in a dictionary (no period included), and the `Extensions` class provides the following key features:

### Key Features:
  - **File Extension Mapping**: Retrieve all supported file extensions with their respective loader methods.
  - **Loader Method Retrieval**: Retrieve the loader method for a specific file extension.
  - **Loader Method Check**: Check if a specific file extension has a loader method implemented that's not `open`.
  - **Supported Extension Check**: Check if a specific file extension is supported.

### Parameters:
  - No parameters are required for the `Extensions` class.
  - `Extensions()`: Initializes the `Extensions` class with all implemented file extensions and their respective loader methods.
    - Acts as a dictionary for accessing supported file extensions and their loader methods via Extensions().ALL_EXTS.

### **Class Methods**:
  - `ALL_EXTS`: Retrieve all supported file extensions with their respective loader methods.
  - `get_loader`: Retrieve the loader method for a specific file extension.
  - `has_loader`: Checks if a specific file extension has a loader method implemented thats not `open`.
  - `is_supported`: Checks if a specific file extension is supported.
---

# Usage:

## `DataLoader` Usage
  ```py
  from dataloader import DataLoader
  # Load files from a single directory
  dl = DataLoader(path="path/to/directory")
  print(dl.files) # Loaded files from a single directory (returns as a generator object)

  # Load files from multiple directories
  dl = DataLoader(directories=["path/to/directory1", "path/to/directory2"])
  print(dl.dir_files) # Loaded files from specified directories (returns as a generator object)

  # Load a specific file
  file = DataLoader.load_file("path/to/file")
  print(file) # Loaded file

  # Parameter Configuration
  dl = DataLoader(path="path/to/directory", total_workers=200, default_extensions=[".csv", ".json"], generator=False, verbose=True)
  print(dl.files) # Loaded files from a single directory with specified parameters (returns as a dictionary)

  # Load files from a single directory with specified extensions
  dl = DataLoader(directories=["path/to/directory1", "path/to/directory2"], total_workers=200, default_extensions=[".csv", ".json"], generator=False, verbose=True)
  # Loaded files from specified directories (returns as a dictionary)
  ```
---

## `DataMetrics` Usage
  ```py
  from dataloader import DataMetrics
  # Retrieve statistics for all paths
  dm = DataMetrics(files=["path/to/directory1", "path/to/directory2"])
  print(dm.all_stats) # Retrieve statistics for all paths
  # Calculate the total size of all paths
  print(dm.total_size) # Calculate the total size of all paths
  # Calculate the total number of files in all paths
  print(dm.total_files) # Calculate the total number of files in all paths
  ```
---

## `Extensions` Usage
  ```py
  from dataloader import Extensions
  ALL_EXTS = Extensions() # Initializes the Extensions class or use the default instance Extensions().ALL_EXTS
  print("csv" in ALL_EXTS) # True
  print(ALL_EXTS.get_loader("csv")) # <function read_csv at 0x7f8e3e3e3d30>
  # or
  print(ALL_EXTS.get_loader(".pickle")) # <function read_csv at 0x7f8e3e3e3d30>
  print(ALL_EXTS.has_loader("docx")) # False
  print(ALL_EXTS.is_supported("docx")) # True
  ```
---

# Feedback

Feedback is crucial for the improvement of the `DataLoader` project. If you encounter any issues, have suggestions, or want to share your experience, please consider the following channels:

1. **GitHub Issues**: Open an issue on the [GitHub repository](https://github.com/yousefabuz17/dataloader) to report bugs or suggest enhancements.

2. **Contact**: Reach out to the project maintainer via the following:

### Contact Information
    - [Discord](https://discord.com/users/581590351165259793)
    - [Gmail](yousefzahrieh17@gmail.com)

> *Your feedback and contributions play a significant role in making the `DataLoader` project more robust and valuable for the community. Thank you for being part of this endeavor!*


