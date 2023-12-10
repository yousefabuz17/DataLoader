__all__ = ('_PASS', '_ERRORS')

_PASS = (
        "Password",
        "Passcode",
        "Secret",
        "PassKey",
        "AccessCode",
        "SecurityPhrase",
        "AuthenticationToken",
        "PIN",
        "Passphrase",
        "SecureKey",
        "Cipher",
        "LockCode",
        "AuthenticationCode",
        "SecurePassword",
        "EncryptionKey",
        'HiddenKey',
        'Token',
        )

_ERRORS = {
        None: '{}',
        -1000: 'CODE -1000: HumanError>>Error code {} is not being used anymore.',
        -1: 'CODE -1: ImportantUserMessage>>Please exercise caution when providing specific keyword arguments, depending on how the DataLoader is initialized. In case of any errors, you can utilize the \'no_method\' keyword to return all files as IO.textWrapper.',
        0: 'CODE 0: EmptyDataLoaderError>>No files were detected. Please initialize a DataLoader instance with validated file paths to proceed.',
        1: 'CODE 1: NoDundersOrDynamicError>>{!r} does not support the required __dunder__ methods or lacks ({!r}, {!r}) capabilities.\n>>Please include the necessary dynamic arguments, otherwise, initialize the {!r} and will return as a generator.\n>>If the issue persists, ensure that the {!r} object has been correctly initialized and invoked.\n>>Additionally, double-check all instances are correct.',
        7: 'CODE 7: SQLConfigCreation>>An empty SQL .INI configuration file has been successfully created as {!r}.\n' \
            'Once you have made the necessary modifications, please re-run ConfigManager using the updated SQL configuration file to include encryption.\n',
        13: 'CODE 13: PermissionError>>You do not have permission to access {!r}',
        17: 'CODE 17: AddingDataLoaderError>>{!r} is not an instance of {}.',
        18: 'CODE 18: AddingMessage>>DataLoader files are treated as a dictionary. Please be cautious if any file names are relevant when adding DataLoader objects.',
        25: 'CODE 25: DataLoaderSizeOfError>> If the issue persists, exercise caution to ensure that the returned object is not a generator; review the parameters provided to the {!r} for potential discrepancies.',
        100: 'CODE 100: UnicodeDecodeError>>There was an encoding error when reading {!r}',
        102: 'CODE 102: JSONDecodeError>>Failed to decode JSON: {!r}. Position: {!r}. Line: {!r}. Column: {!r}.',
        150: 'CODE 150: InjectingFilesError>>Requires at least one valid directory to be injected.',
        170: 'CODE 170: ComparingSetsError>>Provided arguments are not suitable formats for comparing.\nERROR: {}',
        200: 'CODE 200: DataLoaderAttributeError>>{!r} necessitates a valid file path. Please review all paths before initiating.',
        201: 'CODE 201: DataLoaderCallingError>>Cannot specify both \'dynamic\' and \'generator\' options. Omitting both will default to returning an instance of {!r}.',
        202: 'CODE 202: DataLoderAttributeError>>Cannot specify both \'defaults\' and \'all_\' attributes. Omit both and {!r} will use the following extensions by default:\n{!r}.',
        210: 'CODE 210: DefaultExtensionsError>>All provided extension{} are invalid: {!r}\nAll available extensions:\n{!r}',
        215: 'CODE 215: ExtensionsError>>Skipping invalid extensions: {!r}',
        220: 'CODE 220: AddingFilesError>>The input must include at least one valid file path.',
        221: 'CODE 221: GetKeyError>>{!r} is not a valid key. Did you mean {!r}?',
        222: 'CODE 222: NoKeyError>>A key must be provided; otherwise, pass in None to return None.',
        223: 'CODE 223: NoItemError>>{!r} was not found within the loaded files. Did you mean {!r}?',
        225: 'CODE 225: NoAttributeError>>{!r} is not a valid attribute name. Did you mean {!r}?',
        227: 'CODE 227: GetAttributeError>>Using __getitem__ is not suitable. File names with unique extensions have been identified. It is imperative to include the file extensions.',
        228: 'CODE 228: DistinctFileNamesError>> File names with unique extensions have been identified. ',
        230: 'CODE 230: DataLoaderPathError>>A valid path is required. If a path is provided, then the value of {!r} is considered invalid.\nError message: {!r}',
        270: 'CODE 270: IntegrityCheckerError>>The integrity checker has failed during the loading process, indicating potential data tampering.\n>>ERROR: {}',
        303: 'CODE 303: ParserError>>Parsing error for file {!r}',
        370: 'CODE 370: FileStatsError>>Ensure that all provided paths are specified as absolute paths. {!r} only works with instances of posix.\n>>{}',
        371: 'CODE 371: NeedsPosixError>>For optimal compatibility, "posix" attr must be passed in when using {} attributes.\n>>Provided paths will default to POSIX for you, ensuring no errors are raised if all are validated.',
        400: 'CODE 400: DtypeWarning>>There are data type warnings for {!r}.\nConsider specifying data types with the dtype parameter',
        402: 'CODE 402: OSError>>{!r} obtained an OSError.\n{!r}',
        404: 'CODE 404: FileNotFoundError>>The file {!r} does not exist.',
        500: 'CODE 500: Exception>>An unexpected error occurred: {!r}',
        530: 'CODE 530: NoMethodFoundError>>Unable to identify a suitable loading method for {!r}. Please ensure that the file is accessible, is set to read or read-write only and is capable of being opened.',
        702: 'CODE 702: ExtensionTypeError>>Check file {!r} extensions. Failed to find a relative working loading method ({!r}). Defaulting to {!r}',
        607: 'CODE 607: EmptyDataError>>{!r} is an empty dataset. No columns to parse. File will be skipped.',
        707: 'CODE 707: PathTypeError>>{!r} is not valid. Must be an existing file, directory, or an absolute path.',
        800: 'CODE 800: AttributeError>>The provided argument is not valid {!r}',
        870: 'CODE 870: ConfigDatabaseError>>{!r} is an unsupported database type.',
        880: 'CODE 880: NoConfigFileError>>A configuration file is required.',
        890: 'CODE 890: ConfigSectionsError>>No sections were found for {!r}',
        899: 'CODE 899: ConfigValuesError>>No values were found for {!r}',
        1000: 'CODE 1000: ConfigFileError>>{!r} is currently empty and only contains null values.\nPlease verify the contents of your configuration file before proceeding.',
        1001: 'CODE 1001: ConfigSectionError>>{!r} -> Spelling errors detected. Possible Sections based on arguments provided are as follows\n({!r})',
        1002: 'CODE 1002: ConfigEmptySectionError>>{!r}... was found but is currently empty and/or contains null values.\n',
        1003: 'CODE 1003: ConfigAttributeSectionError>>The provided key sections {!r} are invalid. Please review the arguments being passed.',
}

__str__ = __repr__ = lambda: f'<module {"constants"!r} from {__file__!r}\nDEFAULTS:\n{dict(zip(__all__,(_PASS, list(_ERRORS))))}'
__version__ = '1.0.0'
__doc__ = f"""
Module: constansts (DataLoader)

This module defines constants and error messages used in the DataLoader project.

Constants:
    - _PASS: Tuple[str]: A tuple containing strings representing various password-related terms.
    - _ERRORS: Dict[int, str]: A dictionary containing error codes and corresponding error messages.

Error Codes and Descriptions (Sample):
    - CODE -1000: HumanError >> Error code {{}} is not being used anymore.
    - CODE -1: ImportantUserMessage >> Please exercise caution when providing specific keyword arguments...
    - CODE 0: EmptyDataLoaderError >> No files were detected. Please initialize a DataLoader instance...

Functions:
    - __defaults__(): Returns a tuple containing _PASS and _ERRORS.
    - __str__()|__repr__(): Returns the string representation of __all__ values.
    - __version__: Module version set to {__version__!r}.

Usage:
    - Import the module using 'from constants import *' to access the defined constants.

Example:
    >>> from constants import _PASS, _ERRORS
    >>> print(_PASS)
    ['Password', 'Passcode', ...]
    >>> print(_ERRORS[-1])
    'CODE -1: ImportantUserMessage >> Please exercise caution when providing specific keyword arguments...'
"""
