
_PASS = [
        "Password",
        "Passcode",
        "Secret",
        "Key",
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
    ]

_ERRORS = {
        -1: '[CODE -1: HumanError]\nCheck if error code `{}` is still being used.',
        404: '[CODE 404: FileNotFoundError]\nThe file `{}` not does not exist.',
        7: '[CODE 7: SQLConfigCreation]\nAn _empty SQL .INI configuration file has been successfully created as `{}`.\n' \
            'Once you have made the necessary modifications, please re-run ConfigInfo using the updated SQL configuration file.\n',
        13: '[CODE 13: PermissionError]\nYou do not have permission to access `{}`',
        100: '[CODE 100: UnicodeDecodeError]\nThere was an encoding error when reading `{}`',
        303: '[CODE 303: ParserError]\nParsing error for file `{}`',
        400: '[CODE 400: DtypeWarning]\nThere are data type warnings for `{}`.\n' \
                                            'Consider specifying data types with the dtype parameter',
        500: '[CODE 500: Exception]\nAn unexpected error occured: `{}`',
        530: '[CODE 530: NoMethodFoundError]\nNo loading method found for `{}`',
        607: '[CODE 702: ExtensionTypeError]\nCheck `{}` extensions and make sure its relative to extension loading method',
        607: '[CODE 702: ExtensionPandasError]\n`{}` is an _empty dataset',
        707: '[CODE 707: PathTypeError]\n`{}` is not valid. Must be an existing file, directory, or an absolute path.',
        800: '[CODE 800: AttributeError]\nThe provided argument is not valid `{}`',
        810: '[CODE 810: ConfigAttributeError]\nCannot specify both `ext_defaults` and `all_` attributes.',
        870: '[CODE 870: ConfigDatabaseError]\n`{}` is an unsupported database type.',
        880: '[CODE 880: ConfigDatabaseInputError]\nConfiguration file is needed.',
        890: '[CODE 890: ConfigSourcesError]\nNo sections were found for {}',
        1000: '[CODE 1000: ConfigFileError]\n`{}` is currently _empty and only contains null values.\n' \
            'Please verify the contents of your configuration file.',
        1001: '[CODE 1001: ConfigValueError]\n`{}` was found but is currently _empty and/or contains null values.\n' \
            'Please verify the contents of your configuration file before proceeding.',
        
        }