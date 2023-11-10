_ERRORS = {
        404: "[ERROR CODE 404: FileNotFoundError]\n The file `{}` not does not exist.",
        13: "[ERROR CODE 13: PermissionError]\n You do not have permission to access `{}`",
        100: "[ERROR CODE 100: UnicodeDecodeError]\n There was an encoding error when reading `{}`",
        303: "[ERROR CODE 303: ParserError]\n Parsing error for file `{}`",
        400: "[ERROR CODE 400: DtypeWarning]\n There are data type warnings for `{}`.\n" \
                                            "Consider specifying data types with the dtype parameter",
        500: '[ERROR CODE 500: Exception]\n An unexpected error occured: `{}`',
        530: '[ERROR CODE 530: NoMethodFoundError]\n No loading method found for `{}`',
        607: '[ERROR CODE 607: EmptyDataError]\n The `{}` dataset is empty.',
        702: '[ERROR CODE 702: ExtensionTypeError]\n Check `{}` extensions and make sure its relative to extension loading method',
        707: '[ERROR CODE 707: PathTypeError]\n `{}` is not valid. Must be an existing file, directory, or an absolute path.',
        800: '[ERROR CODE 800: AttributeError]\n The provided argument is not valid `{}`',
        810: '[ERROR CODE 810: AttributeError]\n Cannot specify both `ext_defaults` and `all_` attributes.',
        1000: '[ERROR CODE 1000: ConfigFileError]\n `{}` is either empty or specified database contains null values.\n' \
            'Please verify the contents of your configuration file.',
        }