"""
Error handling constants and exception classes for distrepos.
"""

#
# Exit codes
#

ERR_CONFIG = 3
ERR_RSYNC = 4
ERR_FAILURES = 5
ERR_EMPTY = 6

#
# Error classes
#


class ProgramError(RuntimeError):
    """
    Class for fatal errors during execution.  The `returncode` parameter
    should be used as the exit code for the program.
    """

    def __init__(self, returncode, *args):
        super().__init__(*args)
        self.returncode = returncode


class RsyncError(ProgramError):
    """Class for fatal errors with rsync"""

    def __init__(self, *args):
        super().__init__(ERR_RSYNC, *args)

    def __str__(self):
        return f"rsync error: {super().__str__()}"


class ConfigError(ProgramError):
    """Class for errors with the configuration"""

    def __init__(self, *args):
        super().__init__(ERR_CONFIG, *args)

    def __str__(self):
        return f"Config error: {super().__str__()}"


class MissingOptionError(ConfigError):
    """Class for missing a required option in a config section"""

    def __init__(self, section_name: str, option_name: str):
        super().__init__(
            f"Section [{section_name}] missing or empty required option {option_name}"
        )


class TagFailure(Exception):
    """
    Class for failure for a specific tag.  Not meant to be fatal.
    """
