import os
from typing import Any


class EnvironmentVariable:
    """
    Get the credentials from the Environment Variables
    """

    args = {}
    cache = {}

    def __init__(self, args: dict = None) -> None:  # type: ignore
        if args is None:
            args = dict()
        self.args = args

    def get_var(self, key, default: Any = None, throw_error: bool = False) -> Any:
        """
        Get the value of a variable from the environment variables.

        :param key: The key of the variable
        :param default: The default value if the variable is not found.
        :param throw_error: If true, throw an error if the variable is not found.
        :return: The value of the variable.
        """
        if key in self.args and self.args[key] is not None and self.args[key] != "":
            self.cache[key] = self.args[key]
        elif os.getenv(key) is not None:
            self.cache[key] = os.getenv(key)
        elif throw_error:
            raise Exception("Environment variable {} is not set".format(key))
        else:
            self.cache[key] = default
        return self.cache[key]


envs_instance = None


def get_envs(args: dict = None) -> EnvironmentVariable:  # type: ignore
    """
    Get the envs instance. If it doesn't exist, create it.

    :param args: The arguments to pass to the envs.
    :return: The envs.
    """
    global envs_instance
    if args is None:
        args = dict()
    if envs_instance is None:
        envs_instance = EnvironmentVariable(args)
    return envs_instance
