from importlib import import_module


class Mappings(dict[str, dict[str, str]]):
    """
    A dictionary that maps a module name to a dictionary of mappings defined in
    that module. This allows for only importing the mapping that is needed for
    a given job.
    """

    def __missing__(self, key):
        """
        Import the module and return the mapping, caching the mapping on the
        dictionary so that it is only imported once.
        """
        try:
            mapping = import_module(f".{key}", __package__).MAPPING
            # cache the mapping so that it is only imported once
            self[key] = mapping
            return mapping
        except ModuleNotFoundError as e:
            raise KeyError(key) from e


__all__ = [Mappings]
