from functools import wraps


def cached_property(f):
    """Property that will replace itself with a calculated value"""
    name = "__" + f.__name__

    @wraps(f)
    def wrapper(self):
        if not hasattr(self, name):
            setattr(self, name, f(self))
        return getattr(self, name)

    return property(wrapper)
