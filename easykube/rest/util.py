from collections.abc import MutableMapping


class PropertyDict(MutableMapping):
    """
    View onto a dictionary that allows property-based access.
    """
    def __init__(self, wrapped):
        self.__dict__["_wrapped"] = wrapped

    def _wrap(self, value):
        """
        If the given value is a dict, wrap it in a property dict.
        """
        if isinstance(value, dict) and not isinstance(value, self.__class__):
            return self.__class__(value)
        else:
            return value

    def __getitem__(self, key):
        return self._wrap(self.__dict__["_wrapped"].__getitem__(key))

    def __setitem__(self, key, value):
        self.__dict__["_wrapped"].__setitem__(key, value)

    def __delitem__(self, key):
        self.__dict__["_wrapped"].__delitem__(key)

    def __iter__(self):
        yield from self.__dict__["_wrapped"].keys()

    def __len__(self):
        return self.__dict__["_wrapped"].__len__()

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")

    def __setattr__(self, name, value):
        self[name] = value

    def __repr__(self):
        class_name = self.__class__.__name__
        data = self.__dict__["_wrapped"].__repr__()
        return f"{class_name}({data})"
