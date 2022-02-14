class PropertyDict(dict):
    """
    Dictionary implementation that also supports property-based access (read-only).
    """
    def __getitem__(self, key):
        value = super().__getitem__(key)
        return self.__class__(value) if isinstance(value, dict) else value

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")

    def __setattr__(self, name, value):
        self[name] = value
