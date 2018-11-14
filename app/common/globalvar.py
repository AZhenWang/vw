class GL(object):

    _global_dict = {}

    @classmethod
    def set_value(cls, name, value):
        cls._global_dict[name] = value

    @classmethod
    def get_value(cls, name, default=None):
        try:
            return cls._global_dict[name]
        except KeyError:
            return default

