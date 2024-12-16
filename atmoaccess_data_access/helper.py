def getattr_or_keyval(obj, attrs=None, keys=None):
    if obj is None:
        return None
    if attrs is None:
        attrs = []
    if keys is None:
        keys = []
    for attr in attrs:
        try:
            obj = getattr(obj, attr, None)
        except AttributeError:
            return None
    for key in keys:
        try:
            obj = obj[key]
        except KeyError:
            return None
    return obj


def getattrs(obj, *attrs):
    if obj is None:
        return None
    for attr in attrs:
        try:
            obj = getattr(obj, attr, None)
        except AttributeError:
            return None
    return obj


def getkeyvals(obj, *keys):
    if obj is None or not isinstance(obj, dict):
        return None
    for key in keys:
        try:
            obj = obj[key]
        except KeyError:
            return None
    return obj
