import pandas as pd


def _set_int(data):
    return data.astype('int')


def _set_float(data):
    return data.astype('float')


def _set_str(data):
    return data.astype('str')


def _set_datetime(data):
    dt_data = pd.to_datetime(data)
    return dt_data.dt.tz_convert(None)


def set_type(type, data):
    type_selector = {
        'int': _set_int,
        'float': _set_float,
        'str': _set_str,
        'datetime': _set_datetime
    }

    try:
        type_setter = type_selector.get(type)
        return type_setter(data)
    except Exception:
        return data
