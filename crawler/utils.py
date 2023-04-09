import datetime


def str2bool(value):
    return value.lower() in {'true', '1'}


def str2dt(x):
    return datetime.datetime.strptime(x, '%d.%m.%Y').date()