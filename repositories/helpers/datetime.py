import datetime


def convert_unix_time_to_datetime(unix_time: float):
    """
    Converts a unix time to a datetime object.
    :param unix_time: The unix time to convert.
    :return: A datetime object.
    """
    return datetime.datetime.fromtimestamp(unix_time)


def convert_datetime_to_unix_time(datetime: datetime.datetime):
    """
    Converts a datetime object to a unix time.
    :param datetime: The datetime object to convert.
    :return: A unix time.
    """
    return datetime.timestamp()
