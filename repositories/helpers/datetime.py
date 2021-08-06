import croniter
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


def determine_whether_to_execute_or_not(cron_expression: str, datetime_now: datetime.datetime, datetime_last_execution: datetime.datetime):
    """
    Determines whether the cron expression is currently valid.
    :param cron_expression: The cron expression to check.
    :param datetime_now: The current datetime.
    :param datetime_last_execution: The last datetime the cron expression was executed.
    :return: True if the cron expression is valid, False otherwise.
    """
    cron_expression_iterator = croniter.croniter(
        cron_expression, datetime_last_execution)
    next_cron_expression_time = cron_expression_iterator.get_next(
        datetime.datetime)
    if next_cron_expression_time < datetime_now:
        return True
    else:
        return False


def convert_datetime_string_to_datetime(datetime_string: str, format: str = "%Y-%m-%d %H:%M:%S"):
    """
    Converts a datetime string to a datetime object.
    :param datetime_string: The datetime string to convert.
    :param format: Format for conversion. Default is "%Y-%m-%d %H:%M:%S".
    :return: A datetime object.
    """
    return datetime.datetime.strptime(datetime_string, format)


def convert_datetime_to_datetime_string(datetime: datetime.datetime, format: str = "%Y-%m-%d %H:%M:%S"):
    """
    Converts a datetime object to a datetime string.
    :param datetime: The datetime object to convert.
    :param format: Format for conversion. Default is "%Y-%m-%d %H:%M:%S".
    :return: A datetime string.
    """
    return datetime.strftime(format)
