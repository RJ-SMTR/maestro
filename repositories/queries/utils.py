import re
import time
from typing import List

from google.cloud.storage.blob import Blob
from pottery.redlock import Redlock

from repositories.helpers.constants import constants


def get_largest_blob_mtime(blobs_list: List[Blob]) -> int:
    """
    Returns the largest mtime of the blobs in the list.
    """
    largest_mtime = 0
    for blob in blobs_list:
        mtime = time.mktime(blob.updated.timetuple())
        if mtime > largest_mtime:
            largest_mtime = mtime
    return largest_mtime


def filter_blobs_by_mtime(blobs_list: List[Blob], mtime: int) -> List[Blob]:
    """
    Returns the blobs in the list that have a mtime greater than the given mtime.
    """
    filtered_blobs = []
    for blob in blobs_list:
        if time.mktime(blob.updated.timetuple()) > mtime:
            filtered_blobs.append(blob)
    return filtered_blobs


def check_mat_view_dict_format(mat_view_dict: dict) -> bool:
    """ Checks if the mat_view_dict is in the correct format. """
    keys_ok = set(mat_view_dict.keys()) == set(
        constants.REDIS_MATERIALIZED_VIEW_DEFAULT_DICT_KEYS.value)
    values_ok = True
    for key in constants.REDIS_MATERIALIZED_VIEW_DEFAULT_DICT_KEYS.value:
        values_ok = values_ok and isinstance(mat_view_dict[key], str)
    return keys_ok and values_ok


def update_dict_with_dict(dict_to_update: dict, dict_to_add: dict) -> None:
    """
    Updates the dict_to_update with the dict_to_add.
    """
    for key in dict_to_add:
        dict_to_update[key] = dict_to_add[key]


def replace_table_name_with_query(table_name: str, table_query: str, original_query: str):
    """
    Replaces the table_name with the table_query in the original_query.
    """
    regular_expression = r" ?`?rj-smtr.*{}`? ?".format(table_name)
    find_counts = len(re.findall(regular_expression, original_query))
    return re.sub(regular_expression, f"({table_query})", original_query), find_counts
