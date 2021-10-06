

from typing import List, Union

from pottery import Redlock


def acquire(locks: Union[List[Redlock], Redlock]):
    if isinstance(locks, Redlock):
        locks = [locks]
    for lock in locks:
        lock.acquire()


def release(locks: Union[List[Redlock], Redlock]):
    if isinstance(locks, Redlock):
        locks = [locks]
    for lock in locks:
        try:
            lock.release()
        except:
            pass
