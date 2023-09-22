'''
Filtering utility functions
'''

from typing import List


def key_contains(key: str, contains: List[str]) -> bool:
    '''
    Check if the key contains any of the strings in the list

    :param key: The key to check
    :param contains: A list of strings to check if the key contains
    :returns: True if the key contains any of the strings in the list, False otherwise
    '''
    for c in contains:
        if c in key:
            return True
    return False