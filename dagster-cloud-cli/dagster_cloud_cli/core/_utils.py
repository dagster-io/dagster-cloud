# Same as dagster._utils.merge_dicts
def merge_dicts(*args) -> dict:
    """Returns a dictionary with with all the keys in all of the input dictionaries.

    If multiple input dictionaries have different values for the same key, the returned dictionary
    contains the value from the dictionary that comes latest in the list.
    """
    result: dict = {}
    for arg in args:
        result.update(arg)
    return result
