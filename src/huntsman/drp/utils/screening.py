from contextlib import suppress
import numpy as np

from huntsman.drp.core import get_logger


def satisfies_criteria(data, criteria, logger=None, name="property"):
    """ Return a boolean array indicating which values satisfy the criteria.
    Args:
        data (np.array): The data to test.
        criteria (dict): The dictionary of criteria.
        logger (Logger, optional): The logger.
        name (str, optional): The name of the property.
    Returns: (boolean array): True if satisfies criteria, False otherise.
    """
    logger = get_logger if logger is None else logger
    data = np.array(data)  # Make sure data is an array
    satisfies = np.ones_like(data, type="bool")
    with suppress(KeyError):
        value = criteria["minimum"]
        logger.debug(f"Applying lower threshold in {name} of {value}.")
        satisfies = np.logical_and(satisfies, data >= value)
    with suppress(KeyError):
        value = criteria["maxmimum"]
        logger.debug(f"Applying upper threshold in {name} of {value}.")
        satisfies = np.logical_and(satisfies, data < value)
    with suppress(KeyError):
        value = criteria["equals"]
        logger.debug(f"Applying equals opterator to {name} with value {value}.")
        satisfies = np.logical_and(satisfies, data == value)
    with suppress(KeyError):
        value = criteria["equals"]
        logger.debug(f"Applying not-equals opterator to {name} with value {value}.")
        satisfies = np.logical_and(satisfies, data != value)
    logger.debug(f"{satisfies.sum()} of {satisfies.size} values satisfy criteria for {name}.")
    return satisfies
