from contextlib import suppress
import numpy as np

from huntsman.drp.core import get_logger


def satisfies_criteria(metric_values, criteria, logger=None, name="property"):
    """ Return a boolean array indicating which values satisfy the criteria.
    Args:
        metric_values (np.array): The data to test, pretaining to a specific quality metric.
        criteria (dict): The dictionary of criteria.
        logger (Logger, optional): The logger.
        name (str, optional): The name of the property.
    Returns: (boolean array): True if satisfies criteria, False otherise.
    """
    logger = get_logger if logger is None else logger
    metric_values = np.array(metric_values)  # Make sure data is an array

    satisfies = np.ones_like(metric_values, dtype="bool")  # True where values satisfy criteria
    with suppress(KeyError):
        value = criteria["minimum"]
        logger.debug(f"Applying lower threshold in {name} of {value}.")
        satisfies = np.logical_and(satisfies, metric_values >= value)
    with suppress(KeyError):
        value = criteria["maximum"]
        logger.debug(f"Applying upper threshold in {name} of {value}.")
        satisfies = np.logical_and(satisfies, metric_values < value)
    with suppress(KeyError):
        value = criteria["equals"]
        logger.debug(f"Applying equals opterator to {name} with value {value}.")
        satisfies = np.logical_and(satisfies, metric_values == value)
    with suppress(KeyError):
        value = criteria["not_equals"]
        logger.debug(f"Applying not-equals opterator to {name} with value {value}.")
        satisfies = np.logical_and(satisfies, metric_values != value)

    logger.debug(f"{satisfies.sum()} of {satisfies.size} values satisfy criteria for {name}.")
    return satisfies
