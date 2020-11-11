from collections import abc
from contextlib import suppress
import nimpy as np

# These are responsible for converting string keys into equivalent mongoDB operators
MONGO_OPERATORS = {"equals": "$eq",
                   "not_equals": "$ne",
                   "greater_than": "$gt",
                   "greater_than_equals": "$gte",
                   "less_than": "$lt",
                   "less_than_equals": "$lte",
                   "in": "$in",
                   "not_in": "$nin"}

# These are responsible for applying logical operators based on a string key
OPERATORS = {"equals": lambda x, y: x == y,
             "not_equals": lambda x, y: x != y,
             "greater_than": lambda x, y: x > y,
             "greater_than_equals": lambda x, y: x >= y,
             "less_than": lambda x, y: x < y,
             "less_than_equals": lambda x, y: x <= y,
             "in": lambda x, y: np.isin(x, y),
             "not_in": lambda x, y: np.isin(x, y, invert=True)}


def encode_mongo_value(value):
    """ Encode object for a pymongodb query.
    Args:
        value (object): The value to encode.
    Returns:
        object: The encoded value.
    """
    if isinstance(abc.Iterable, value):
        return [encode_mongo_value(v) for v in value]
    elif isinstance(value, np.bool_):
        return bool(value)
    elif isinstance(value, np.int64):
        return int(value)
    elif isinstance(value, np.float64):
        return float(value)
    return value


def criteria_is_satisfied(values, criteria):
    """ Return a boolean array indicating which values satisfy the criteria.
    Args:
        values (np.array): The test values.
        criteria (abc.Mapping): The criteria dictionary.
    Returns:
        boolean array: True if satisfies criteria, False otherise.
    """
    satisfied = np.ones_like(values, dtype="bool")
    for operator, opvalue in criteria.items():
        satisfied = np.logical_and(satisfied, OPERATORS[operator](values, opvalue))
    return satisfied


class QueryCriteria():
    """ The purpose of this class is to provide an abstract implementation of a query criteria,
    allowing configured criteria to be easily converted to whatever format the database requires
    and be applied to DataFrames.
    """

    def __init__(self, query_criteria):
        """
        Args:
            criteria (abc.Mappable): The query criteria.
        """
        self.query_criteria = query_criteria
        # Make sure the query criteria is valid
        operator_keys = list(OPERATORS.keys())
        for column_name, criteria in self.query_criteria.items():
            for key in criteria.keys():
                if key not in operator_keys:
                    raise ValueError("Unrecognised operator key in query criteria for"
                                     f" {column_name} column: {key}. Valid columns are:"
                                     f" {operator_keys}.")

    def to_mongo(self):
        """ Return the criteria as a dictionary suitable for pymongo.
        Returns:
            dict: The query dictionary.
        """
        new = {}
        for column_name, criteria in self.query_criteria.items():
            d = {}
            for k, v in criteria.items():
                with suppress(KeyError):
                    k = MONGO_OPERATORS[k]
                if v is not None:
                    d[k] = encode_mongo_value(v)
            new[column_name] = d
        return new

    def is_satisfied(self, df):
        """ Return a boolean array indicating which rows satisfy the criteria.
        Args:
            df (pd.DataFrame): The DataFrame to test.
        Returns:
            boolean array: True if satisfies criteria, False otherise.
        """
        satisfied = np.ones(df.shape[0], dtype="bool")
        for column_name, column_criteria in self.query_criteria.keys():
            values = df[column_name].values
            satisfied = np.logical_and(satisfied, criteria_is_satisfied(values, column_criteria))
        return satisfied

    def get_filtered_dataframe(self, df):
        """ Return a copied DataFrame with only the rows that satisfy the criteria retained.
        Args:
            df (pd.DataFrame): The original DataFrame.
        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        satisfied = self.is_satisfied(df)
        return df[satisfied].reset_index(drop=True).copy()
