"""
Screeners are objects that facilitate convenient and configurable filtering of query results based
on metadata. Screeners are created and configured for specific data types, e.g. raw bias frames.
Screeners can be parsed to the `query` method of `huntsman.drp.datatable.DataTable` objects.
"""
import numpy as np

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.core import get_config
from huntsman.drp.utils.library import load_module
from huntsman.drp.utils.query import QueryCriteria


class Screener(HuntsmanBase):

    _data_type_key = "dataType"

    def __init__(self, data_type, config=None, logger=None, **kwargs):
        super().__init__(config=config, logger=logger, **kwargs)
        self.data_type = data_type
        self.screen_config = self.config["screening"][self.data_type]
        # Prepare the reference table
        self._reference_key = self.screen_config["ref_table_key"]
        reference_table_class = load_module(self.screen_config["ref_table_class"])
        self._ref_table = reference_table_class(config=self.config, logger=self.logger)

    def screen(self, df):
        """ Apply screening to DataFrame and return boolean array indicating valid rows.
        Args:
            df (pd.DataFrame): The catalogue to screen.
        Returns:
            np.array of boolean type: True where rows should be kept.
        """
        if "criteria" not in self.screen_config.keys():
            return np.ones(df.shape[0], dtype="bool")

        # Only select rows of the correct type
        screen_result = df[self._data_type_key].values == self.data_type

        # Get the matches in the reference table
        df_ref = self._ref_table.query_matches(values=df[self._reference_key].values)

        # Get the matches in the reference table
        df_ref = self._ref_table.query_matches(values=df[self._reference_key].values)

        # Apply the configured criteria
        criteria = QueryCriteria(self.screen_config["criteria"])
        meets_criteria = criteria.is_satisfied(df_ref)
        screen_result = np.logical_and(screen_result, meets_criteria)

        return screen_result

    def screen_dataframe(self, df):
        """ Apply screening to DataFrame and return DataFrame with only valid rows. """
        cond = self.screen(df)
        return df[cond].reset_index(drop=True).copy()


class CompoundScreener(HuntsmanBase):
    """
    Similar to a normal Screener, but for multiple data types.
    """

    def __init__(self, data_types, **kwargs):
        super().__init__(**kwargs)
        self.screeners = {}
        for data_type in data_types:
            self.screeners[data_type] = Screener(data_type, config=self.config, logger=self.logger)

    def screen(self, df):
        """ Apply screening to DataFrame and return boolean array indicating valid rows.
        Args:
            df (pd.DataFrame): The catalogue to screen.
        Returns:
            np.array of boolean type: True where rows should be kept.
        """
        screen_result = np.zeros(df.shape[0], dtype="bool")
        for data_type, screener in self.screeners.items():
            self.logger.debug(f"Applying screening for data type={data_type}.")
            screen_result = np.logical_or(screen_result, screener.screen(df))
        return screen_result

    def screen_dataframe(self, df):
        """ Apply screening to DataFrame and return DataFrame with only valid rows. """
        cond = self.screen(df)
        return df[cond].reset_index(drop=True)


class RawCalibScreener(CompoundScreener):
    """
    A compound screener for raw calibs.
    """

    def __init__(self, config=None, *args, **kwargs):
        config = get_config() if config is None else config
        data_types = config["calibs"]["types"]
        super().__init__(data_types=data_types, config=config, *args, **kwargs)
