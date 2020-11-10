"""
Screeners are objects that facilitate filtering of query results based on metadata. Screeners
are created and configured for specific data types, e.g. raw bias frames.
"""
import numpy as np

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.core import get_config
from huntsman.drp.utils.library import load_module
from huntsman.drp.utils.screening import satisfies_criteria


class Screener(HuntsmanBase):

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
        df_ref = self.dqtable.query_matches(values=df[self._reference_key].values)

        # Apply the configured criteria
        for metric_name, criteria in self.screen_config["criteria"].items():
            self.logger.debug(f"Screening {metric_name} with criteria: {criteria}.")
            metric_data = metric_data = df_ref[metric_name].values

            # Check which rows satisfy criteria
            meets_criteria = satisfies_criteria(metric_data, criteria, logger=self.logger,
                                                metric_name=metric_name)
            self.logger.debug(f"{meets_criteria.sum()} of {meets_criteria.size} rows satisfy"
                              f" {metric_name} criteria.")
            screen_result = np.logical_and(screen_result, meets_criteria)

        return screen_result

    def screen_dataframe(self, df):
        """ Apply screening to DataFrame and return DataFrame with only valid rows. """
        cond = self.screen(df)
        return df[cond].reset_index(drop=True)


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
        for data_type, screener in self.screeners:
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
