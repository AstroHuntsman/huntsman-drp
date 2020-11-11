import numpy as np
import pandas as pd

from huntsman.drp.screener import Screener, RawCalibScreener


def test_screener(raw_data_table, raw_quality_table, config):
    """ Test Screener functionality. """
    # Get some files
    df = raw_data_table.query(criteria={"dataType": "science"})[:3]
    assert df.shape[0] == 3

    # Make up some metadata
    raw_quality_table.insert_one(metadata={"test_metric": 0, "filename": df.iloc[0]["filename"]})
    raw_quality_table.insert_one(metadata={"test_metric": 1, "filename": df.iloc[1]["filename"]})
    raw_quality_table.insert_one(metadata={"test_metric": 2, "filename": df.iloc[2]["filename"]})

    criteria = {"test_metric": {"greater_than_equals": -1, "less_than": 3}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 3

    criteria = {"test_metric": {"greater_than_equals": 1, "less_than": 3}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 2

    criteria = {"test_metric": {"greater_than_equals": 1, "less_than": 2}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 1

    criteria = {"test_metric": {"equals": 1}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 1

    criteria = {"test_metric": {"not_equals": 1}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 2

    criteria = {"test_metric": {"greater_than": 1}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 1

    criteria = {"test_metric": {"less_than": 2}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 2

    criteria = {"test_metric": {"less_than_equals": 2}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 3

    criteria = {"test_metric": {"in": [0, 1]}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 2

    criteria = {"test_metric": {"not_in": [0, 1]}}
    config["screening"]["science"]["criteria"] = criteria
    screener = Screener("science", config=config)
    assert screener.screen_dataframe(df).shape[0] == 1


def test_calib_screener(raw_data_table, raw_quality_table, config):
    """ Test CalibScreener functionality. """
    # Get files
    df_flat = raw_data_table.query(criteria={"dataType": "flat"})[:2]
    df_bias = raw_data_table.query(criteria={"dataType": "bias"})[:2]
    df = pd.concat([df_flat, df_bias], ignore_index=True)

    # Make fake metadata and put into DB
    raw_quality_table.insert_one(metadata={"test_flat_metric": 0,
                                           "filename": df_flat.iloc[0]["filename"]})
    raw_quality_table.insert_one(metadata={"test_flat_metric": 1,
                                           "filename": df_flat.iloc[1]["filename"]})
    raw_quality_table.insert_one(metadata={"test_bias_metric": 0,
                                           "filename": df_bias.iloc[0]["filename"]})
    raw_quality_table.insert_one(metadata={"test_bias_metric": 1,
                                           "filename": df_bias.iloc[1]["filename"]})

    # Specify screen criteria
    config["screening"]["flat"]["criteria"] = {"test_flat_metric": 0}
    config["screening"]["bias"]["criteria"] = {"test_bias_metric": {"not_equals": 0}}

    # Do the screening
    screener = RawCalibScreener(config=config)
    df_screened = screener.screen_dataframe(df)

    # Check results
    assert df_screened.shape[0] == 2
    assert (df_screened["dataType"].values == "bias").sum() == 1
    assert (df_screened["dataType"].values == "flat").sum() == 1
    filenames = [df_flat.iloc[0]["filename"], df_bias.iloc[1]["filename"]]
    assert np.isin(df_screened["filename"].values, filenames).all()
