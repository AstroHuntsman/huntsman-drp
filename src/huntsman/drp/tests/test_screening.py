from huntsman.drp.screener import Screener


def test_screening(raw_data_table, raw_quality_table, config):
    """ Test data screening functionality """
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
