import pytest

from huntsman.drp import quality
from huntsman.drp.datatable import DataQualityTable


@pytest.fixture(scope="module")
def filename_list(raw_data_table):
    """
    Load a small amount of data to run the tests on.
    """
    return raw_data_table.query_column("filename", dataType="science")[:2]


@pytest.fixture(scope="module")
def data_quality_table(config):
    return DataQualityTable(config=config)


def test_get_metadata(filename_list, config):
    """
    Placeholder for a more detailed test.
    """
    mds = []
    for filename in filename_list:
        mds.append(quality.get_metadata(filename, config=config))


def test_data_quality_table(filename_list, config, data_quality_table):
    """
    """
    metadata = {}
    for filename in filename_list:
        metadata[filename] = quality.get_metadata(filename, config=config)
        data_quality_table.insert_one(data_id=dict(filename=filename), metadata=metadata[filename])
    query = data_quality_table.query()
    for md in query:
        filename = md["filename"]
        assert len(metadata[filename]) == len(md) - 1  # No _id column
        for key, value in metadata[filename].items():
            assert md[key] == value
