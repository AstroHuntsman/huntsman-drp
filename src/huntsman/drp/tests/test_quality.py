import time
import pytest

from huntsman.drp import quality
from huntsman.drp.quality.calexp import CalexpQualityMonitor


@pytest.fixture(scope="function")
def metadata_dataframe(exposure_table):
    """ Load a small amount of data to run the tests on. """
    document_filter = dict(dataType="science")
    return exposure_table.get_metrics(document_filter=document_filter)[:2]


def test_metadata_from_fits(metadata_dataframe, config):
    """ Placeholder for a more detailed test. """
    mds = []
    for i in range(metadata_dataframe.shape[0]):
        mds.append(quality.metadata_from_fits(metadata_dataframe.iloc[i], config=config))


def test_calexp_quality_monitor(exposure_table_real_data):
    """ Test that the quality monitor is able to calculate and archive calexp metrics. """
    n_to_process = exposure_table_real_data.count_documents({"dataType": "science"})
    m = CalexpQualityMonitor(exposure_table=exposure_table_real_data, sleep=1)
    m.start()
    i = 0
    try:
        while (i < 120) and (m.status["processed"] != n_to_process) and m.is_running:
            i += 1
            time.sleep(1)
        if i == 60:
            raise RuntimeError(f"Timeout while waiting for processing of {n_to_process} images.")
        if not m.is_running:
            raise RuntimeError("Calexp monitor has stopped running.")
        for md in exposure_table_real_data.find({"dataType": "science"}):
            assert "calexp" in md["quality"].keys()
    finally:
        m.stop()
        assert not m.is_running
