import pytest
import logging
from unittest.mock import patch

import pandas as pd

from delphi_usafacts.pull import pull_usafacts_data

from test_run import local_fetch

BASE_URL_GOOD = "test_data/small_{metric}_pull.csv"

BADCASE_PARAMS = {
    "missing_days":
        {"url":"test_data/bad_{metric}_missing_days.csv",
         "missing_dates": ['2020-03-01', '2020-03-04', '2020-03-05', '2020-03-08']
         },
    "missing_cols": "test_data/bad_{metric}_missing_cols.csv",
    "extra_cols": "test_data/bad_{metric}_extra_cols.csv"
}

TEST_LOGGER = logging.getLogger()
TEST_LOGGER.propagate = True

@patch("delphi_usafacts.pull.fetch", local_fetch)
class TestPullUSAFacts:
    def test_good_file(self):
        metric = "deaths"
        df = pull_usafacts_data(BASE_URL_GOOD, metric, TEST_LOGGER)
        expected_df = pd.DataFrame({
            "fips": ["00001", "00001", "00001", "36009", "36009", "36009"],
            "timestamp": [pd.Timestamp("2020-02-29"), pd.Timestamp("2020-03-01"),
                          pd.Timestamp("2020-03-02"), pd.Timestamp("2020-02-29"),
                          pd.Timestamp("2020-03-01"), pd.Timestamp("2020-03-02")],
            "new_counts": [0., 0., 1., 2., 2., 2.],
            "cumulative_counts": [0, 0, 1, 2, 4, 6]},
            index=[1, 2, 3, 5, 6, 7])
        # sort since rows order doesn't matter
        pd.testing.assert_frame_equal(df.sort_index(), expected_df.sort_index())

    def test_missing_days(self, caplog):
        metric = "confirmed"
        pull_usafacts_data(
            BADCASE_PARAMS["missing_days"]["url"], metric, TEST_LOGGER
        )
        assert " ,".join(BADCASE_PARAMS["missing_days"]["missing_dates"]) in caplog.text


    def test_missing_cols(self):

        metric = "confirmed"
        with pytest.raises(ValueError):
            pull_usafacts_data(
                BADCASE_PARAMS["missing_cols"]["url"], metric, TEST_LOGGER
            )

    def test_extra_cols(self):

        metric = "confirmed"
        with pytest.raises(ValueError):
            pull_usafacts_data(
                BADCASE_PARAMS["extra_cols"]["url"], metric, TEST_LOGGER
            )
