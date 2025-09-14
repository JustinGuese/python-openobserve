"""
Pytest file for python-openobserve - memray tests

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=duplicate-code

import os

from datetime import datetime, timedelta
from pprint import pprint
import pytest  # type: ignore
from dotenv import load_dotenv  # type: ignore
from python_openobserve.openobserve import OpenObserve

# os.environ["REQUESTS_CA_BUNDLE"] = (
#     os.environ["HOME"] + "/tmp/ca-bundle.pem"
# )
# OO_HOST = "https://openobserve"
# OO_USER = "root@example.com"
# OO_PASS = ""


load_dotenv()

OO_HOST = OO_USER = OO_PASS = ""  # nosec B106 B105
if "OPENOBSERVE_URL" in os.environ:
    OO_HOST = os.environ["OPENOBSERVE_URL"]
if "OPENOBSERVE_USER" in os.environ:
    OO_USER = os.environ["OPENOBSERVE_USER"]
if "OPENOBSERVE_PASS" in os.environ:
    OO_PASS = os.environ["OPENOBSERVE_PASS"]


@pytest.mark.limit_memory("10 MB")
def test_search1():
    """Ensure can do logs search (default stream)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    search_results = oo_conn.search(
        sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
    )
    pprint(search_results)
    assert search_results


@pytest.mark.limit_memory("10 MB")
def test_search1_df():
    """Ensure can do logs search (default stream, dataframe output)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    df_search_results = oo_conn.search2df(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
    )
    pprint(df_search_results)
    assert not df_search_results.empty
    assert df_search_results.shape
    assert not df_search_results.columns.empty
    assert "log_file_name" in df_search_results.columns
    assert "count(*)" in df_search_results.columns
