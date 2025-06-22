"""
Pytest file for python-openobserve with fireducks

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=duplicate-code
import os

# import json
from datetime import datetime, timedelta
from pprint import pprint
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


def test_connection_settings():
    """Ensure have connection settings from environment"""
    assert "OPENOBSERVE_URL" in os.environ
    assert "OPENOBSERVE_USER" in os.environ
    assert "OPENOBSERVE_PASS" in os.environ


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


def test_search1_df():
    """Ensure can do logs search (default stream, dataframe output)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    df_search_results = oo_conn.search2df_fireducks(
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


def test_search1_dftypes():
    """Ensure can do logs search (default stream, dataframe output, timestamp type)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT _timestamp FROM "default" order by _timestamp desc limit 1'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    df_search_results = oo_conn.search2df_fireducks(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
    )
    pprint(df_search_results)
    pprint(df_search_results.dtypes)
    assert not df_search_results.empty
    assert df_search_results.shape
    assert not df_search_results.columns.empty
    assert df_search_results["_timestamp"].dtypes == "int64"

    df_search_results = oo_conn.search2df_fireducks(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
        timestamp_conversion_auto=True,
    )
    pprint(df_search_results)
    pprint(df_search_results.dtypes)
    assert not df_search_results.empty
    assert df_search_results.shape
    assert not df_search_results.columns.empty
    assert df_search_results["_timestamp"].dtypes == "datetime64[ns]"
