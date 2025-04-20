"""Pytest file for python-openobserve"""

import os
from datetime import datetime, timedelta
from pprint import pprint
import jmespath
from dotenv import load_dotenv
from python_openobserve.openobserve import OpenObserve

# os.environ["REQUESTS_CA_BUNDLE"] = (
#     os.environ["HOME"] + "/tmp/ca-bundle.pem"
# )
# OO_HOST = "https://openobserve"
# OO_USER = "root@example.com"
# OO_PASS = ""


load_dotenv()

OO_HOST = OO_USER = OO_PASS = ""
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


def test_list_streams():
    """Ensure can list streams and have 'default' one (list_streams)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_streams()
    # pprint(res)
    default_stream = jmespath.search("list[?name=='default']", res)
    assert default_stream


def test_list_object_streams():
    """Ensure can list streams and have 'default' one (list_objects)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_objects("streams")
    # pprint(res)
    default_stream = jmespath.search("list[?name=='default']", res)
    assert default_stream


def test_list_object_users():
    """Ensure can list users and have 'root@example.com' one (list_objects)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_objects("users")
    pprint(res)
    user = jmespath.search("data[?email=='root@example.com']", res)
    assert user


def test_config_export(tmpdir):
    """Ensure can do config export to json"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    oo_conn.config_export(f"{tmpdir}/", verbosity=0, split=True)
    assert os.path.isdir(f"{tmpdir}/alerts")
    assert os.path.isdir(f"{tmpdir}/dashboards")
    assert os.path.isdir(f"{tmpdir}/functions")
    assert os.path.isdir(f"{tmpdir}/pipelines")
    assert os.path.isdir(f"{tmpdir}/streams")
    assert os.path.exists(f"{tmpdir}/streams/default.json")
    assert os.path.exists(f"{tmpdir}/streams/journald.json")
    assert os.path.isdir(f"{tmpdir}/users")
    assert os.path.exists(f"{tmpdir}/users/root@example.com.json")


def test_config_export_csv(tmpdir):
    """Ensure can do config export to csv"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    oo_conn.config_export(f"{tmpdir}/", verbosity=0, outformat="csv")
    assert os.path.exists(f"{tmpdir}/alerts.csv")
    assert os.path.exists(f"{tmpdir}/dashboards.csv")
    assert os.path.exists(f"{tmpdir}/functions.csv")
    assert os.path.exists(f"{tmpdir}/pipelines.csv")
    assert os.path.exists(f"{tmpdir}/streams.csv")
    assert os.path.exists(f"{tmpdir}/users.csv")


def test_config_export_xlsx(tmpdir):
    """Ensure can do config export to xlsx"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    oo_conn.config_export(f"{tmpdir}/", verbosity=0, outformat="xlsx")
    assert os.path.exists(f"{tmpdir}/alerts.xlsx")
    assert os.path.exists(f"{tmpdir}/dashboards.xlsx")
    assert os.path.exists(f"{tmpdir}/functions.xlsx")
    assert os.path.exists(f"{tmpdir}/pipelines.xlsx")
    assert os.path.exists(f"{tmpdir}/streams.xlsx")
    assert os.path.exists(f"{tmpdir}/users.xlsx")


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
    df_search_results = oo_conn.search(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
        outformat="df",
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
    df_search_results = oo_conn.search(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
        outformat="df",
    )
    pprint(df_search_results)
    pprint(df_search_results.dtypes)
    assert not df_search_results.empty
    assert df_search_results.shape
    assert not df_search_results.columns.empty
    assert df_search_results["_timestamp"].dtypes == "datetime64[ns]"
