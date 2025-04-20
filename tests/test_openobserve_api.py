"""Pytest file for python-openobserve"""

import os
from datetime import datetime, timedelta
from pprint import pprint
import pytest  # type: ignore
import sqlglot  # type: ignore
import jmespath
import requests
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


def test_connection_incorrect_params1():
    """Ensure error if incorrect parameter"""
    oo_conn = OpenObserve(host="invalid", user="***", password="")  # nosec B106
    with pytest.raises(
        requests.exceptions.MissingSchema,
        match="Invalid URL",
    ):
        oo_conn.list_objects("streams")


def test_connection_incorrect_params2():
    """Ensure error if incorrect parameter"""
    oo_conn = OpenObserve(
        host="invalid", user="invalid@example.com", password="", timeout=3  # nosec B106
    )
    with pytest.raises(
        requests.exceptions.MissingSchema,
        match="Invalid URL",
    ):
        oo_conn.list_objects("streams")


def test_connection_incorrect_params3():
    """Ensure error if incorrect parameter"""
    with pytest.raises(
        TypeError,
        match=(
            r"OpenObserve.__init__\(\) missing 2 required positional arguments:"
            " 'user' and 'password'"
        ),
    ):
        # pylint: disable=no-value-for-parameter
        OpenObserve()


def test_connection_incorrect_params4():
    """Ensure error if incorrect parameter"""
    oo_conn = OpenObserve(
        host="https://nonexistent.example.com",
        user="invalid@example.com",
        password="",  # nosec B106
        timeout=3,
    )
    with pytest.raises(
        Exception,
        match="Max retries exceeded with url:",
    ):
        oo_conn.list_objects("streams")


def test_list_object_streams():
    """Ensure can list streams and have 'default' one (list_objects)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_objects("streams")
    # pprint(res)
    default_stream = jmespath.search("list[?name=='default']", res)
    assert default_stream


def test_list_object_streams401():
    """Ensure can list streams and have 'default' one (list_objects)"""
    oo_conn = OpenObserve(
        host=OO_HOST, user="invalid@example.com", password=""
    )  # nosec B106
    with pytest.raises(
        Exception,
        match="Openobserve GET_streams returned 401. Text: Unauthorized Access",
    ):
        oo_conn.list_objects("streams")


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


def test_search1_dftypes():
    """Ensure can do logs search (default stream, dataframe output, timestamp type)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT _timestamp FROM "default" order by _timestamp desc limit 1'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    df_search_results = oo_conn.search2df(
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

    df_search_results = oo_conn.search2df(
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


def test_search_sql_invalid1():
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "SELECT NOT SQL"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(Exception, match="Openobserve search returned 502."):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


def test_search_sql_invalid2():
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "INVALID"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(
        Exception,
        match=(
            "Openobserve search returned 500. Text: "
            '{"code":500,"message":"sql parser error: Expected: an SQL statement, found: INVALID"}'
        ),
    ):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


def test_search_sql_invalid3():
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "NOT SQL"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(
        Exception,
        match=(
            "Openobserve search returned 500. Text: "
            '{"code":500,"message":"sql parser error: Expected: an SQL statement, found: NOT"}'
        ),
    ):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


def test_search_sql_invalid4():
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "123"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(
        Exception,
        match=(
            "Openobserve search returned 500. Text: "
            '{"code":500,"message":"sql parser error: Expected: an SQL statement, found: 123"}'
        ),
    ):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


def test_search_sql_parse_error1():
    """Ensure par error on sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT _timestamp FROM (SELECT _timestamp FROM "default"'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()

    # Warning! "SQLGlot does not aim to be a SQL validator"
    #   https://github.com/tobymao/sqlglot?tab=readme-ov-file#faq
    with pytest.raises(sqlglot.errors.ParseError, match=r"Expecting \)"):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


def test_search_time_invalid1():
    """Ensure error on invalid time input (float)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
    start_timeperiod = 0.1
    end_timeperiod = 0
    with pytest.raises(Exception, match="Search invalid start_time input"):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


def test_search_time_invalid2():
    """Ensure error on invalid time input (bytes)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
    start_timeperiod = b"abc"
    end_timeperiod = 0
    with pytest.raises(Exception, match="Search invalid start_time input"):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


# fixed bug: failed attempt to convert journald field 'body___monotonic_timestamp',
# 'body__runtime_scope', and kunai 'info_utc_time' from __intts2datetime just
# detecting 'time' in key
def test_search_time_conversion1(capsys):
    """Repeat time conversion issue"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "SELECT * FROM \"kunai\" WHERE data_path LIKE '/etc/sudoers.d/%' LIMIT 1"
    start_timeperiod = datetime.now() - timedelta(days=1)
    end_timeperiod = datetime.now()

    with pytest.raises(
        UnboundLocalError,
        match=(
            "cannot access local variable 'timestamp_out' where "
            "it is not associated with a value"
        ),
    ):
        oo_conn.search(
            sql,
            start_time=start_timeperiod,
            end_time=end_timeperiod,
            verbosity=5,
            timestamp_conversion_auto=True,
        )
        captured = capsys.readouterr()
        assert "could not convert timestamp:" in captured.out


def test_search_time_conversion2(capsys):
    """Ensure no time conversion issue if no auto conversion"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "SELECT * FROM \"kunai\" WHERE data_path LIKE '/etc/sudoers.d/%' LIMIT 1"
    start_timeperiod = datetime.now() - timedelta(days=1)
    end_timeperiod = datetime.now()
    oo_conn.search(
        sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=5
    )
    captured = capsys.readouterr()
    assert "could not convert timestamp:" not in captured.out


def test_search_time_conversion3(capsys):
    """Ensure correct explicit time conversion with search2df()"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "SELECT * FROM \"kunai\" WHERE data_path LIKE '/etc/sudoers.d/%' LIMIT 1"
    start_timeperiod = datetime.now() - timedelta(days=1)
    end_timeperiod = datetime.now()
    df_res = oo_conn.search2df(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
        timestamp_columns=["info_utc_time"],
    )
    captured = capsys.readouterr()
    assert "could not convert timestamp:" not in captured.out
    assert df_res["_timestamp"].dtypes == "datetime64[ns]"
    assert df_res["info_utc_time"].dtypes == "datetime64[ns, UTC]"
    # python Objects aka string
    assert df_res["body___monotonic_timestamp"].dtypes == "O"
    # assert df_res["body__runtime_scope"].dtypes == "O"
