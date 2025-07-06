"""
Pytest file for python-openobserve

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

import os

import json
from datetime import datetime, timedelta
from pprint import pprint
import pytest  # type: ignore
import sqlglot  # type: ignore
import jmespath
import requests
from dotenv import load_dotenv  # type: ignore
from python_openobserve.openobserve import OpenObserve, is_ksuid, is_name

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


def test_is_ksuid():
    """Test is_ksuid() function"""

    res = is_ksuid("abc")
    assert not res
    res2 = is_ksuid("ksuid1234567890abcdefghijkl")
    assert res2


def test_is_name():
    """Test is_name() function"""

    res = is_name("Test Alert Name")
    assert not res
    res2 = is_name("Test_Alert_Name")
    assert res2


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


def test_list_object_alerts():
    """Ensure can list alerts and have right fields (list_objects)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_objects("alerts")
    pprint(res)
    owner = jmespath.search("list[?owner=='root@example.com']", res)
    folder = jmespath.search("list[?folder_name=='default']", res)
    # destinations = jmespath.search("list[?destinations]", res)
    assert owner
    assert folder
    # FIXME! underlying API call issue.
    # assert destinations != []


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


def test_config_export_strip(tmpdir):
    """Ensure can do config export to json"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    oo_conn.config_export(f"{tmpdir}/", verbosity=0, split=True, strip=True)
    assert os.path.isdir(f"{tmpdir}/alerts")
    assert os.path.exists(f"{tmpdir}/streams/default.json")
    with open(f"{tmpdir}/streams/default.json", "r", encoding="utf-8") as json_file:
        json_data = json.loads(json_file.read())
        stats = jmespath.search("stats", json_data)
        assert not stats


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


def test_search_df_limit1():
    """
    Ensure can get high volume of logs from search - 10005
    Note: suppose enough data in journald stream to do so
    """
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT _timestamp FROM "journald" LIMIT 10005'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    df_search_results = oo_conn.search2df(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
    )
    assert not df_search_results.empty
    assert df_search_results.shape[0] == 10005
    assert not df_search_results.columns.empty
    assert "_timestamp" in df_search_results.columns


def test_search_df_limit2():
    """
    Ensure can get high volume of logs from search - 100005
    Note: suppose enough data in journald stream to do so
    """
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT _timestamp FROM "journald" LIMIT 100005'
    start_timeperiod = datetime.now() - timedelta(days=10)
    end_timeperiod = datetime.now()
    df_search_results = oo_conn.search2df(
        sql,
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        verbosity=5,
    )
    assert not df_search_results.empty
    assert df_search_results.shape[0] == 100005
    assert not df_search_results.columns.empty
    assert "_timestamp" in df_search_results.columns


def test_delete_object_users1():
    """Ensure can delete users - invalid/non-existent"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    # Exception: Openobserve update_object_users returned 404.
    # Text: {"code":404,"message":"User for the organization not found"}
    with pytest.raises(Exception, match="User for the organization not found"):
        oo_conn.delete_object("users", "pytest-invalid@example.com")


def test_create_object_users(capsys):
    """Ensure can create user"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.create_object(
        "users",
        {
            "email": "pytest@example.com",
            "password": "pytest@example.com",
            "first_name": "pytest",
            "last_name": "",
            "role": "admin",
            "is_external": False,
        },
        verbosity=3,
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "User saved successfully" in captured.out
    assert "Create object users url:" in captured.out


def test_delete_object_users(capsys):
    """Ensure can delete user - after alerts/destination tests"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.delete_object("users", "pytest@example.com", verbosity=3)
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "User removed from organization" in captured.out
    assert "Delete object users url: " in captured.out


def test_import_function1(capsys):
    """Ensure import function works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_function = {
        "function": (
            "if exists(.body) {\n   body_expanded, parse_err = parse_json(.body)\n"
            "   if parse_err == null {\n       ., merge_err = merge(., body_expanded)\n"
            "       ._messagetime = to_unix_timestamp(parse_timestamp!("
            '.time, "%Y-%m-%dT%H:%M:%S+00:00"), unit: "microseconds")\n'
            "       if merge_err == null {\n           del(.body)\n       }\n }\n}\n"
            "if exists(.remote_addr) {\n"
            "    geo_city, geo_err = get_enrichment_table_record("
            '"maxmind_city", {"ip": .remote_addr })\n'
            "    geo_asn, geo_err2 = get_enrichment_table_record("
            '"maxmind_asn", {"ip": .remote_addr })\n'
            "    if geo_err == null {\n        .geo_city = geo_city\n    }\n"
            "    if geo_err2 == null {\n        .geo_asn = geo_asn\n }\n}\n."
        ),
        "name": "pytest_nginx_json_body",
        "params": "row",
        "numArgs": 1,
        "transType": 0,
    }

    oo_conn.import_objects_split(
        "functions",
        json_function,
        "",
        verbosity=5,
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists (Exception/400)
    assert (
        "Return 200. Text: " in captured.out or "returned 400. Text: " in captured.out
    )
    assert "Create returns " in captured.out


def test_delete_object_functions(capsys):
    """Ensure can delete functions"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.delete_object("functions", "pytest_nginx_json_body", verbosity=3)
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "Function deleted" in captured.out
    assert "Delete object functions url: " in captured.out


def test_import_pipeline0(capsys):
    """
    Ensure import pipeline works - empty
    Empty pipeline (nodes/edges) will return error (400)
    """
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_pipeline = {
        "pipeline_id": "7308207793515791234",
        "version": 0,
        "enabled": True,
        "org": "default",
        "name": "pytest_web",
        "description": "",
        "source": {
            "source_type": "realtime",
            "org_id": "default",
            # can't have multiple pipelines on same stream
            "stream_name": "pytestweb",
            "stream_type": "logs",
        },
        "nodes": [],
        "edges": [],
    }

    with pytest.raises(Exception, match="Invalid pipeline Empty pipeline."):
        oo_conn.import_objects_split(
            "pipelines",
            json_pipeline,
            "",
            verbosity=5,
        )
        captured = capsys.readouterr()
        assert "Openobserve returned 404." not in captured.out
        assert "returned 400. Text: " in captured.out
        assert (
            "Invalid pipeline Empty pipeline. Please add Source/Destination node"
            in captured.out
        )
        assert "Create returns False" in captured.out


def test_import_pipeline1(capsys):
    """
    Ensure import pipeline works
    Note: empty pipeline (nodes/edges) will return error (400)
    """
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_pipeline = {
        "pipeline_id": "7308207793515791234",
        "version": 0,
        "enabled": True,
        "org": "default",
        "name": "pytest_web",
        "description": "",
        "source": {
            "source_type": "realtime",
            "org_id": "default",
            # can't have multiple pipelines on same stream
            "stream_name": "pytestweb",
            "stream_type": "logs",
        },
        "nodes": [
            {
                "id": "4efe0a03-0992-489c-b35b-15e3cf85a0fa",
                "data": {
                    "node_type": "stream",
                    "org_id": "default",
                    "stream_name": "web",
                    "stream_type": "logs",
                },
                "position": {"x": 407.66666, "y": 66.333336},
                "io_type": "input",
            },
            {
                "id": "c7325c9c-8859-4f38-9111-4c29145bd3e5",
                "data": {
                    "node_type": "stream",
                    "org_id": "default",
                    "stream_name": "web",
                    "stream_type": "logs",
                },
                "position": {"x": 406.66666, "y": 401.33334},
                "io_type": "output",
            },
            {
                "id": "f0114d07-8d5c-4a68-ae19-bf930659b969",
                "data": {
                    "node_type": "function",
                    "name": "nginx_json_body",
                    "after_flatten": False,
                    "num_args": 0,
                },
                "position": {"x": 373.33334, "y": 298.33334},
                "io_type": "default",
            },
            {
                "id": "640e0ff9-871e-4dd7-8cb0-101549901f5f",
                "data": {
                    "node_type": "condition",
                    "conditions": [
                        {
                            "column": "log_file_path",
                            "operator": "=",
                            "value": "/var/log/nginx/access_json.log",
                            "ignore_case": False,
                        }
                    ],
                },
                "position": {"x": 303.0, "y": 168.66667},
                "io_type": "default",
            },
        ],
        "edges": [
            {
                "id": "e4efe0a03-0992-489c-b35b-15e3cf85a0fa-640e0ff9-871e-4dd7-8cb0-101549901f5f",
                "source": "4efe0a03-0992-489c-b35b-15e3cf85a0fa",
                "target": "640e0ff9-871e-4dd7-8cb0-101549901f5f",
            },
            {
                "id": "e640e0ff9-871e-4dd7-8cb0-101549901f5f-f0114d07-8d5c-4a68-ae19-bf930659b969",
                "source": "640e0ff9-871e-4dd7-8cb0-101549901f5f",
                "target": "f0114d07-8d5c-4a68-ae19-bf930659b969",
            },
            {
                "id": "ef0114d07-8d5c-4a68-ae19-bf930659b969-c7325c9c-8859-4f38-9111-4c29145bd3e5",
                "source": "f0114d07-8d5c-4a68-ae19-bf930659b969",
                "target": "c7325c9c-8859-4f38-9111-4c29145bd3e5",
            },
        ],
    }
    oo_conn.import_objects_split(
        "pipelines",
        json_pipeline,
        "",
        verbosity=5,
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert (
        "Return 200. Text: " in captured.out or "returned 400. Text: " in captured.out
    )
    assert "Create returns " in captured.out
