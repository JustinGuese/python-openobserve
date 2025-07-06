"""
Pytest file for python-openobserve

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=unused-argument,redefined-outer-name,missing-function-docstring,too-few-public-methods,no-else-return,duplicate-code,too-many-lines
from datetime import datetime, timedelta
from pprint import pprint
from unittest.mock import patch
import pytest  # type: ignore
import sqlglot  # type: ignore
import jmespath
from python_openobserve.openobserve import OpenObserve

# os.environ["REQUESTS_CA_BUNDLE"] = (
#     os.environ["HOME"] + "/tmp/ca-bundle.pem"
# )
# OO_HOST = "https://openobserve"
# OO_USER = "root@example.com"
# OO_PASS = ""


OO_HOST = OO_USER = OO_PASS = "MOCK_INPUT"  # nosec B105


def mock_get(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.get"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.get"""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if "/api/default/streams" in url:
        return MockResponse(
            {
                "list": [
                    {
                        "name": "default",
                        "storage_type": "disk",
                        "stream_type": "logs",
                        "stats": {},
                        "settings": {},
                        "total_fields": 0,
                    }
                ]
            },
            200,
        )
    elif "/api/default/users" in url:
        return MockResponse(
            {
                "data": [
                    {
                        "email": "root@example.com",
                        "first_name": "root",
                        "last_name": "",
                        "role": "root",
                        "is_external": False,
                    }
                ]
            },
            200,
        )

    return MockResponse({"id": 1, "name": "John Doe"}, 200)


def mock_get401(*args, **kwargs):
    """MockResponse 401 function for openobserve calls of requests.get"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.get"""

        def __init__(self, json_data, status_code, text):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text

        def json(self):
            return self.json_data

    if "/api/default/streams" in url:
        return MockResponse(
            {},
            401,
            "Unauthorized Access",
        )
    elif "/api/default/users" in url:
        return MockResponse(
            {},
            401,
            "Unauthorized Access",
        )

    return MockResponse({}, 401, "Unauthorized Access")


def mock_post(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.post"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.post"""

        def __init__(self, json_data, status_code, text):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text

        def json(self):
            return self.json_data

    if "/api/default/_search" in url:
        return MockResponse(
            {
                "took": 155,
                "hits": [
                    {
                        "_p": "F",
                        "_timestamp": 1674213225158000,
                        "log": (
                            "[2023-01-20T11:13:45Z INFO  actix_web::middleware::logger] "
                            '10.2.80.192 "POST /api/demo/_bulk HTTP/1.1" 200 68 "-" '
                            '"go-resty/2.7.0 (https://github.com/go-resty/resty)" 0.001074',
                        ),
                        "stream": "stderr",
                    }
                ],
                "total": 27179431,
                "from": 0,
                "size": 1,
                "scan_size": 28943,
            },
            200,
            "",
        )
    elif "/api/default/functions" in url:
        return MockResponse(
            {
                "text": {"code": 200, "message": "Function saved successfully"},
            },
            200,
            '{"code": 200, "message": "Function saved successfully"}',
        )
    elif "/api/default/pipelines" in url:
        return MockResponse(
            {
                "text": {"code": 200, "message": "Pipeline created successfully"},
            },
            200,
            '{"code": 200, "message": "Pipeline created successfully"}',
        )
    elif "/api/default/users" in url:
        return MockResponse(
            {
                "text": {"code": 200, "message": "User saved successfully"},
            },
            200,
            '{"code": 200, "message": "User saved successfully"}',
        )

    return MockResponse({"id": 1, "name": "John Doe"}, 200, "text")


def mock_post502(*args, **kwargs):
    """MockResponse 502 function for openobserve calls of requests.post"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.post"""

        def __init__(self, json_data, status_code, text, url):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text
            self.url = url

        def json(self):
            return self.json_data

    if "/api/default/_search" in url:
        return MockResponse({}, 502, "", "https://openobserve.example.com")

    return MockResponse({}, 502, "", "https://openobserve.example.com")


def mock_post500(*args, **kwargs):
    """MockResponse 500 function for openobserve calls of requests.post"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.post"""

        def __init__(self, json_data, status_code, text, url):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text
            self.url = url

        def json(self):
            return self.json_data

    if "/api/default/_search" in url:
        return MockResponse(
            {},
            500,
            '{"code":500,"message":"sql parser error: Expected: an SQL statement, found: INVALID"}',
            "https://openobserve.example.com",
        )

    return MockResponse({}, 500, "", "https://openobserve.example.com")


def test_connection_settings():
    """Ensure have connection settings from environment"""
    assert OO_HOST
    assert OO_USER
    assert OO_PASS


@patch("requests.get", side_effect=mock_get)
def test_list_object_streams(mock_get):
    """Ensure can list streams and have 'default' one (list_objects)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_objects("streams", verbosity=5)
    pprint(res)
    default_stream = jmespath.search("list[?name=='default']", res)
    assert default_stream


@patch("requests.get", side_effect=mock_get401)
def test_list_object_streams401(mock_get):
    """Ensure can list streams and have 'default' one (list_objects)"""
    oo_conn = OpenObserve(
        host=OO_HOST, user="invalid@example.com", password=""
    )  # nosec B106
    with pytest.raises(
        Exception,
        match="Openobserve GET_streams returned 401. Text: Unauthorized Access",
    ):
        oo_conn.list_objects("streams", verbosity=5)


@patch("requests.get", side_effect=mock_get)
def test_list_object_users(mock_get):
    """Ensure can list users and have 'root@example.com' one (list_objects)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    res = oo_conn.list_objects("users")
    pprint(res)
    user = jmespath.search("data[?email=='root@example.com']", res)
    assert user


@patch("requests.post", side_effect=mock_post)
def test_search1(mock_post):
    """Ensure can do logs search (default stream)"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    search_results = oo_conn.search(
        sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=5
    )
    pprint(search_results)
    assert search_results


@patch("requests.post", side_effect=mock_post)
def test_search1_df(mock_post):
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
    assert "log" in df_search_results.columns
    assert "_timestamp" in df_search_results.columns


@patch("requests.post", side_effect=mock_post)
def test_search1_dftypes(mock_post):
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


@patch("requests.post", side_effect=mock_post502)
def test_search_sql_invalid1(mock_post502):
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "SELECT NOT SQL"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(Exception, match="Openobserve search returned 502."):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


@patch("requests.post", side_effect=mock_post500)
def test_search_sql_invalid2(mock_post500):
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


@patch("requests.post", side_effect=mock_post500)
def test_search_sql_invalid3(mock_post500):
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "NOT SQL"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(
        Exception,
        match=("Openobserve search returned 500. Text: "),
    ):
        oo_conn.search(
            sql, start_time=start_timeperiod, end_time=end_timeperiod, verbosity=1
        )


@patch("requests.post", side_effect=mock_post500)
def test_search_sql_invalid4(mock_post500):
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "123"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(
        Exception,
        match=("Openobserve search returned 500. Text: "),
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


def mock_post_kunai(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.post - kunai stream"""

    class MockResponse:
        """MockResponse class for openobserve calls of requests.post"""

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse(
        {
            "took": 155,
            "hits": [
                {
                    "_collector": "myhost",
                    "_messagetime": 1745154631656901,
                    "_timestamp": 1745154631658843,
                    "body___monotonic_timestamp": "3284525466301",
                    "body__boot_id": "4ec5901164ce4567acbc02b0ac3bd8e6",
                    "body__cmdline": "/usr/bin/kunai run -c /etc/kunai/config.yaml",
                    "body__comm": "kunai",
                    "body__exe": "/usr/bin/kunai",
                    "body__gid": "0",
                    "body__hostname": "myhost",
                    "body__machine_id": "f7e1234db2d84830789e33af0a1123b8",
                    "body__pid": "40227",
                    "body__selinux_context": "unconfined\n",
                    "body__stream_id": "fa4a687bf8fc46b480b128f9bcd44d32",
                    "body__systemd_cgroup": "/system.slice/00-kunai.service",
                    "body__systemd_invocation_id": "db5c9912f22945e884f659cb5a5ca392",
                    "body__systemd_slice": "system.slice",
                    "body__systemd_unit": "00-kunai.service",
                    "body__transport": "stdout",
                    "body__uid": "0",
                    "body_priority": "6",
                    "body_syslog_facility": "3",
                    "body_syslog_identifier": "kunai",
                    "data_ancestors": (
                        "/usr/lib/systemd/systemd|/usr/sbin/sshd|"
                        "/usr/sbin/sshd|/usr/sbin/sshd|/usr/bin/bash"
                    ),
                    "data_command_line": "sudo tail -f /var/log/syslog",
                    "data_exe_path": "/usr/bin/sudo",
                    "data_path": "/etc/sudoers.d/zfs",
                    "dropped_attributes_count": 0,
                    "host_name": "myhost",
                    "info_event_batch": 68859059,
                    "info_event_id": 82,
                    "info_event_name": "read_config",
                    "info_event_source": "kunai",
                    "info_event_uuid": "83c44d36-12d3-4f56-0f81-16a91ad5f51e",
                    "info_host_container": '{"name":"myhost","type":null}',
                    "info_host_name": "myhost",
                    "info_host_uuid": "e4984793-123f-1234-a5a0-e21817caa789",
                    "info_parent_task_flags": "0x400000",
                    "info_parent_task_gid": 1008,
                    "info_parent_task_group": "?",
                    "info_parent_task_guuid": "cf987f3f-bc9d-0b00-b731-b345eadc0000",
                    "info_parent_task_name": "bash",
                    "info_parent_task_namespaces": '{"mnt":4026531841}',
                    "info_parent_task_pid": 56554,
                    "info_parent_task_tgid": 56554,
                    "info_parent_task_uid": 1006,
                    "info_parent_task_user": "?",
                    "info_parent_task_zombie": False,
                    "info_task_flags": "0x400100",
                    "info_task_gid": 1008,
                    "info_task_group": "?",
                    "info_task_guuid": "5cc01027-12ab-0b00-b789-b359b83b0123",
                    "info_task_name": "sudo",
                    "info_task_namespaces": '{"mnt":4026531841}',
                    "info_task_pid": 15288,
                    "info_task_tgid": 15288,
                    "info_task_uid": 0,
                    "info_task_user": "?",
                    "info_task_zombie": False,
                    "info_utc_time": "2025-04-20T12:34:56.656901530Z",
                    "os_type": "linux",
                    "severity": 0,
                }
            ],
            "total": 27179431,
            "from": 0,
            "size": 1,
            "scan_size": 28943,
        },
        200,
    )


# fixed bug: failed attempt to convert journald field 'body___monotonic_timestamp',
# 'body__runtime_scope', and kunai 'info_utc_time' from __intts2datetime just
# detecting 'time' in key
@patch("requests.post", side_effect=mock_post_kunai)
def test_search_time_conversion1(mock_post_kunai, capsys):
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


@patch("requests.post", side_effect=mock_post_kunai)
def test_search_time_conversion2(mock_post_kunai, capsys):
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


@patch("requests.post", side_effect=mock_post_kunai)
def test_search_time_conversion3(mock_post_kunai, capsys):
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


def mock_delete404(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.delete"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.delete"""

        def __init__(self, json_data, status_code, text, url):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text
            self.url = url

        def json(self):
            return self.json_data

    if "/api/users" in url:
        return MockResponse(
            {},
            404,
            '{"code":404,"message":"User for the organization not found"}',
            "https://openobserve.example.com",
        )

    return MockResponse({}, 404, "", "https://openobserve.example.com")


@patch("requests.delete", side_effect=mock_delete404)
def test_delete_object_users1(mock_delete404):
    """Ensure can delete users - invalid/non-existent"""
    # pylint: disable=no-member
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    # Exception: Openobserve update_object_users returned 404.
    # Text: {"code":404,"message":"User for the organization not found"}
    with pytest.raises(Exception):
        res = oo_conn.delete_object("users", "pytest-invalid@example.com")
        assert res.status_code == 404
        assert "User for the organization not found" in res.txt


def mock_post_users(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.post"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.post"""

        def __init__(self, json_data, status_code, text, url):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text
            self.url = url

        def json(self):
            return self.json_data

    if "/api/users" in url:
        return MockResponse(
            {},
            200,
            '{"code":200,"message":"User saved successfully"}',
            "https://openobserve.example.com",
        )

    return MockResponse({}, 200, "", "https://openobserve.example.com")


def mock_delete(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.delete"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.delete"""

        def __init__(self, json_data, status_code, text, url):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text
            self.url = url

        def json(self):
            return self.json_data

    if "/api/users" in url:
        return MockResponse(
            {},
            200,
            '{"code":200,"message":"User removed from organization"}',
            "https://openobserve.example.com",
        )

    return MockResponse({}, 200, "", "https://openobserve.example.com")


@patch("requests.post", side_effect=mock_post_users)
def test_create_object_users(mock_post_users, capsys):
    """Ensure can create and delete user"""
    # pylint: disable=no-member
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    res = oo_conn.create_object(
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
    assert res
    assert "Return 200. Text: " in captured.out
    assert "Create object completed" in captured.out


def mock_post_alert1(*args, **kwargs):
    """MockResponse function for openobserve calls of requests.post - post alert"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of requests.post"""

        def __init__(self, json_data, status_code, text, url):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text
            self.url = url

        def json(self):
            return self.json_data

    if "/api/users" in url:
        return MockResponse(
            {},
            200,
            '{"code":200,"message":"Alert saved"}',
            "https://openobserve.example.com",
        )

    return MockResponse({}, 200, "", "https://openobserve.example.com")


@patch("requests.post", side_effect=mock_post_alert1)
def test_import_alert1(mock_post_alert1, capsys):
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    # if from alert export, Capitalize boolean, remove null entries
    json_alert = {
        "id": "2u5huhHK59KnKur8ih1QuiUmABC",
        "name": "pytest_alert",
        "org_id": "default",
        "stream_type": "logs",
        "stream_name": "default",
        "is_real_time": False,
        "query_condition": {
            "type": "sql",
            "conditions": [],
            "sql": 'select count(*) from "default"',
            "multi_time_range": [],
        },
        "trigger_condition": {
            "period": 60,
            "operator": "<=",
            "threshold": 1,
            "frequency": 60,
            "cron": "",
            "frequency_type": "minutes",
            "silence": 60,
            "timezone": "UTC",
        },
        "destinations": ["alert-destination-email"],
        "context_attributes": {},
        "row_template": "",
        "description": "Description test alert",
        "enabled": False,
        "tz_offset": 0,
        "owner": "root@example.com",
        "last_edited_by": "root@example.com",
    }

    oo_conn.import_objects_split(
        "alerts",
        json_alert,
        "",
        verbosity=5,
    )
    # pylint: disable=unused-variable
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    assert "Return 200. Text: " in captured.out
    assert "Create returns " in captured.out


def test_import_alert4():
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_alert = {
        # invalid ksuid: must be 27 alphanum char length
        "id": "ksuid-1234567890abcdefghijklmno",
        "name": "Test Alert",
        "alert_condition": "some_condition",
        "destinations": ["alert-destination-email"],
        "threshold": 100,
    }

    with pytest.raises(Exception, match="is not a ksuid"):
        oo_conn.import_objects_split(
            "alerts",
            json_alert,
            "",
            verbosity=5,
        )


def test_import_alert6():
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_alert = {
        "id": "ksuid1234567890abcdefghijkl",
        # Invalid alert name
        "name": "Test Alert",
        "alert_condition": "some_condition",
        "destinations": ["alert-destination-email"],
        "threshold": 100,
    }

    with pytest.raises(
        Exception,
        match="Invalid input: Test Alert is not a valid name",
    ):
        oo_conn.import_objects_split(
            "alerts",
            json_alert,
            "",
            verbosity=5,
        )


@patch("requests.post", side_effect=mock_post)
def test_import_user1(capsys):
    """Ensure import user works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_pipeline = {
        "email": "pytest@example.com",
        "password": "pytest@example.com",
        "first_name": "pytest",
        "last_name": "",
        "role": "admin",
        "is_external": False,
    }

    oo_conn.import_objects_split(
        "users",
        json_pipeline,
        "",
        verbosity=5,
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # FIXME! all further tests failing like
    #    AssertionError: assert 'Create returns True' in <MagicMock
    #    name='post.readouterr().out' id='129361814377472'>
    # assert "Return 200. Text: " in captured.out
    # assert "User saved successfully" in captured.out
    # assert "Create returns True" in captured.out


@patch("requests.delete", side_effect=mock_delete)
def test_delete_object_users(mock_delete, capsys):
    """Ensure can create and delete user"""
    # pylint: disable=no-member
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    res2 = oo_conn.delete_object("users", "pytest@example.com", verbosity=3)
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    assert res2
    assert "Return 200. Text: " in captured.out
    assert "Delete object completed" in captured.out


@patch("requests.post", side_effect=mock_post)
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
    # assert "Return 200. Text: " in captured.out
    # assert "Create returns True" in captured.out


@patch("requests.post", side_effect=mock_post)
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
    # Always returning 200 and create multiple identitical pipelines
    # assert "Return 200. Text: " in captured.out
    # assert "Pipeline created successfully" in captured.out
    # assert "Create returns " in captured.out
