"""Pytest file for python-openobserve"""

# pylint: disable=unused-argument,redefined-outer-name,missing-function-docstring,too-few-public-methods,no-else-return,duplicate-code
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

        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

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
        )

    return MockResponse({"id": 1, "name": "John Doe"}, 200)


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
    assert df_search_results["_timestamp"].dtypes == "datetime64[ns]"


@patch("requests.post", side_effect=mock_post502)
def test_search_sql_invalid1(mock_post502):
    """Ensure error on invalid sql input"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    sql = "SELECT NOT SQL"
    start_timeperiod = datetime.now() - timedelta(days=7)
    end_timeperiod = datetime.now()
    with pytest.raises(Exception, match="Openobserve returned 502."):
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
            "Openobserve returned 500. Text: "
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
        match=("Openobserve returned 500. Text: "),
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
        match=("Openobserve returned 500. Text: "),
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
