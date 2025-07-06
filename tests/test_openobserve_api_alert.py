"""
Pytest file for python-openobserve alerts

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=duplicate-code
import os

# import json
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


def test_connection_settings():
    """Ensure have connection settings from environment"""
    assert "OPENOBSERVE_URL" in os.environ
    assert "OPENOBSERVE_USER" in os.environ
    assert "OPENOBSERVE_PASS" in os.environ


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


def test_import_alert1(capsys):
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


def test_import_alert2(capsys):
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    # if from alert export, Capitalize boolean, remove null entries
    json_alert = {
        "context_attributes": {},
        "description": "Detects the doas tool execution in linux host platform. ...",
        "destinations": ["alert-destination-email"],
        "enabled": True,
        "id": "123huhHK12KnKur8ih1QuiUmABC",
        "is_real_time": False,
        "name": "pytest_Linux_Doas_Tool_Execution",
        "org_id": "default",
        "owner": "root@example.com",
        "query_condition": {
            "type": "sql",
            "conditions": [],
            "sql": 'select count(*) from "default"',
            "multi_time_range": [],
        },
        "row_template": "",
        "stream_name": "kunai",
        "stream_type": "logs",
        "trigger_condition": {
            "cron": "",
            "frequency": 60,
            "frequency_type": "minutes",
            "operator": ">=",
            "period": 60,
            "silence": 240,
            "threshold": 3,
            "timezone": "UTC",
        },
        "tz_offset": 0,
        "last_edited_by": "root@example.com",
    }

    oo_conn.import_objects_split(
        "alerts",
        json_alert,
        "",
        verbosity=5,
    )
    captured = capsys.readouterr()
    assert "Return 200. Text: " in captured.out
    assert "Create returns " in captured.out
    assert "Return 400. Text: " not in captured.out


def test_import_alert3(capsys):
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

    with pytest.raises(Exception, match="Json deserialize error: Failed to decode at"):
        oo_conn.import_objects_split(
            "alerts",
            json_alert,
            "",
            verbosity=5,
            force=True,
        )
        captured = capsys.readouterr()
        assert "Return 400. Text: " in captured.out


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


def test_import_alert5(capsys):
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
        match="Alert name cannot contain ':', '#', '\\?', '&', '%', quotes and space characters",
    ):
        oo_conn.import_objects_split(
            "alerts",
            json_alert,
            "",
            verbosity=5,
            force=True,
        )
        captured = capsys.readouterr()
        assert "Return 400. Text: " in captured.out


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


def test_import_alert7(capsys):
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_alert = {
        # "id": "ksuid1234567890abcdefghijkl",
        "name": "pytest_Test_Alert",
        "alert_condition": "some_condition",
        "destinations": ["alert-destination-email"],
        "threshold": 100,
    }

    # Abnormal error from upstream as field is present
    with pytest.raises(
        Exception,
        match="Alert name is required",
    ):
        oo_conn.import_objects_split(
            "alerts",
            json_alert,
            "",
            verbosity=5,
        )
        captured = capsys.readouterr()
        assert "Return 400. Text: " in captured.out


def test_import_alert8(capsys):
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    json_alert = {
        # "id": "ksuid1234567890abcdefghijkl",  # not required
        "name": "pytest_Test_Alert",
        "alert_condition": "some_condition",
        "destinations": ["alert-destination-email"],
        "threshold": 100,
        "stream_name": "default",  # required
        "enabled": False,
    }

    oo_conn.import_objects_split(
        "alerts",
        json_alert,
        "",
        verbosity=5,
    )
    captured = capsys.readouterr()
    assert "Return 200. Text: " in captured.out
    assert "Create returns " in captured.out
    assert "Return 400. Text: " not in captured.out


# def test_delete_object_alert2(capsys):
#     """Ensure can delete alert"""
#     oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
#
#     oo_conn.delete_object("alerts", "pytest Linux Doas Tool Execution", verbosity=3)
#     captured = capsys.readouterr()
#     assert "Openobserve returned 404." not in captured.out
#     # Can create successfully or not if already exists
#     assert "Return 200. Text: " in captured.out
#     assert "Alert deleted" in captured.out
#     assert "Delete object alerts url: " in captured.out


def test_delete_object_alert2_by_name1(capsys):
    """Ensure can delete alert by name"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.delete_object_by_name("alerts", "pytest_alert", verbosity=5)
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "Alert deleted" in captured.out
    assert "Delete object alerts url: " in captured.out
    assert "Delete by name deleted 1 object(s)." in captured.out


def test_delete_object_alert2_by_name2(capsys):
    """Ensure can delete alert by name"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.delete_object_by_name(
        "alerts", "pytest_Linux_Doas_Tool_Execution", verbosity=5
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "Alert deleted" in captured.out
    assert "Delete object alerts url: " in captured.out
    assert "Delete by name deleted 1 object(s)." in captured.out


def test_delete_object_alert2_by_name3(capsys):
    """Ensure can delete alert by name"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.delete_object_by_name("alerts", "pytest_Test_Alert", verbosity=5)
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "Alert deleted" in captured.out
    assert "Delete object alerts url: " in captured.out
    assert "Delete by name deleted 1 object(s)." in captured.out


def test_create_update_alert(capsys):
    """Ensure create_update_alert alert works
    Add twice and ensure only one alert.
    """
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    # if from alert export, Capitalize boolean, remove null entries
    json_alert = {
        "id": "2u5huhHK59KnKur8ih1QuiUmABC",
        "name": "pytest_create_update_alert",
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
    json_alert2 = {
        "name": "pytest_create_update_alert",
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
        "description": "Description test alert 2",
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

    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    assert "Return 200. Text: " in captured.out
    assert "Create returns " in captured.out

    oo_conn.create_update_object_by_name(
        "alerts", json_alert2, verbosity=5, overwrite=True
    )
    # pylint: disable=unused-variable
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    assert "Return 200. Text: " in captured.out
    assert "Alert Updated" in captured.out

    oo_conn.delete_object_by_name("alerts", "pytest_create_update_alert", verbosity=5)
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    assert "Return 200. Text: " in captured.out
    assert "Alert deleted" in captured.out
    assert "Delete object alerts url: " in captured.out
    assert "Delete by name deleted 1 object(s)." in captured.out


# if no matching user:
# Openobserve returned 400. Text:
# {"code":400,"message":"Email destination recipients must be part of this org"}
def test_import_alert_destination(capsys):
    """Ensure import alert works"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    # if from alert export, Capitalize boolean, remove null entries
    json_alert_dest = {
        "name": "pytest-alert-destination-email",
        "url": "",
        "method": "post",
        "skip_tls_verify": False,
        "template": "alert-email-o2",
        "emails": ["pytest@example.com"],
        "type": "email",
    }

    oo_conn.import_objects_split(
        "alerts/destinations",
        json_alert_dest,
        "",
        verbosity=5,
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    assert "Return 200. Text: " in captured.out
    assert "Create returns " in captured.out


def test_delete_object_alert_destination(capsys):
    """Ensure can delete alert/destinations"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)

    oo_conn.delete_object(
        "alerts/destinations", "pytest-alert-destination-email", verbosity=3
    )
    captured = capsys.readouterr()
    assert "Openobserve returned 404." not in captured.out
    # Can create successfully or not if already exists
    assert "Return 200. Text: " in captured.out
    assert "Alert destination deleted" in captured.out
    assert "Delete object alerts/destinations url: " in captured.out


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
