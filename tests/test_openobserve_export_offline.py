"""
Pytest file for python-openobserve - export offline

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=unused-argument,redefined-outer-name,missing-function-docstring,too-few-public-methods,no-else-return,duplicate-code,too-many-lines,line-too-long

import os
import json
from datetime import datetime, timedelta
from unittest.mock import patch

# import pytest  # type: ignore
import pandas
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


def test_connection_settings():
    """Ensure have connection settings from environment"""
    assert "OPENOBSERVE_URL" in os.environ
    assert "OPENOBSERVE_USER" in os.environ
    assert "OPENOBSERVE_PASS" in os.environ


def mock_post(*args, **kwargs):
    """MockResponse function for openobserve calls of httpx.post"""
    url = args[0]

    class MockResponse:
        """MockResponse class for openobserve calls of httpx.post"""

        def __init__(self, json_data, status_code, text):
            self.json_data = json_data
            self.status_code = status_code
            self.text = text

        def json(self):
            return self.json_data

    if "/api/default/_search" in url:
        return MockResponse(
            {
                "took": 5,
                "hits": [
                    {
                        "_timestamp": 1789242312136715,
                        "body___cursor": "s=24ce90cbd37843c698fa6841ab2934b1;i=8338750;b=0031c4e9e88e40cf81ffcb7a1aaf6cc9;m=151011d7b3f;t=65b4e706e980b;x=3c7ef7995c81659b",
                        "body___monotonic_timestamp": "1447422688063",
                        "body__boot_id": "1031c4e9e12e40cf34ffcb7a1abf6cd9",
                        "body__cap_effective": "1ffffffffff",
                        "body__cmdline": '"sshd: [accepted]"',
                        "body__comm": "sshd",
                        "body__exe": "/usr/sbin/sshd",
                        "body__gid": "0",
                        "body__hostname": "hostname1.internal",
                        "body__machine_id": "6012dc054ae1434aa163c410d56457db",
                        "body__pid": "1812",
                        "body__runtime_scope": "system",
                        "body__source_realtime_timestamp": "1789242312136448",
                        "body__systemd_cgroup": "/system.slice/ssh.service",
                        "body__systemd_invocation_id": "49e3f46b72ef43f99b546294f1e69215",
                        "body__systemd_slice": "system.slice",
                        "body__systemd_unit": "ssh.service",
                        "body__transport": "syslog",
                        "body__uid": "0",
                        "body_message": "error: kex_exchange_identification: Connection closed by remote host",
                        "body_priority": "3",
                        "body_syslog_facility": "4",
                        "body_syslog_identifier": "sshd",
                        "body_syslog_pid": "1812",
                        "body_syslog_timestamp": "Sep 12 19:45:12 ",
                        "dropped_attributes_count": 0,
                        "host_name": "hostname1.internal",
                        "os_type": "linux",
                        "severity": 0,
                    },
                    {
                        "_timestamp": 1789242312032541,
                        "body___cursor": "s=24ce90cbd37843c698fa6841ab2934b1;i=833874f;b=0031c4e9e88e40cf81ffcb7a1aaf6cc9;m=151011be451;t=65b4e706d011d;x=5b80b27ea10988cc",
                        "body___monotonic_timestamp": "1447422583889",
                        "body__boot_id": "0031c4e9e88e40cf81ffcb7a1aaf6cc9",
                        "body__cap_effective": "1ffffffffff",
                        "body__cmdline": '"sshd: [accepted]"',
                        "body__comm": "sshd",
                        "body__exe": "/usr/sbin/sshd",
                        "body__gid": "0",
                        "body__hostname": "hostname1.internal",
                        "body__machine_id": "6012dc054ae1434aa163c410d56457db",
                        "body__pid": "1812",
                        "body__runtime_scope": "system",
                        "body__source_realtime_timestamp": "1789242312032494",
                        "body__systemd_cgroup": "/system.slice/ssh.service",
                        "body__systemd_invocation_id": "49e3f46b12ef43f89b546234f1e62215",
                        "body__systemd_slice": "system.slice",
                        "body__systemd_unit": "ssh.service",
                        "body__transport": "syslog",
                        "body__uid": "0",
                        "body_message": 'Connection from 10.1.2.3 port 49802 on 192.168.10.20 port 22 rdomain ""',
                        "body_priority": "6",
                        "body_syslog_facility": "4",
                        "body_syslog_identifier": "sshd",
                        "body_syslog_pid": "1812",
                        "body_syslog_timestamp": "Sep 12 19:45:12 ",
                        "dropped_attributes_count": 0,
                        "host_name": "hostname1.internal",
                        "os_type": "linux",
                        "severity": 0,
                    },
                    {
                        "_timestamp": 1789242305971198,
                        "body___cursor": "s=4d5707818a2740efafe5856b076c7f37;i=a65d72f;b=1b91ce75325949928b621acd243ab666;m=33dc9d92c44;t=65b4e701083fe;x=7a502866357e098",
                        "body___monotonic_timestamp": "3563914341444",
                        "body__audit_loginuid": "995",
                        "body__audit_session": "33302",
                        "body__boot_id": "1b91ce75325949928b621acd243ab666",
                        "body__cap_effective": "1ffffffffff",
                        "body__cmdline": "/usr/sbin/CRON -f -P",
                        "body__comm": "cron",
                        "body__exe": "/usr/sbin/cron",
                        "body__gid": "0",
                        "body__hostname": "opcentral.internal",
                        "body__machine_id": "f7e6787db2d84830854e33af0a1338b8",
                        "body__pid": "59927",
                        "body__selinux_context": "unconfined\n",
                        "body__source_realtime_timestamp": "1789242305971181",
                        "body__systemd_cgroup": "/system.slice/cron.service",
                        "body__systemd_invocation_id": "9a1e3466302f4197b57188be087e3a7e",
                        "body__systemd_slice": "system.slice",
                        "body__systemd_unit": "cron.service",
                        "body__transport": "syslog",
                        "body__uid": "0",
                        "body_message": "pam_unix(cron:session): session closed for user _zeek",
                        "body_priority": "6",
                        "body_syslog_facility": "10",
                        "body_syslog_identifier": "CRON",
                        "body_syslog_pid": "59927",
                        "body_syslog_timestamp": "Sep 12 19:45:05 ",
                        "dropped_attributes_count": 0,
                        "host_name": "opcentral.internal",
                        "os_type": "linux",
                        "severity": 0,
                    },
                ],
                "total": 5,
                "from": 0,
                "size": 10,
                "scan_size": 10,
            },
            200,
            "",
        )


@patch("httpx.post", side_effect=mock_post)
def test_logs_export_json(mock_post, tmpdir):
    """Ensure can do logs export"""
    duration = 2
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    start_timeperiod = datetime.now() - timedelta(hours=duration)
    end_timeperiod = datetime.now()
    oo_conn.search2export(
        'SELECT * from "journald"',
        f"{tmpdir}/pytest",
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        export_format="json",
        split_period="H",
        verbosity=0,
    )
    ls = os.listdir(f"{tmpdir}")
    print(f"ls: {ls}")
    assert len(ls) == duration
    assert all("-export.json" in x for x in ls)
    assert "pytest--" in ls[0]
    assert os.path.exists(os.path.join(tmpdir, ls[0]))
    with open(os.path.join(tmpdir, ls[0]), "r", encoding="utf-8") as file:
        data = json.load(file)
        assert "_timestamp" in data[0].keys()
        assert "body__hostname" in data[0].keys()
        assert "body_message" in data[0].keys()
        assert "body__cap_effective" in data[0].keys()


@patch("httpx.post", side_effect=mock_post)
def test_logs_export_csv(mock_post, tmpdir):
    """Ensure can do logs export"""
    duration = 2
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    start_timeperiod = datetime.now() - timedelta(hours=duration)
    end_timeperiod = datetime.now()
    oo_conn.search2export(
        'SELECT * from "journald"',
        f"{tmpdir}/pytest",
        start_time=start_timeperiod,
        end_time=end_timeperiod,
        export_format="csv",
        split_period="H",
        verbosity=0,
    )
    ls = os.listdir(f"{tmpdir}")
    print(f"ls: {ls}")
    assert len(ls) == duration
    assert all("-export.csv" in x for x in ls)
    assert os.path.exists(os.path.join(tmpdir, ls[0]))
    df = pandas.read_csv(os.path.join(tmpdir, ls[0]))
    assert "_timestamp" in df.columns
    assert "body__hostname" in df.columns
    assert "body_message" in df.columns
    assert "body__cap_effective" in df.columns
    assert df.shape[0] == 3
