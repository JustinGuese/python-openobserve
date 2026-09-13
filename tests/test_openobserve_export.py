"""
Pytest file for python-openobserve - export

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=duplicate-code

import os

import json
from datetime import datetime, timedelta

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


def test_logs_export_json(tmpdir):
    """Ensure can do logs export"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    start_timeperiod = datetime.now() - timedelta(hours=6)
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
    assert len(ls) == 6
    assert all("-export.json" in x for x in ls)
    assert "pytest--" in ls[0]
    assert os.path.exists(os.path.join(tmpdir, ls[0]))
    with open(os.path.join(tmpdir, ls[0]), "r", encoding="utf-8") as file:
        data = json.load(file)
        assert "_timestamp" in data[0].keys()
        assert "body__hostname" in data[0].keys()
        assert "body_message" in data[0].keys()
        assert "body__cap_effective" in data[0].keys()


def test_logs_export_csv(tmpdir):
    """Ensure can do logs export"""
    oo_conn = OpenObserve(host=OO_HOST, user=OO_USER, password=OO_PASS)
    start_timeperiod = datetime.now() - timedelta(hours=6)
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
    assert len(ls) == 6
    assert all("-export.csv" in x for x in ls)
    assert os.path.exists(os.path.join(tmpdir, ls[0]))
    df = pandas.read_csv(os.path.join(tmpdir, ls[0]))
    assert "_timestamp" in df.columns
    assert "body__hostname" in df.columns
    assert "body_message" in df.columns
    assert "body__cap_effective" in df.columns
    assert df.shape[0] > 100
