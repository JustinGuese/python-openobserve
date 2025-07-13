"""
Fuzzing tests

from `hypothesis write python_openobserve.openobserve`
This test code was written by the `hypothesis.extra.ghostwriter` module
and is provided under the Creative Commons Zero public domain dedication.

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=fixme,missing-function-docstring,too-many-arguments,too-many-positional-arguments

from hypothesis import given, strategies as st
import python_openobserve.openobserve


@given(
    user=st.text(),
    password=st.text(),
    organisation=st.just("default"),
    host=st.just("http://localhost:5080"),
    verify=st.booleans(),
    timeout=st.just(10),
)
# pylint: disable=invalid-name
def test_fuzz_OpenObserve(user, password, organisation, host, verify, timeout) -> None:
    python_openobserve.openobserve.OpenObserve(
        user=user,
        password=password,
        organisation=organisation,
        host=host,
        verify=verify,
        timeout=timeout,
    )


@given(
    sql=st.text(),
    timeout=st.just(10),
)
def test_fuzz_search(sql, timeout) -> None:
    o2 = python_openobserve.openobserve.OpenObserve(
        user="root@example.com",
        password="Complexpass#123",  # nosec B106
    )
    o2.search(
        sql=sql,
        timeout=timeout,
    )


@given(
    dictionary=st.dictionaries(st.text(), st.integers() | st.text()),
    parent_key=st.just(""),
    separator=st.just("."),
)
def test_fuzz_flatten(dictionary, parent_key, separator) -> None:
    python_openobserve.openobserve.flatten(
        dictionary=dictionary, parent_key=parent_key, separator=separator
    )


@given(input_string=st.text())
def test_fuzz_is_ksuid(input_string: str) -> None:
    python_openobserve.openobserve.is_ksuid(input_string=input_string)


@given(input_string=st.text())
def test_fuzz_is_name(input_string: str) -> None:
    python_openobserve.openobserve.is_name(input_string=input_string)
