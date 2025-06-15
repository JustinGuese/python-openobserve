"""
Fuzzing tests

from `hypothesis write python_openobserve.openobserve`
This test code was written by the `hypothesis.extra.ghostwriter` module
and is provided under the Creative Commons Zero public domain dedication.
"""

# pylint: disable=fixme,missing-function-docstring,too-many-arguments,too-many-positional-arguments

from hypothesis import given, strategies as st
import python_openobserve.openobserve

# TODO: replace st.nothing() with appropriate strategies


@given(
    user=st.nothing(),
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


@given(dictionary=st.nothing(), parent_key=st.just(""), separator=st.just("."))
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
