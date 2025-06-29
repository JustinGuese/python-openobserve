# Tests

<!--
    SPDX-FileCopyrightText: 2025 The python_openobserve authors
    SPDX-License-Identifier: CC-BY-SA-4.0
-->

## Pre-commit

Github workflow includes pre-commit as a control point but it is expected that developers submitting PR use it locally.

```shell
$ pre-commit run -a
check yaml...............................................................Passed
fix end of files.........................................................Passed
trim trailing whitespace.................................................Passed
check for added large files..............................................Passed
check json...............................................................Passed
pretty format json.......................................................Passed
detect private key.......................................................Passed
check for case conflicts.................................................Passed
fix requirements.txt.....................................................Passed
check python ast.........................................................Passed
check that scripts with shebangs are executable..........................Passed
check for merge conflicts................................................Passed
check for broken symlinks............................(no files to check)Skipped
check toml...............................................................Passed
check xml............................................(no files to check)Skipped
check docstring is first.................................................Passed
black....................................................................Passed
pylint...................................................................Passed
mypy.....................................................................Passed
bandit...................................................................Passed
codespell................................................................Passed
Detect secrets...........................................................Passed
TruffleHog...............................................................Passed
```

## Pytest

Project mostly use pytest for functional validation with online and offline tests. Online test requires access to a server with configuration in a .env file. Offline tests can be done without a server but are based on behavior at writing time which may change.

```shell
$ pytest tests/test_openobserve_api.py
=============================================================================== test session starts ===============================================================================
platform linux -- Python 3.13.3, pytest-8.3.5, pluggy-1.5.0
rootdir: /home/user/tmp/python-openobserve
configfile: pyproject.toml
plugins: anyio-4.8.0, subtests-0.14.1, check-2.5.0, schemathesis-3.39.16, cov-6.0.0, hypothesis-6.131.9
collected 46 items

tests/test_openobserve_api.py ..............................................                                                                                                [100%]

================================================================================ warnings summary =================================================================================
../venv/lib64/python3.13/site-packages/schemathesis/generation/coverage.py:305
  /home/user/tmp/venv/lib64/python3.13/site-packages/schemathesis/generation/coverage.py:305: DeprecationWarning: jsonschema.exceptions.RefResolutionError is deprecated as of version 4.18.0. If you wish to catch potential reference resolution errors, directly catch referencing.exceptions.Unresolvable.
    ref_error: type[Exception] = jsonschema.RefResolutionError,

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
========================================================================= 46 passed, 1 warning in 34.87s ==========================================================================
$ pytest tests/test_openobserve_api_offline.py
=============================================================================== test session starts ===============================================================================
platform linux -- Python 3.13.3, pytest-8.3.5, pluggy-1.5.0
rootdir: /home/user/tmp/python-openobserve
configfile: pyproject.toml
plugins: anyio-4.8.0, subtests-0.14.1, check-2.5.0, schemathesis-3.39.16, cov-6.0.0, hypothesis-6.131.9
collected 26 items

tests/test_openobserve_api_offline.py ..........................                                                                                                            [100%]

================================================================================ warnings summary =================================================================================
../venv/lib64/python3.13/site-packages/schemathesis/generation/coverage.py:305
  /home/user/tmp/venv/lib64/python3.13/site-packages/schemathesis/generation/coverage.py:305: DeprecationWarning: jsonschema.exceptions.RefResolutionError is deprecated as of version 4.18.0. If you wish to catch potential reference resolution errors, directly catch referencing.exceptions.Unresolvable.
    ref_error: type[Exception] = jsonschema.RefResolutionError,

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
========================================================================== 26 passed, 1 warning in 0.63s ==========================================================================
```

## Security

* [Github code scanning](https://docs.github.com/en/code-security/code-scanning/introduction-to-code-scanning/about-code-scanning)
* [Guarddog](https://github.com/DataDog/guarddog) - CLI tool to Identify malicious PyPI and npm packages
* [osv-scanner](https://github.com/google/osv-scanner/) - Vulnerability scanner
* [OSSF Scorecard](https://github.com/marketplace/actions/ossf-scorecard-action)
