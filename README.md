# python-openobserve

<!--
    SPDX-FileCopyrightText: 2025 The python_openobserve authors
    SPDX-License-Identifier: CC-BY-SA-4.0
-->

[![Actions Status - Main](https://github.com/JustinGuese/python-openobserve/workflows/pytest/badge.svg)](https://github.com/JustinGuese/python-openobserve/actions?query=branch%3Amain)
[![PyPI Downloads](https://static.pepy.tech/badge/python-openobserve/week)](https://pepy.tech/projects/python-openobserve)
[![Documentation build status](https://img.shields.io/readthedocs/python-openobserve?logo=read-the-docs)](https://python-openobserve.readthedocs.org/en/latest/)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/JustinGuese/python-openobserve/badge)](https://scorecard.dev/viewer/?uri=github.com/JustinGuese/python-openobserve)

A python connector to interact with OpenObserve (https://github.com/openobserve/openobserve) be it sending data, searching, or exporting/importing settings.

The idea is to have a similar python connector to the "Elasticsearch" package, which allows a 1:1 replacement of the "Elasticsearch" package with the "OpenObserve" package.

OpenObserve is way more lightweight than Elasticsearch, and it is open source, like everything should be.

## install

`pip install python-openobserve`

## usage

see [example.ipynb](docs/example.ipynb) for a full example

```python
from python_openobserve.openobserve import OpenObserve

OO = OpenObserve(user = "root@example.com", password = "Complexpass#123")

from datetime import datetime
from random import random
from pprint import pprint
document = {
    "@timestamp" : datetime.utcnow(),
    "component" : "testagent",
    "action" : "buy",
    "amount" : random() * 100,
    "portfolio" : {
        "USD" : random() * 100.0,
        "BTC" : 0.1 + random() * 0.1
    }
}
pprint(document)

# insert document
OO.index("dd", document)

#search
# example sql parsing helper
sql = 'SELECT * FROM "dd"'
results = OO.search(sql, days=1)
print(f"got {len(results)} results")
pprint(results)

OO = OpenObserve(host="https://o2.example.com", user="root@example.com", password="Complexpass#123")
sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
start_timeperiod = datetime.now() - timedelta(days=7)
end_timeperiod = datetime.now()
df_search_results = oo_conn.search2df(
    sql,
    start_time=start_timeperiod,
    end_time=end_timeperiod,
    verbosity=5,
)

# Export settings in split json, csv, or xlsx
OO.config_export(f"{tmpdir}/", verbosity=0, split=True)
OO.config_export(f"{tmpdir}/", verbosity=0, outformat="csv")
OO.config_export(f"{tmpdir}/", verbosity=0, outformat="xlsx")
```

## Troubleshooting

If you have issues, try:

* increase verbosity
* reproduce issue with curl directly
* check known issues

## Known issues

There are notable gaps and inconsistence in openobserve API at Q2 2025. Check against documentation and swagger. Report API issues upstream.

* folder_id in alerts creation is ignored
* API field names are variable (id, alert_id)
* API returns are inconsistent (use folderId at alert creation but get folder name at alert listing)
