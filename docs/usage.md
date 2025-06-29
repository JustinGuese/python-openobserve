# Usage

<!--
    SPDX-FileCopyrightText: 2025 The python_openobserve authors
    SPDX-License-Identifier: CC-BY-SA-4.0
-->

## Installation

```shell
pip install python_openobserve
```

## Send data

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
```

## Search data

```python
# example sql parsing helper
sql = 'SELECT * FROM "dd"'
results = OO.search(sql, days=1)
print(f"got {len(results)} results")
pprint(results)

sql = 'SELECT log_file_name,count(*) FROM "default" GROUP BY log_file_name'
start_timeperiod = datetime.now() - timedelta(days=7)
end_timeperiod = datetime.now()
df_search_results = oo_conn.search2df(
    sql,
    start_time=start_timeperiod,
    end_time=end_timeperiod,
    verbosity=5,
)
```

## Analyse or visualize data

python_openobserve in itself has no analysis/visualization capacity but has integration with pandas, polars, and fireducks. Thus it benefits of the corresponding ecosystems to manipulate data.

## Export/Import configuration settings

You can export and import configuration settings

* alerts
* alerts/destinations
* alerts/templates
* dashboards
* functions
* pipelines
* streams
* users

```python
# Import as alerts object the content of json_alert
oo_conn.import_objects_split(
    "alerts",
    json_alert,
    "",
    verbosity=5,
)

# Export all settings in split json, csv, or xlsx
OO.config_export(f"{tmpdir}/", verbosity=0, split=True)
OO.config_export(f"{tmpdir}/", verbosity=0, outformat="csv")
OO.config_export(f"{tmpdir}/", verbosity=0, outformat="xlsx")
```
