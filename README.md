# python-openobserve
A python connector to interact with OpenObserve (https://github.com/openobserve/openobserve) be it sending data, searching, or exporting/importing settings.

The idea is to have a similar python connector to the "Elasticsearch" package, which allows a 1:1 replacement of the "Elasticsearch" package with the "OpenObserve" package.

OpenObserve is way more lightweight than Elasticsearch, and it is open source, like everything should be.

## install

`pip install python-openobserve`

## usage

see [example.ipynb](example.ipynb) for a full example

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
