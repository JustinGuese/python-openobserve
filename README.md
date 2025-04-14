# python-openobserve
A python connector to submit information to OpenObserve (https://github.com/openobserve/openobserve)

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
results = OO.search(sql)
print(f"got {len(results)} results")
pprint(results)
```
