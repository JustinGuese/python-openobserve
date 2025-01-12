import requests
import base64
from datetime import datetime, timedelta
from collections.abc import MutableMapping
from typing import List, Dict
import sqlglot
import json
from pprint import pprint


def flatten(dictionary, parent_key='', separator='.'):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)

class OpenObserve:
    def __init__(self, user, password, organisation = "default", host = "http://localhost:5080", verify = True) -> None:
        bas64encoded_creds = base64.b64encode(bytes(user + ":" + password, "utf-8")).decode("utf-8")
        self.openobserve_url = host + "/api/" + organisation + "/" + "[STREAM]" 
        self.headers =  {'Content-Type': 'application/x-www-form-urlencoded', "Authorization": "Basic " + bas64encoded_creds}
        self.verify = verify

    def __timestampConvert(self, timestamp: datetime) -> int:
        return int(timestamp.timestamp() * 1000)
    
    def __unixTimestampConvert(self, timestamp: int) -> datetime:
        try: 
            timestamp = datetime.fromtimestamp(timestamp/1000000)
        except:
            print("could not convert timestamp: " + str(timestamp))
        return timestamp
    
    def __intts2datetime(self, flatdict: dict) -> dict:
        for key, val in flatdict.items():
            if "time" in key:
                flatdict[key] = self.__unixTimestampConvert(val)
        return flatdict

    def __datetime2Str(self, flatdict: dict) -> dict:
        for key, val in flatdict.items():
            if isinstance(val, datetime):
                flatdict[key] = self.__timestampConvert(val)
        return flatdict

    def index(self, index: str, document: dict):
        assert isinstance(document, dict), "document must be a dict"
        # expects a flattened json
        document = flatten(document)
        document = self.__datetime2Str(document)

        res = requests.post(self.openobserve_url.replace("[STREAM]", index) + "/_json", headers=self.headers, json=[document], verify=self.verify)
        if res.status_code != 200:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if res["status"][0]["failed"] > 0:
            raise Exception(f"Openobserve index failed. {res['status'][0]['error']}. document: {document}")
        return res

    def search(self, sql: str, start_time: datetime = 0, end_time: datetime = 0, verbosity: int = 0) -> List[Dict]:
        if isinstance(start_time, datetime):
            # convert to unixtime 
            start_time = self.__timestampConvert(start_time)
        if isinstance(end_time, datetime):
            # convert to unixtime 
            end_time = self.__timestampConvert(end_time)

        # verify sql
        try:
            sqlglot.transpile(sql)
        except sqlglot.errors.ParseError as e:
            raise e

        query = {"query" : {
                    "sql": sql,
                    "start_time": start_time,
                    "end_time": end_time
                }}
        if verbosity > 0:
            pprint(query)
        res = requests.post(self.openobserve_url.replace("/[STREAM]", "") + "/_search", json = query, headers=self.headers, verify=self.verify)
        if res.status_code != 200:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}. url: {res.url}")
        # timestamp back convert
        res = res.json()["hits"]
        res = [self.__intts2datetime(x) for x in res]
        return res
