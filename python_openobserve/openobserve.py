import requests
import base64
from datetime import datetime, timedelta
from collections.abc import MutableMapping
from typing import List, Dict
import sqlglot
import json
from pprint import pprint
import pandas


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
        self.openobserve_host = host
        self.headers =  {'Content-Type': 'application/json', "Authorization": "Basic " + bas64encoded_creds}
        self.verify = verify

    def __timestampConvert(self, timestamp: datetime) -> int:
        return int(timestamp.timestamp() * 1000000)
    
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

    def search(self, sql: str, start_time: datetime = 0, end_time: datetime = 0, verbosity: int = 0, outformat: str = 'json') -> List[Dict]:
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
        if outformat == 'df':
            # FIXME! set type for _timestamp column
            return pandas.json_normalize(res["hits"])
        return res

    def list_functions(self, verbosity: int = 0, outformat: str = 'json'):
        """
        List available functions
        https://openobserve.ai/docs/api/functions
        """
        url = self.openobserve_url.replace("[STREAM]", f"functions")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            return pandas.json_normalize(res["list"])
        return res

    def list_pipelines(self, verbosity: int = 0, outformat: str = 'json'):
        """
        List available pipelines
        """
        url = self.openobserve_url.replace("[STREAM]", f"pipelines")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            return pandas.json_normalize(res["list"])
        return res

    def list_streams(self, schema: bool = False, streamtype: str = 'logs', verbosity: int = 0, outformat: str = 'json'):
        """
        List available streams
        https://openobserve.ai/docs/api/stream/list/
        """
        url = self.openobserve_url.replace("[STREAM]", f"streams?fetchSchema={schema}&type={streamtype}")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            return pandas.json_normalize(res["list"])
        return res

    def list_alerts(self, verbosity: int = 0, outformat: str = 'json'):
        """
        List configured alerts on server
        """
        url = self.openobserve_url.replace("[STREAM]", "alerts")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            return pandas.json_normalize(res["list"])
        return res

    def list_users(self, verbosity: int = 0, outformat: str = 'json'):
        """
        List available users
        https://openobserve.ai/docs/api/users
        """
        url = self.openobserve_url.replace("[STREAM]", f"users")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            try:
                return pandas.json_normalize(res["data"])
            except KeyError as err:
                raise Exception(f"Exception KeyError: {err}")
            except Exception as err:
                raise Exception(f"Exception: {err}")
        return res

    def list_dashboards(self, verbosity: int = 0, outformat: str = 'json'):
        """
        List available dashboards
        https://openobserve.ai/docs/api/dashboards
        """
        url = self.openobserve_url.replace("[STREAM]", f"dashboards")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            return pandas.json_normalize(res["dashboards"])
        return res

    def config_export(self, file_path: str, verbosity: int = 0, outformat: str = 'json'):
        """
        Export OpenObserve configuration to json/csv/xlsx
        """
        outformat2 = outformat
        if outformat == 'csv' or outformat == 'xlsx':
            outformat2 = 'df'
        functions1 = self.list_objects('functions', verbosity=verbosity, outformat=outformat2)
        pipelines1 = self.list_objects('pipelines', verbosity=verbosity, outformat=outformat2)
        alerts1 = self.list_objects('alerts', verbosity=verbosity, outformat=outformat2)
        alerts_destinations1 = self.list_objects('alerts/destinations', verbosity=verbosity, outformat=outformat2)
        alerts_templates1 = self.list_objects('alerts/templates', verbosity=verbosity, outformat=outformat2)
        dashboards1 = self.list_objects('dashboards', verbosity=verbosity, outformat=outformat2)
        streams1 = self.list_objects('streams', verbosity=verbosity, outformat=outformat2)
        users1 = self.list_objects('users', verbosity=verbosity, outformat=outformat2)
        if outformat == 'csv':
            functions1.to_csv(f"{file_path}functions.csv")
            pipelines1.to_csv(f"{file_path}pipelines.csv")
            alerts1.to_csv(f"{file_path}alerts.csv")
            alerts_destinations1.to_csv(f"{file_path}alerts-destinations.csv")
            alerts_templates1.to_csv(f"{file_path}alerts-templates.csv")
            dashboards1.to_csv(f"{file_path}dashboards.csv")
            streams1.to_csv(f"{file_path}streams.csv")
            users1.to_csv(f"{file_path}users.csv")
        elif outformat == 'xlsx':
            functions1.to_excel(f"{file_path}functions.xlsx")
            pipelines1.to_excel(f"{file_path}pipelines.xlsx")
            alerts1.to_excel(f"{file_path}alerts.xlsx")
            alerts_destinations1.to_excel(f"{file_path}alerts-destinations.xlsx")
            alerts_templates1.to_excel(f"{file_path}alerts-templates.xlsx")
            dashboards1.to_excel(f"{file_path}dashboards.xlsx")
            streams1.to_excel(f"{file_path}streams.xlsx")
            users1.to_excel(f"{file_path}users.xlsx")
        else:
            # default json
            with open(f"{file_path}functions.json", 'w', encoding='utf-8') as f:
                json.dump(functions1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}pipelines.json", 'w', encoding='utf-8') as f:
                json.dump(pipelines1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}alerts.json", 'w', encoding='utf-8') as f:
                json.dump(alerts1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}alerts-destinations.json", 'w', encoding='utf-8') as f:
                json.dump(alerts_destinations1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}alerts-templates.json", 'w', encoding='utf-8') as f:
                json.dump(alerts_templates1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}dashboards.json", 'w', encoding='utf-8') as f:
                json.dump(dashboards1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}streams.json", 'w', encoding='utf-8') as f:
                json.dump(streams1, f, ensure_ascii=False, indent=4)
            with open(f"{file_path}users.json", 'w', encoding='utf-8') as f:
                json.dump(users1, f, ensure_ascii=False, indent=4)

    def create_function(self, function_json: str, verbosity: int = 0, outformat: str = 'json'):
        """
        Create function
        https://openobserve.ai/docs/api/function/create
        """
        url = self.openobserve_url.replace("[STREAM]", f"functions")
        if verbosity > 0:
            pprint(f"Create function url: {url}")
        if verbosity > 2:
            pprint(f"Create function json input: {function_json}")
        res = requests.post(url, json=function_json, headers=self.headers, verify=self.verify)
        if verbosity > 1:
            pprint(f"Return {res.status_code}. Text: {res.text}")
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        if verbosity > 0:
            pprint(f"Create function completed")
        return True

    def update_function(self, function_json: str, verbosity: int = 0, outformat: str = 'json'):
        """
        Update function
        https://openobserve.ai/docs/api/function/update
        """
        url = self.openobserve_url.replace("[STREAM]", f"functions/{function_json['name']}")
        if verbosity > 0:
            pprint(f"Update function url: {url}")
        if verbosity > 2:
            pprint(f"Update function json input: {function_json}")
        res = requests.put(url, json=function_json, headers=self.headers, verify=self.verify)
        if verbosity > 1:
            pprint(f"Return {res.status_code}. Text: {res.text}")
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        if verbosity > 0:
            pprint(f"Update function completed")
        return True

    def import_functions(self, file_path: str, overwrite: bool = False, verbosity: int = 0, outformat: str = 'json'):
        """
        Import functions from json
        Note: API does not import list of functions, need to do one by one.
        """
        url = self.openobserve_url.replace("[STREAM]", f"functions")
        if verbosity > 0:
            pprint(url)
        with open(file_path, 'r') as json_file:
           json_data = json.loads(json_file.read())
           if verbosity > 2:
               pprint(json_data)
           for function in json_data['list']:
               if verbosity > 0:
                  pprint(function['name'])
               if verbosity > 1:
                  pprint(function)
               try:
                  res = self.create_function(function, verbosity=verbosity)
                  return res
               except Exception as err:
                  if overwrite:
                      print(f"Overwrite enabled. Updating function {function['name']}")
                      res = self.update_function(function, verbosity=verbosity)
                      return res
        return True

    def list_objects(self, object_type: str, verbosity: int = 0, outformat: str = 'json'):
        """
        List available objects for given type
        """
        key = 'list'
        if object_type == 'dashboards':
            key = 'dashboards'
        if object_type == 'users':
            key = 'data'
        if object_type == 'alerts/destinations' or object_type == 'alerts/templates':
            key = 0
        url = self.openobserve_url.replace("[STREAM]", f"{object_type}")
        res = requests.get(url, headers=self.headers, verify=self.verify)
        if verbosity > 0:
            pprint(url)
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
        res = res.json()
        if outformat == 'df':
            return pandas.json_normalize(res[key])
        return res

    def create_object(self, object_type: str, object_json: str, verbosity: int = 0):
        """
        Create object
        """
        url = self.openobserve_url.replace("[STREAM]", f"{object_type}")
        if verbosity > 1:
            pprint(f"Create object {object_type} url: {url}")
        if verbosity > 2:
            pprint(f"Create object json input: {object_json}")
        res = requests.post(url, json=object_json, headers=self.headers, verify=self.verify)
        if verbosity > 1:
            pprint(f"Return {res.status_code}. Text: {res.text}")
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
            # pprint(f"Openobserve returned {res.status_code}. Text: {res.text}")
            # return False
        if verbosity > 0:
            pprint(f"Create object completed")
        return True

    def update_object(self, object_type: str, object_json: str, verbosity: int = 0):
        """
        Update object
        """
        url = self.openobserve_url.replace("[STREAM]", f"{object_type}/{object_json['name']}")
        if verbosity > 1:
            pprint(f"Update object {object_type} url: {url}")
        if verbosity > 2:
            pprint(f"Update object json input: {object_json}")
        res = requests.put(url, json=object_json, headers=self.headers, verify=self.verify)
        if verbosity > 3:
            pprint(f"Return {res.status_code}. Text: {res.text}")
        if res.status_code != requests.codes.ok:
            raise Exception(f"Openobserve returned {res.status_code}. Text: {res.text}")
            # pprint(f"Openobserve returned {res.status_code}. Text: {res.text}")
            # return False
        if verbosity > 0:
            pprint(f"Update object completed")
        return True

    def import_objects(self, object_type: str, file_path: str, overwrite: bool = False, verbosity: int = 0):
        """
        Import objects from json
        Note: API does not import list of objects, need to do one by one.
        """
        key = 'list'
        key2 = 'name'
        if object_type == 'dashboards':
            key = 'dashboards'
            key2 = 'dashboardId'
        if object_type == 'users':
            key = 'data'
            key2 = 'email'
        with open(file_path, 'r') as json_file:
           json_data = json.loads(json_file.read())
           if verbosity > 3:
               pprint(json_data)
           if object_type == 'alerts/destinations' or object_type == 'alerts/templates':
               json_list = json_data
           else:
               try:
                  json_list = json_data[key]
               except:
                  json_list = [ json_data ]
           for json_object in json_list:
               if verbosity > 0:
                  pprint(f"Try to create {object_type} {json_object[key2]}...")
               if verbosity > 2:
                  pprint(json_object)
               try:
                  res = self.create_object(object_type, json_object, verbosity=verbosity)
                  pprint(f"Create returns {res}.")
                  return res
               except Exception as err:
                  if overwrite:
                      print(f"Overwrite enabled. Updating object {json_object['name']}")
                      res = self.update_object(object_type, json_object, verbosity=verbosity)
                      pprint(f"Update returns {res}.")
        return True

    def config_import(self, object_type: str, file_path: str, overwrite: bool = False, verbosity: int = 0):
        """
        Import OpenObserve configuration from json
        object_type all does everything
        """
        if object_type == 'all':
            self.import_objects('functions', f"{file_path}functions.json", overwrite, verbosity)
            self.import_objects('pipelines', f"{file_path}pipelines.json", overwrite, verbosity)
            self.import_objects('alerts', f"{file_path}alerts.json", overwrite, verbosity)
            self.import_objects('alerts/destinations', f"{file_path}alerts-destinations.json", overwrite, verbosity)
            self.import_objects('alerts/templates', f"{file_path}alerts-templates.json", overwrite, verbosity)
            self.import_objects('dashboards', f"{file_path}dashboards.json", overwrite, verbosity)
            # No CreateStream, only CreateStreamSettings
            # self.import_objects('streams', f"{file_path}streams.json", overwrite, verbosity)
            # "Return 400. Text: Json deserialize error: missing field `password` at line 1" = Extra field required
            # self.import_objects('users', f"{file_path}users.json", overwrite, verbosity)
        else:
            self.import_objects(object_type, file_path, overwrite, verbosity)
