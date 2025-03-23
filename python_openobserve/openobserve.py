import requests
import base64
from datetime import datetime
from collections.abc import MutableMapping
from typing import List, Dict, Union, Optional, Any
import sqlglot
import json

try:
    import pandas

    HAVE_MODULE_PANDAS = True
except ImportError:
    print(
        "Can't import pandas. dataframe output, csv and xlsx export will be unavailable."
    )
    HAVE_MODULE_PANDAS = False


def flatten(dictionary, parent_key="", separator="."):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


class OpenObserve:
    def __init__(
        self,
        user: str,
        password: str,
        organisation: str = "default",
        host: str = "http://localhost:5080",
        verify: bool = True,
    ) -> None:
        bas64encoded_creds = base64.b64encode(
            f"{user}:{password}".encode("utf-8")
        ).decode("utf-8")
        self.openobserve_url = f"{host}/api/{organisation}/[STREAM]"
        self.openobserve_host = host
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {bas64encoded_creds}",
        }
        self.verify = verify

    def _debug(self, msg: Any, verbosity: int, level: int = 1) -> None:
        """Print debug messages if verbosity level is sufficient"""
        if verbosity >= level:
            from pprint import pprint

            pprint(msg)

    def _handle_response(self, res: requests.Response, action: str = "request") -> dict:
        """Handle API response and return JSON if successful"""
        if res.status_code != requests.codes.ok:
            raise Exception(
                f"Openobserve {action} returned {res.status_code}. Text: {res.text}"
            )
        return res.json()

    def __timestampConvert(self, timestamp: datetime) -> int:
        """Convert Python datetime to OpenObserve timestamp (microseconds)"""
        return int(timestamp.timestamp() * 1000000)

    def __unixTimestampConvert(self, timestamp: int) -> datetime:
        """Convert OpenObserve timestamp to Python datetime"""
        try:
            return datetime.fromtimestamp(timestamp / 1000000)
        except:
            print(f"could not convert timestamp: {timestamp}")
            return datetime.fromtimestamp(0)

    def __intts2datetime(self, flatdict: dict) -> dict:
        """Convert timestamp fields in dict to datetime objects"""
        for key, val in flatdict.items():
            if "time" in key:
                flatdict[key] = self.__unixTimestampConvert(val)
        return flatdict

    def __datetime2Str(self, flatdict: dict) -> dict:
        """Convert datetime fields in dict to timestamp integers"""
        for key, val in flatdict.items():
            if isinstance(val, datetime):
                flatdict[key] = self.__timestampConvert(val)
        return flatdict

    def index(self, index: str, document: dict) -> dict:
        """Index a document in OpenObserve"""
        assert isinstance(document, dict), "document must be a dict"
        document = self.__datetime2Str(flatten(document))

        res = requests.post(
            f"{self.openobserve_url.replace('[STREAM]', index)}/_json",
            headers=self.headers,
            json=[document],
            verify=self.verify,
        )
        response_json = self._handle_response(res, "index")

        if response_json["status"][0]["failed"] > 0:
            raise Exception(
                f"Openobserve index failed. {response_json['status'][0]['error']}. document: {document}"
            )
        return response_json

    def search(
        self,
        sql: str,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
        outformat: str = "json",
    ) -> List[Dict]:
        """Execute a search query using SQL"""
        # Convert datetime objects to timestamps if needed
        if isinstance(start_time, datetime):
            start_time = self.__timestampConvert(start_time)
        if isinstance(end_time, datetime):
            end_time = self.__timestampConvert(end_time)

        # Verify SQL syntax
        try:
            sqlglot.transpile(sql)
        except sqlglot.errors.ParseError as e:
            raise e

        query = {"query": {"sql": sql, "start_time": start_time, "end_time": end_time}}
        self._debug(query, verbosity)

        res = requests.post(
            f"{self.openobserve_url.replace('/[STREAM]', '')}/_search",
            json=query,
            headers=self.headers,
            verify=self.verify,
        )

        response_json = self._handle_response(res, "search")
        hits = [self.__intts2datetime(x) for x in response_json["hits"]]

        if outformat == "df" and HAVE_MODULE_PANDAS:
            return pandas.json_normalize(hits["hits"])
        return hits

    def _execute_api_request(
        self,
        endpoint: str,
        verbosity: int = 0,
        method: str = "GET",
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
    ) -> dict:
        """Execute API request with proper error handling and debugging"""
        url = self.openobserve_url.replace("[STREAM]", endpoint)
        self._debug(url, verbosity)

        if method == "GET":
            res = requests.get(
                url, headers=self.headers, params=params, verify=self.verify
            )
        elif method == "POST":
            res = requests.post(
                url, headers=self.headers, json=json_data, verify=self.verify
            )
        elif method == "PUT":
            res = requests.put(
                url, headers=self.headers, json=json_data, verify=self.verify
            )
        else:
            raise ValueError(f"Unsupported method: {method}")

        return self._handle_response(res, f"{method}_{endpoint.split('/')[0]}")

    def list_functions(self, verbosity: int = 0, outformat: str = "json"):
        """
        List available functions
        https://openobserve.ai/docs/api/functions
        """
        response_json = self._execute_api_request("functions", verbosity)
        if outformat == "df" and HAVE_MODULE_PANDAS:
            return pandas.json_normalize(response_json["list"])
        return response_json

    def list_pipelines(self, verbosity: int = 0, outformat: str = "json"):
        """List available pipelines"""
        response_json = self._execute_api_request("pipelines", verbosity)
        if outformat == "df" and HAVE_MODULE_PANDAS:
            return pandas.json_normalize(response_json["list"])
        return response_json

    def list_streams(
        self,
        schema: bool = False,
        streamtype: str = "logs",
        verbosity: int = 0,
        outformat: str = "json",
    ):
        """
        List available streams
        https://openobserve.ai/docs/api/stream/list/
        """
        response_json = self._execute_api_request(
            f"streams?fetchSchema={schema}&type={streamtype}", verbosity
        )
        if outformat == "df":
            return pandas.json_normalize(response_json["list"])
        return response_json

    def list_alerts(self, verbosity: int = 0, outformat: str = "json"):
        """List configured alerts on server"""
        response_json = self._execute_api_request("alerts", verbosity)
        if outformat == "df":
            return pandas.json_normalize(response_json["list"])
        return response_json

    def list_users(self, verbosity: int = 0, outformat: str = "json"):
        """List available users  https://openobserve.ai/docs/api/users"""
        response_json = self._execute_api_request("users", verbosity)
        if outformat == "df" and HAVE_MODULE_PANDAS:
            try:
                return pandas.json_normalize(response_json["data"])
            except KeyError as err:
                raise Exception(f"Exception KeyError: {err}")
            except Exception as err:
                raise Exception(f"Exception: {err}")
        return response_json

    def list_dashboards(self, verbosity: int = 0, outformat: str = "json"):
        """List available dashboards  https://openobserve.ai/docs/api/dashboards"""
        response_json = self._execute_api_request("dashboards", verbosity)
        if outformat == "df" and HAVE_MODULE_PANDAS:
            return pandas.json_normalize(response_json["dashboards"])
        return response_json

    def list_objects(
        self, object_type: str, verbosity: int = 0, outformat: str = "json"
    ):
        """List available objects for given type"""
        key_mapping = {
            "dashboards": "dashboards",
            "users": "data",
            "alerts/destinations": 0,
            "alerts/templates": 0,
        }

        key = key_mapping.get(object_type, "list")
        response_json = self._execute_api_request(object_type, verbosity)

        if outformat == "df" and HAVE_MODULE_PANDAS:
            return pandas.json_normalize(response_json[key])
        return response_json

    def config_export(
        self, file_path: str, verbosity: int = 0, outformat: str = "json"
    ):
        """Export OpenObserve configuration to json/csv/xlsx"""
        # Convert format for initial data retrieval if needed
        data_format = "df" if outformat in ["csv", "xlsx"] else outformat

        # Collect all configuration data
        object_types = {
            "functions": "functions",
            "pipelines": "pipelines",
            "alerts": "alerts",
            "alerts-destinations": "alerts/destinations",
            "alerts-templates": "alerts/templates",
            "dashboards": "dashboards",
            "streams": "streams",
            "users": "users",
        }

        # Collect all data
        data = {
            name: self.list_objects(
                api_path, verbosity=verbosity, outformat=data_format
            )
            for name, api_path in object_types.items()
        }

        # Export based on format
        if outformat == "csv":
            for name, df in data.items():
                df.to_csv(f"{file_path}{name}.csv")
        elif outformat == "xlsx":
            for name, df in data.items():
                df.to_excel(f"{file_path}{name}.xlsx")
        else:  # default json
            for name, object_data in data.items():
                with open(f"{file_path}{name}.json", "w", encoding="utf-8") as f:
                    json.dump(object_data, f, ensure_ascii=False, indent=4)

    def create_function(self, function_json: dict, verbosity: int = 0):
        """Create function https://openobserve.ai/docs/api/function/create"""
        url = self.openobserve_url.replace("[STREAM]", "functions")
        self._debug(f"Create function url: {url}", verbosity)
        self._debug(f"Create function json input: {function_json}", verbosity, level=2)

        res = requests.post(
            url, json=function_json, headers=self.headers, verify=self.verify
        )
        self._debug(f"Return {res.status_code}. Text: {res.text}", verbosity, level=1)
        self._handle_response(res, "create_function")

        self._debug("Create function completed", verbosity)
        return True

    def update_function(self, function_json: dict, verbosity: int = 0):
        """Update function         https://openobserve.ai/docs/api/function/update"""
        url = self.openobserve_url.replace(
            "[STREAM]", f"functions/{function_json['name']}"
        )
        self._debug(f"Update function url: {url}", verbosity)
        self._debug(f"Update function json input: {function_json}", verbosity, level=2)

        res = requests.put(
            url, json=function_json, headers=self.headers, verify=self.verify
        )
        self._debug(f"Return {res.status_code}. Text: {res.text}", verbosity, level=1)
        self._handle_response(res, "update_function")

        self._debug("Update function completed", verbosity)
        return True

    def import_functions(
        self, file_path: str, overwrite: bool = False, verbosity: int = 0
    ):
        """Import functions from json file"""
        self._debug(self.openobserve_url.replace("[STREAM]", "functions"), verbosity)

        with open(file_path, "r") as json_file:
            json_data = json.load(json_file)
            self._debug(json_data, verbosity, level=2)

            for function in json_data.get("list", []):
                self._debug(function["name"], verbosity)
                self._debug(function, verbosity, level=1)

                try:
                    return self.create_function(function, verbosity=verbosity)
                except Exception:
                    if overwrite:
                        print(
                            f"Overwrite enabled. Updating function {function['name']}"
                        )
                        return self.update_function(function, verbosity=verbosity)
        return True

    def create_object(self, object_type: str, object_json: dict, verbosity: int = 0):
        """Create object"""
        url = self.openobserve_url.replace("[STREAM]", object_type)
        self._debug(f"Create object {object_type} url: {url}", verbosity, level=1)
        self._debug(f"Create object json input: {object_json}", verbosity, level=2)

        res = requests.post(
            url, json=object_json, headers=self.headers, verify=self.verify
        )
        self._debug(f"Return {res.status_code}. Text: {res.text}", verbosity, level=1)
        self._handle_response(res, f"create_object_{object_type}")

        self._debug("Create object completed", verbosity)
        return True

    def update_object(self, object_type: str, object_json: dict, verbosity: int = 0):
        """Update object"""
        url = self.openobserve_url.replace(
            "[STREAM]", f"{object_type}/{object_json['name']}"
        )
        self._debug(f"Update object {object_type} url: {url}", verbosity, level=1)
        self._debug(f"Update object json input: {object_json}", verbosity, level=2)

        res = requests.put(
            url, json=object_json, headers=self.headers, verify=self.verify
        )
        self._debug(f"Return {res.status_code}. Text: {res.text}", verbosity, level=3)
        self._handle_response(res, f"update_object_{object_type}")

        self._debug("Update object completed", verbosity)
        return True

    def import_objects(
        self,
        object_type: str,
        file_path: str,
        overwrite: bool = False,
        verbosity: int = 0,
    ):
        """Import objects from json file"""
        # Determine key mappings based on object type
        key_mappings = {
            "dashboards": ("dashboards", "dashboardId"),
            "users": ("data", "email"),
            "alerts/destinations": (None, "name"),
            "alerts/templates": (None, "name"),
        }
        list_key, id_key = key_mappings.get(object_type, ("list", "name"))

        with open(file_path, "r") as json_file:
            json_data = json.load(json_file)
            self._debug(json_data, verbosity, level=3)

            # Handle special cases or use standard list extraction
            if object_type in ["alerts/destinations", "alerts/templates"]:
                json_list = json_data
            else:
                try:
                    json_list = json_data[list_key] if list_key else [json_data]
                except:
                    json_list = [json_data]

            # Process each object
            for json_object in json_list:
                object_id = json_object.get(id_key, "unknown")
                self._debug(f"Try to create {object_type} {object_id}...", verbosity)
                self._debug(json_object, verbosity, level=2)

                try:
                    res = self.create_object(
                        object_type, json_object, verbosity=verbosity
                    )
                    self._debug(f"Create returns {res}.", verbosity)
                    return res
                except Exception:
                    if overwrite and "name" in json_object:
                        print(
                            f"Overwrite enabled. Updating object {json_object['name']}"
                        )
                        res = self.update_object(
                            object_type, json_object, verbosity=verbosity
                        )
                        self._debug(f"Update returns {res}.", verbosity)
        return True

    def config_import(
        self,
        object_type: str,
        file_path: str,
        overwrite: bool = False,
        verbosity: int = 0,
    ):
        """Import OpenObserve configuration from json files"""
        if object_type == "all":
            importable_types = [
                "functions",
                "pipelines",
                "alerts",
                "alerts/destinations",
                "alerts/templates",
                "dashboards",
                # 'streams' and 'users' are not supported by the API
            ]

            for item in importable_types:
                file_suffix = item.replace("/", "-")
                self.import_objects(
                    item, f"{file_path}{file_suffix}.json", overwrite, verbosity
                )
        else:
            self.import_objects(object_type, file_path, overwrite, verbosity)
