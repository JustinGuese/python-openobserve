"""OpenObserve API module"""

# pylint: disable=too-many-arguments,bare-except,broad-exception-raised,broad-exception-caught,too-many-public-methods
import base64
import json

# import glob
import os
import sys
from datetime import datetime
from collections.abc import MutableMapping
from typing import List, Dict, Union, Optional, Any, cast
from pathlib import Path

import requests
import sqlglot  # type: ignore

try:
    import pandas

    HAVE_MODULE_PANDAS = True
except ImportError:
    print(
        "Can't import pandas. dataframe output, csv and xlsx export will be unavailable."
    )
    HAVE_MODULE_PANDAS = False


def flatten(dictionary, parent_key="", separator="."):
    """Flatten dictionary"""
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


class OpenObserve:
    """
    OpenObserve class based on OpenObserve REST API
    https://openobserve.ai/docs/api/
    https://<openobserve server>/swagger/
    """

    def __init__(
        self,
        user,
        password,
        *,
        organisation="default",
        host="http://localhost:5080",
        verify=True,
        timeout=10,
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
        self.timeout = timeout

    def _debug(self, msg: Any, verbosity: int, level: int = 1) -> None:
        """Print debug messages if verbosity level is sufficient"""
        if verbosity >= level:
            # pylint: disable=import-outside-toplevel
            from pprint import pprint

            pprint(msg)

    def _handle_response(
        self, res: requests.Response, action: str = "request"
    ) -> List[Dict]:
        """Handle API response and return JSON if successful"""
        if res.status_code != requests.codes.ok:
            raise Exception(
                f"Openobserve {action} returned {res.status_code}. Text: {res.text}"
            )
        return res.json()

    # pylint: disable=invalid-name
    def __timestampConvert(self, timestamp: datetime, verbosity: int = 0) -> int:
        try:
            self._debug(f"__timestampConvert input: {timestamp}", verbosity, 2)
            return int(timestamp.timestamp() * 1000000)
        except Exception as exc:
            # pylint: disable=raising-bad-type
            raise (  # type: ignore[misc]
                f"Exception from __timestampConvert for timestamp {timestamp}: {exc}"
            )

    # pylint: disable=invalid-name
    def __unixTimestampConvert(self, timestamp: int) -> datetime:
        """Convert OpenObserve timestamp to Python datetime"""
        try:
            return datetime.fromtimestamp(timestamp / 1000000)
        except:
            print(f"could not convert timestamp: {timestamp}")
            return datetime.fromtimestamp(0)

    def __intts2datetime(
        self, flatdict: dict, timestamp_columns: Union[List[str], None]
    ) -> dict:
        if timestamp_columns is None:
            for key, val in flatdict.items():
                if "time" in key:
                    flatdict[key] = self.__unixTimestampConvert(val)
        else:
            for key, val in flatdict.items():
                if key in timestamp_columns:
                    flatdict[key] = self.__unixTimestampConvert(val)
        return flatdict

    # pylint: disable=invalid-name
    def __datetime2Str(self, flatdict: dict) -> dict:
        """Convert datetime fields in dict to timestamp integers"""
        for key, val in flatdict.items():
            if isinstance(val, datetime):
                flatdict[key] = self.__timestampConvert(val)
        return flatdict

    def index(self, index: str, document: dict) -> List[dict]:
        """Index a document in OpenObserve"""
        assert isinstance(document, dict), "document must be a dict"
        document = self.__datetime2Str(flatten(document))

        res = requests.post(
            f"{self.openobserve_url.replace('[STREAM]', index)}/_json",
            headers=self.headers,
            json=[document],
            verify=self.verify,
            timeout=self.timeout,
        )
        response_json = self._handle_response(res, "index")

        if response_json["status"][0]["failed"] > 0:  # type:ignore[call-overload]
            raise Exception(
                "Openobserve index failed. "
                f"{response_json['status'][0]['error']}"  # type:ignore[call-overload]
                f". document: {document}"
            )
        return response_json

    def search(
        self,
        sql: str,
        *,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
        timeout: int = 300,
        timestamp_conversion_auto: bool = False,
        timestamp_columns: Union[List[str], None] = None,
    ) -> List[Dict]:
        """
        OpenObserve search function
        https://openobserve.ai/docs/api/search/search/
        """
        if isinstance(start_time, datetime):
            start_time = self.__timestampConvert(start_time, verbosity)
        elif not isinstance(start_time, int):
            raise Exception(
                "Search invalid start_time input, neither datetime, nor int"
            )
        if isinstance(end_time, datetime):
            end_time = self.__timestampConvert(end_time, verbosity)
        elif not isinstance(end_time, int):
            raise Exception("Search invalid end_time input, neither datetime, nor int")

        self._debug(f"Query Time start {start_time} end {end_time}", verbosity, 1)

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
            timeout=timeout,
        )

        response_json = self._handle_response(res, "search")
        res_hits = response_json["hits"]  # type:ignore[call-overload]
        self._debug(res_hits, verbosity, 3)

        if timestamp_conversion_auto or timestamp_columns is not None:
            # timestamp back convert
            res_hits = [self.__intts2datetime(x, timestamp_columns) for x in res_hits]
        return res_hits

    def _execute_api_request(
        self,
        endpoint: str,
        *,
        verbosity: int = 0,
        method: str = "GET",
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
    ) -> List[Dict]:
        """Execute API request with proper error handling and debugging"""
        url = self.openobserve_url.replace("[STREAM]", endpoint)
        self._debug(url, verbosity)

        if method == "GET":
            res = requests.get(
                url,
                headers=self.headers,
                params=params,
                verify=self.verify,
                timeout=self.timeout,
            )
        elif method == "POST":
            res = requests.post(
                url,
                headers=self.headers,
                json=json_data,
                verify=self.verify,
                timeout=self.timeout,
            )
        elif method == "PUT":
            res = requests.put(
                url,
                headers=self.headers,
                json=json_data,
                verify=self.verify,
                timeout=self.timeout,
            )
        else:
            raise ValueError(f"Unsupported method: {method}")

        return self._handle_response(res, f"{method}_{endpoint.split('/')[0]}")

    def search2df(
        self,
        sql: str,
        *,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
        timeout: int = 300,
        timestamp_conversion_auto: bool = False,
        timestamp_columns: Union[List[str], None] = None,
    ) -> pandas.DataFrame:
        """
        OpenObserve search function with df output
        https://openobserve.ai/docs/api/search/search/
        """
        res_json_hits = self.search(
            sql,
            start_time=start_time,
            end_time=end_time,
            verbosity=verbosity,
            timeout=timeout,
            timestamp_conversion_auto=timestamp_conversion_auto,
            # leaving conversion to pandas
            # timestamp_columns=timestamp_columns,
        )
        df_res = pandas.json_normalize(res_json_hits)

        if timestamp_columns is not None:
            for col in list(
                set(df_res.columns) & set(["_timestamp"] + timestamp_columns)
            ):
                try:
                    # ensure timestamp format
                    if col in ["_timestamp"] + timestamp_columns:
                        df_res[col] = pandas.to_datetime(df_res[col])
                except Exception as err:
                    raise Exception(
                        err,
                        "query",
                        f"query column type conversion: {col} -> {df_res[col]}",
                    ) from err
        return df_res

    # pylint: disable=too-many-branches
    def export_objects_split(
        self,
        object_type: str,
        json_data: list[dict],
        file_path: str,
        *,
        verbosity: int = 0,
        flat: bool = False,
    ):
        """
        Export OpenObserve json configuration to split json files
        """
        key = "list"
        key2 = "name"
        if object_type == "dashboards":
            key = "dashboards"
            key2 = "dashboard_id"
        if object_type == "users":
            key = "data"
            key2 = "email"
        self._debug(json_data, verbosity, 3)
        if flat is True:
            dst_path = f"{file_path}{object_type}-"
        else:
            dst_path = f"{file_path}{object_type}/"
            Path(dst_path).mkdir(parents=True, exist_ok=True)
        if object_type in ("alerts/destinations", "alerts/templates"):
            self._debug("json_list set to alerts type", verbosity, 2)
            json_list = cast(List[Dict], json_data)
        else:
            try:
                json_list = cast(List[Dict], json_data[key])  # type: ignore[call-overload]
                self._debug(f"json_list set to key {key}: {json_list}", verbosity, 2)
            except:
                json_list = cast(List[Dict], [json_data])
                self._debug(f"json_list set to array: {json_list}", verbosity, 2)
        for json_object in json_list:
            self._debug(
                f"Export json {object_type} {json_object[key2]}...", verbosity, 0
            )
            self._debug(f"json {json_object}", verbosity, 2)
            try:
                with open(
                    f"{dst_path}{json_object[key2]}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(json_object, f, ensure_ascii=False, indent=4)
            except Exception as err:
                self._debug(
                    f"Exception on json {object_type} {json_object[key2]}: {err}.",
                    verbosity,
                    0,
                )
        return True

    def list_objects(self, object_type: str, verbosity: int = 0) -> List[Dict]:
        """List available objects for given type"""

        response_json = self._execute_api_request(object_type, verbosity=verbosity)

        return response_json

    def list_objects2df(self, object_type: str, verbosity: int = 0) -> pandas.DataFrame:
        """
        List available objects for given type
        Output: Dataframe
        """
        key_mapping = {
            "dashboards": "dashboards",
            "users": "data",
            "alerts/destinations": 0,
            "alerts/templates": 0,
        }
        key = key_mapping.get(object_type, "list")

        res_json = self.list_objects(object_type=object_type, verbosity=verbosity)

        return pandas.json_normalize(res_json[key])  # type: ignore[index,call-overload]

    def config_export(
        self,
        file_path: str,
        verbosity: int = 0,
        *,
        outformat: str = "json",
        split: bool = False,
        flat: bool = False,
    ):
        """Export OpenObserve configuration to json/csv/xlsx"""

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
        if outformat in ("csv", "xlsx"):
            # Collect all data
            data = {
                name: self.list_objects2df(api_path, verbosity=verbosity)
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

            if split is True and flat is False:
                # split json
                data = {
                    name: [
                        api_path,
                        self.list_objects(api_path, verbosity=verbosity),
                    ]  # type:ignore[misc]
                    for name, api_path in object_types.items()
                }

                for name, object_data in data.items():
                    self.export_objects_split(
                        object_data[0],  # type:ignore[arg-type]
                        object_data[1],  # type:ignore[arg-type]
                        file_path,
                        verbosity=verbosity,
                    )
            elif split is True and flat is True:
                print("FIXME! Not implemented")
                sys.exit(1)
            else:
                data = {
                    name: self.list_objects(
                        api_path, verbosity=verbosity
                    )  # type:ignore[misc]
                    for name, api_path in object_types.items()
                }

                for name, object_data in data.items():
                    with open(f"{file_path}{name}.json", "w", encoding="utf-8") as f:
                        json.dump(object_data, f, ensure_ascii=False, indent=4)

    def create_object(self, object_type: str, object_json: dict, verbosity: int = 0):
        """Create object"""
        url = self.openobserve_url.replace("[STREAM]", object_type)
        self._debug(f"Create object {object_type} url: {url}", verbosity, level=1)
        self._debug(f"Create object json input: {object_json}", verbosity, level=2)

        res = requests.post(
            url,
            json=object_json,
            headers=self.headers,
            verify=self.verify,
            timeout=self.timeout,
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
            url,
            json=object_json,
            headers=self.headers,
            verify=self.verify,
            timeout=self.timeout,
        )
        self._debug(f"Return {res.status_code}. Text: {res.text}", verbosity, level=3)
        self._handle_response(res, f"update_object_{object_type}")

        self._debug("Update object completed", verbosity)
        return True

    def import_objects_split(
        self,
        object_type: str,
        json_data: dict,
        file_path: str,
        *,
        overwrite: bool = False,
        verbosity: int = 0,
    ) -> bool:
        """
        Import OpenObserve configuration from split json files
        """
        key2 = "name"
        if object_type == "dashboards":
            key2 = "dashboard_id"
        file = Path(file_path)
        if (json_data is None or not json_data) and file.exists():
            with open(file_path, "r", encoding="utf-8") as json_file:
                self._debug(
                    f"Load json data to import from file {file_path}",
                    verbosity,
                    level=0,
                )
                json_data = json.loads(json_file.read())
        elif json_data is None:
            self._debug(
                "Fatal! import_objects_split(): input json_data None and file_path not exist",
                verbosity,
                level=0,
            )
            return False
        self._debug(f"json_data: {json_data}", verbosity, level=3)
        self._debug(
            f"Try to create {object_type} {json_data[key2]}...", verbosity, level=0
        )
        try:
            res = self.create_object(object_type, json_data, verbosity=verbosity)
            self._debug(f"Create returns {res}.", verbosity, level=0)

            if res:
                return res

            if overwrite:
                self._debug(
                    f"Overwrite enabled. Updating object {json_data[key2]}",
                    verbosity,
                    level=0,
                )
                res = self.update_object(object_type, json_data, verbosity=verbosity)
                self._debug(f"Update returns {res}.", verbosity, level=0)
                return res

        except Exception as exc:
            raise Exception(f"Exception: {exc}") from exc
        return False

    # pylint: disable=too-many-locals
    def import_objects(
        self,
        object_type: str,
        file_path: str,
        *,
        overwrite: bool = False,
        verbosity: int = 0,
        split: bool = False,
    ) -> bool:
        """Import objects from json file
        Note: API does not import list of objects, need to do one by one.
        FIXME! dashboards are always imported as new creating duplicates. no idempotence.
        """
        # Determine key mappings based on object type
        key_mappings = {
            "dashboards": ("dashboards", "dashboardId"),
            "users": ("data", "email"),
            "alerts/destinations": (None, "name"),
            "alerts/templates": (None, "name"),
        }
        list_key, id_key = key_mappings.get(object_type, ("list", "name"))

        if split is True:
            self._debug(
                f"import_objects: search files in {file_path}", verbosity, level=2
            )
            # functions
            # for file in glob.iglob(file_path + "functions/*.json"):
            for file in os.listdir(f"{file_path}"):
                if not file.endswith(".json"):
                    continue
                self._debug(f"import_objects: file {file}", verbosity, level=1)
                self.import_objects_split(
                    object_type,
                    {},
                    f"{file_path}/{file}",
                    overwrite=overwrite,
                    verbosity=verbosity,
                )
            return True

        with open(file_path, "r", encoding="utf-8") as json_file:
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
        *,
        overwrite: bool = False,
        verbosity: int = 0,
        split: bool = False,
    ):
        """Import OpenObserve configuration from json files"""

        if object_type == "all" and split is True:
            importable_types = [
                "functions",
                "pipelines",
                "alerts",
                "alerts/destinations",
                "alerts/templates",
                "dashboards",
                # 'streams' and 'users' are not supported by the API
                # No CreateStream, only CreateStreamSettings
                # self.import_objects('streams', f"{file_path}streams.json", overwrite, verbosity)
                # "Return 400. Text:
                # Json deserialize error: missing field `password` at line 1" = Extra field required
                # self.import_objects('users', f"{file_path}users.json", overwrite, verbosity)
            ]

            for item in importable_types:
                file_suffix = item.replace("/", "-")
                self.import_objects(
                    item,
                    f"{file_path}{item}",
                    overwrite=overwrite,
                    verbosity=verbosity,
                    split=split,
                )

        elif object_type == "all":
            importable_types = [
                "functions",
                "pipelines",
                "alerts",
                "alerts/destinations",
                "alerts/templates",
                "dashboards",
            ]

            for item in importable_types:
                file_suffix = item.replace("/", "-")
                self.import_objects(
                    item,
                    f"{file_path}{file_suffix}.json",
                    overwrite=overwrite,
                    verbosity=verbosity,
                )
        else:
            self.import_objects(
                object_type, file_path, overwrite=overwrite, verbosity=verbosity
            )
