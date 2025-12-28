"""
OpenObserve API module

SPDX-FileCopyrightText: 2025 The python_openobserve authors
SPDX-License-Identifier: GPL-3.0-or-later
"""

# pylint: disable=too-many-arguments,bare-except,broad-exception-raised,broad-exception-caught,too-many-public-methods,too-many-lines
import base64
import json

# import glob
import os
import sys
import re
from datetime import datetime
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

try:
    import polars

    HAVE_MODULE_POLARS = True
except ImportError:
    print("Can't import polars. some functions may be unavailable.")
    HAVE_MODULE_POLARS = False

try:
    import fireducks  # type: ignore

    HAVE_MODULE_FIREDUCKS = True
except ImportError:
    print("Can't import fireducks. Some functions may be unavailable.")
    HAVE_MODULE_FIREDUCKS = False

key_mapping = {
    "dashboards": "dashboards",
    "users": "data",
    "alerts/destinations": 0,
    "alerts/templates": 0,
}
# for deleting. different at create time...
id_mapping = {
    "alerts": "alert_id",
    "alerts/destinations": "destination_name",
    "alerts/templates": "template_name",
    "dashboards": "dashboardId",
    "functions": "name",
    "pipelines": "pipeline_id",
    "streams": "stream_name",
    "users": "email_id",
}
name_mapping = {
    "alerts": "name",
    "alerts/destinations": "name",
    "alerts/templates": "template_name",
    "dashboards": "title",
    "functions": "name",
    # special handling directly in import_objects_split()
    "pipelines": "source.stream_name",
    "streams": "name",
    "users": "email",
}


def flatten(dictionary: dict, parent_key="", separator="."):
    """Flatten dictionary

    Args:
      dictionary: input dictionary to flatten
    """
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        items.append((new_key, value))
    return dict(items)


def is_ksuid(input_string: str) -> bool:
    """Is input a ksuid?

    Args:
      input_string: string to validate
    """
    if re.match(r"^[a-zA-Z0-9]{27}$", input_string):
        return True
    return False


def is_name(input_string: str) -> bool:
    """Is input a valid name aka alert_name?

    Args:
      input_string: string to validate
    """
    if re.match(r"^[^:#\?&%\"'\s]+$", input_string):
        return True
    return False


class OpenObserve:
    """
    OpenObserve class based on OpenObserve REST API
    """

    def __init__(
        self,
        user: str,
        password: str,
        *,
        organisation: str = "default",
        host: str = "http://localhost:5080",
        verify: bool = True,
        timeout: int = 10,
    ) -> None:
        """Class __init__

        Args:
          user: openobserve instance username
          password: openobserve instance matching password
          organisation: openobserve instance organisation (default, _meta...)
          host: url of openobserve instance
          verify: validate certificate
          timeout: default http timeout
        """
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

        Args:
          sql: input sql query
          start_time: start of search interval, either datetime, either int/epoch
          end_time: end of search interval, either datetime, either int/epoch
          verbosity: how verbose to run from 0/less to 5/more
          timeout: http timeout
          timestamp_conversion_auto: try to convert automatically column containing time as name
          timestamp_columns: convert given columns to timestamp
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
        if endpoint in ("alerts", "folders", "folders/alerts", "folders/dashboards"):
            url = url.replace("/api", "/api/v2")
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
        OpenObserve search function with pandas dataframe output

        Args:
          sql: input sql query
          start_time: start of search interval, either datetime, either int/epoch
          end_time: end of search interval, either datetime, either int/epoch
          verbosity: how verbose to run from 0/less to 5/more
          timeout: http timeout
          timestamp_conversion_auto: try to convert automatically column containing time as name
          timestamp_columns: convert given columns to timestamp
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

    def search2df_polars(
        self,
        sql: str,
        *,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
        timeout: int = 300,
        timestamp_conversion_auto: bool = False,
        timestamp_columns: Union[List[str], None] = None,
    ) -> polars.DataFrame:
        """
        OpenObserve search function with polars dataframe output

        Args:
          sql: input sql query
          start_time: start of search interval, either datetime, either int/epoch
          end_time: end of search interval, either datetime, either int/epoch
          verbosity: how verbose to run from 0/less to 5/more
          timeout: http timeout
          timestamp_conversion_auto: try to convert automatically column containing time as name
          timestamp_columns: convert given columns to timestamp
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
        df_res = polars.json_normalize(res_json_hits)

        if timestamp_columns is not None:
            for col in list(
                set(df_res.columns) & set(["_timestamp"] + timestamp_columns)
            ):
                try:
                    # ensure timestamp format
                    if col in ["_timestamp"] + timestamp_columns:
                        df_res[col] = polars.to_datetime(df_res[col])
                except Exception as err:
                    raise Exception(
                        err,
                        "query",
                        f"query column type conversion: {col} -> {df_res[col]}",
                    ) from err
        return df_res

    def search2df_fireducks(
        self,
        sql: str,
        *,
        start_time: Union[datetime, int] = 0,
        end_time: Union[datetime, int] = 0,
        verbosity: int = 0,
        timeout: int = 300,
        timestamp_conversion_auto: bool = False,
        timestamp_columns: Union[List[str], None] = None,
    ) -> fireducks.core.pandas.DataFrame:
        """
        OpenObserve search function with fireducks df output

        Args:
          sql: input sql query
          start_time: start of search interval, either datetime, either int/epoch
          end_time: end of search interval, either datetime, either int/epoch
          verbosity: how verbose to run from 0/less to 5/more
          timeout: http timeout
          timestamp_conversion_auto: try to convert automatically column containing time as name
          timestamp_columns: convert given columns to timestamp
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
        df_res = fireducks.core.pandas.json_normalize(res_json_hits)

        if timestamp_columns is not None:
            for col in list(
                set(df_res.columns) & set(["_timestamp"] + timestamp_columns)
            ):
                try:
                    # ensure timestamp format
                    if col in ["_timestamp"] + timestamp_columns:
                        df_res[col] = fireducks.pandas.to_datetime(df_res[col])
                except Exception as err:
                    raise Exception(
                        err,
                        "query",
                        f"query column type conversion: {col} -> {df_res[col]}",
                    ) from err
        return df_res

    # pylint: disable=too-many-branches,too-many-locals
    def export_objects_split(
        self,
        object_type: str,
        json_data: list[dict],
        file_path: str,
        *,
        verbosity: int = 0,
        flat: bool = False,
        strip: bool = False,
    ):
        """
        Export OpenObserve json configuration to split json files

        Args:
          object_type: what kind of openobserve object to export
          json_data: object data to be exported
          file_path: target file path or prefix
          verbosity: how verbose to run from 0/less to 5/more
          flat: put all files in a flat directory or tree hierarchy
          strip: remove variables data like stats or updated_at fields
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
            if strip:
                keys_to_remove = [
                    # alerts
                    "last_triggered_at",
                    "last_satisfied_at",
                    "updated_at",
                    "last_edited_by",
                    # streams
                    "stats",
                ]
                # data = json.loads(json_object)
                data2 = {
                    k: v for k, v in json_object.items() if k not in keys_to_remove
                }
                # json_object = json.dumps(data2)
                json_object = data2
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
        key = key_mapping.get(object_type, "list")

        res_json = self.list_objects(object_type=object_type, verbosity=verbosity)

        if object_type in ["alerts/destinations", "alerts/templates"]:
            return pandas.json_normalize(res_json)
        if key in res_json:
            return pandas.json_normalize(res_json[key])  # type: ignore[index,call-overload]

        raise Exception(
            (
                f"list_objects2df: can't normalize data {res_json} "
                f"for object type {object_type} and key {key}"
            )
        )

    def config_export(
        self,
        file_path: str,
        verbosity: int = 0,
        *,
        outformat: str = "json",
        split: bool = False,
        flat: bool = False,
        strip: bool = False,
    ):
        """Export OpenObserve configuration aka all object types to json/csv/xlsx

        Args:
          file_path: target file path or prefix
          verbosity: how verbose to run from 0/less to 5/more
          outformat: json, csv, or xlsx
          split: separate list of objects json in one file per object
          flat: put all files in a flat directory or tree hierarchy
          strip: remove variables data like stats or updated_at fields
        """

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
                        strip=strip,
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
        """Create object

        Args:
          object_type: what kind of openobserve object to export
          object_json: json source object to create
          verbosity: how verbose to run from 0/less to 5/more
        """
        url = self.openobserve_url.replace("[STREAM]", object_type)
        if object_type in ("alerts", "folders", "folders/alerts", "folders/dashboards"):
            url = url.replace("/api", "/api/v2")
        if object_type == "alerts" and "folder_id" in object_json:
            url = f"{url}?folder={object_json['folder_id']}"
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
        """Update object

        Args:
          object_type: what kind of openobserve object to export
          object_json: json source object to create
          verbosity: how verbose to run from 0/less to 5/more
        """
        key_id = id_mapping.get(object_type, "id")
        url = self.openobserve_url.replace(
            "[STREAM]", f"{object_type}/{object_json[key_id]}"
        )
        if object_type in ("alerts", "folders", "folders/alerts", "folders/dashboards"):
            url = url.replace("/api", "/api/v2")
        if object_type == "alerts" and "folder_id" in object_json:
            url = f"{url}?folder={object_json['folder_id']}"
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

    def create_update_object_by_name(
        self,
        object_type: str,
        object_json: dict,
        verbosity: int = 0,
        overwrite: bool = False,
    ):
        """Create/Update object by name
        It will first list all objects of given type and erase all those matching exact name.

        Args:
          object_type: what kind of openobserve object
          object_json: json source object to create/update
          verbosity: how verbose to run from 0/less to 5/more
          overwrite: overwrite an existing object - known upstream bug
        """
        key = key_mapping.get(object_type, "list")
        key_id = id_mapping.get(object_type, "id")
        key_name = name_mapping.get(object_type, "name")
        object_name = object_json[key_name]
        count_update = 0
        current = self.list_objects(object_type, verbosity)
        self._debug(f"Create/Update by name objects list: {current}", verbosity, 4)
        for obj in current[key]:  # type:ignore[call-overload]
            if key_name in obj and object_name.strip() == obj[key_name].strip():
                if overwrite:
                    object_json[key_id] = obj[key_id]
                    self._debug(
                        f"Create/Update by name matching object: {obj}", verbosity, 3
                    )
                    self.update_object(object_type, object_json, verbosity)
                    count_update += 1
                    break

                self._debug("  .. matching object but overwrite is false", verbosity, 1)
                return False
        self._debug(
            f"Create/update by name updated {count_update} object(s).", verbosity, 1
        )
        if count_update == 0:
            self.create_object(object_type, object_json, verbosity)
            self._debug("Create/update by name created 1 object(s).", verbosity, 1)
        return True

    def delete_object(self, object_type: str, object_id: str, verbosity: int = 0):
        """Delete object

        Args:
          object_type: what kind of openobserve object to export
          object_id: object id (sometimes name) to delete
          verbosity: how verbose to run from 0/less to 5/more
        """
        url = self.openobserve_url.replace("[STREAM]", f"{object_type}/{object_id}")
        if object_type in ("alerts", "folders", "folders/alerts", "folders/dashboards"):
            url = url.replace("/api", "/api/v2")
        self._debug(f"Delete object {object_type} url: {url}", verbosity, level=1)

        res = requests.delete(
            url,
            headers=self.headers,
            verify=self.verify,
            timeout=self.timeout,
        )
        self._debug(f"Return {res.status_code}. Text: {res.text}", verbosity, level=3)
        self._handle_response(res, f"delete_object_{object_type}")

        self._debug("Delete object completed", verbosity)
        return True

    def delete_object_by_name(
        self, object_type: str, object_name: str, verbosity: int = 0
    ):
        """Delete object by name
        It will first list all objects of given type and erase all those matching exact name.

        Args:
          object_type: what kind of openobserve object to export
          object_name: object name to delete
          verbosity: how verbose to run from 0/less to 5/more
        """
        key = key_mapping.get(object_type, "list")
        key_id = id_mapping.get(object_type, "id")
        key_name = name_mapping.get(object_type, "name")
        count_delete = 0
        current = self.list_objects(object_type, verbosity)
        self._debug(f"Delete by name objects list: {current}", verbosity, 3)
        for obj in current[key]:  # type:ignore[call-overload]
            if "name" in obj and object_name == obj[key_name]:
                self._debug(f"Delete by name matching object: {obj}", verbosity)
                self.delete_object(object_type, obj[key_id], verbosity)
                count_delete += 1
        self._debug(f"Delete by name deleted {count_delete} object(s).", verbosity, 1)
        return True

    def import_objects_split(
        self,
        object_type: str,
        json_data: dict,
        file_path: str,
        *,
        overwrite: bool = False,
        verbosity: int = 0,
        force: bool = False,
    ) -> bool:
        """
        Import OpenObserve configuration from split json files

        Args:
          object_type: what kind of openobserve object to import
          json_data: source json (this one or file_path, leave other empty string)
          file_path: source file path or prefix (json_data or this one)
          overwrite: overwrite an existing object - known upstream bug
          verbosity: how verbose to run from 0/less to 5/more
          force: skip controls like ksuid
        """
        key2 = name_mapping.get(object_type, "name")
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
        if (
            not force
            and object_type in ("alerts")
            and "id" in json_data
            and not is_ksuid(json_data["id"])
        ):
            raise Exception(f"Invalid input: {json_data['id']} is not a ksuid")
        if (
            not force
            and object_type in ("alerts")
            and key2 in json_data
            and not is_name(json_data[key2])
        ):
            raise Exception(f"Invalid input: {json_data[key2]} is not a valid name")
        if object_type == "pipelines":
            self._debug(
                f"Try to create {object_type} {json_data['source']['stream_name']}...",
                verbosity,
                level=0,
            )
        elif key2 in json_data:
            self._debug(
                f"Try to create {object_type} {json_data[key2]}...", verbosity, level=0
            )
        elif "name" in json_data:
            self._debug(
                f"Try to create {object_type} {json_data['name']}...",
                verbosity,
                level=0,
            )
        else:
            self._debug(f"Try to create {object_type}...", verbosity, level=0)
        try:
            res = self.create_object(object_type, json_data, verbosity=verbosity)
            self._debug(f"Create returns {res}.", verbosity, level=0)

            if res:
                return res

            if overwrite:
                self._debug(
                    "Overwrite enabled. Updating object",
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

        Args:
          object_type: what kind of openobserve object to import
          file_path: source file path or prefix (json_data or this one)
          overwrite: overwrite an existing object - known upstream bug
          verbosity: how verbose to run from 0/less to 5/more
          split: separate list of objects json in one file per object
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
        """Import OpenObserve configuration from json files

        Args:
          object_type: what kind of openobserve object to import
          file_path: source file path or prefix (json_data or this one)
          overwrite: overwrite an existing object - known upstream bug
          verbosity: how verbose to run from 0/less to 5/more
          split: separate list of objects json in one file per object
        """

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
