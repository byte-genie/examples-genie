"""
Utility functions for byte-genie API
"""

import os
import json
import time
import requests
import inspect
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_random_exponential, wait_fixed, wait_exponential
import utils.common

## read secrets
SECRETS = utils.common.read_secrets()


class ByteGenie:

    def __init__(
            self,
            api_url: str = 'https://api.esgnie.com/execute',
            secrets_file: str = 'secrets.json',
            task_mode: str = 'sync',
            calc_mode: str = 'async',
            return_data: int = 1,
            overwrite: int = 0,
            verbose: int = 1,
    ):
        """
        :param api_url: byte-genie api url
        :param secrets_file: json file path containing secrets, including byte-genie api key
        :param BYTE_GENIE_KEY_name: key name of byte-genie api in secrets_file
        :param task_mode: task mode ('sync', 'async')
        :param calc_mode: calculation mode ('sync', 'async', 'parallel')
        :param BYTE_GENIE_KEY: api key for byte-genie API
        :param return_data: whether to return output data, or just the output file path containing the data
        :param overwrite: whether to overwrite the output, if it already exists
        :param verbose: whether to write logs from the task or not
        """
        self.api_url = api_url
        self.secrets_file = secrets_file
        self.task_mode = task_mode
        self.calc_mode = calc_mode
        self.return_data = return_data
        self.overwrite = overwrite
        self.verbose = verbose
        self.api_key = self.read_api_key()

    def read_api_key(self):
        filename = os.path.join(self.secrets_file)
        try:
            with open(filename, mode='r') as f:
                secrets = json.loads(f.read())
                api_key = secrets['BYTE_GENIE_KEY']
        except FileNotFoundError:
            api_key = ''
        return api_key

    def create_api_payload(
            self,
            func: str,
            args: dict,
    ):
        """
        Create payload for byte-genie API
        :param func: function/api-endpoint to call
        :param args: arguments for the function
        :return:
        """
        payload = {
            "api_key": self.api_key,
            "tasks": {
                'task_1': {
                    'func': func,
                    'args': args,
                    'overwrite': self.overwrite,
                    'return_data': self.return_data,
                    'verbose': self.verbose,
                    'task_mode': self.task_mode,
                    'calc_mode': self.calc_mode,
                },
            }
        }
        return payload

    def set_headers(self):
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Authorization": "Basic ZTgxMDg5NGY4NWNkNmU5ODc1ZDNiZjY1ODc0ZmExYjk6YjY4YmQ5ZTgwMTgxMzJiZGEyODNhZmZmOWFlNDY5NzU=",
            "Connection": "keep-alive",
            "Content-Type": "application/json"
        }
        return headers

    @retry(wait=wait_exponential(multiplier=1, min=5, max=120), stop=stop_after_attempt(5))
    def call_api(self, payload: dict, method: str = 'POST', timeout: int = 15 * 60):
        headers = self.set_headers()
        response = requests.request(
            method=method,
            url=self.api_url,
            headers=headers,
            json=payload,
            timeout=timeout,
        )
        try:
            json_resp = response.json()
        except Exception as e:
            json_resp = {'payload': payload, 'error': e}
        return json_resp

    def generate_metadata(
            self,
            data: list,
            data_context: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Generates meta-data for input data
        :param data: input data for which to generate meta-data
        :param data_context: a brief context of the data to be passed to the model generating meta-data
        :param timeout: timeout value for the API call
        :return: a json list
        """
        func = 'generate_metadata'
        args = {
            'data': data,
            'data_context': data_context
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def filter_columns(
            self,
            metadata: list,
            query: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Filter relevant columns in a data based on a query and meta-data
        :param metadata: meta-data containing column descriptions
        :param query: query to run on the data
        :param timeout: timeout value for api call
        :return:
        """
        func = 'filter_columns'
        args = {
            'metadata': metadata,
            'query': query
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def filter_data(
            self,
            data: list,
            query: str = None,
            method: str = 'query-relevance',
            timeout: int = 15 * 60,
    ):
        """
        Filter relevant columns in a data based on a query and meta-data
        :param data: data to be filtered
        :param query: query to run on the data
        :param method: filtering method ('query-relevance', 'one-step', 'multi-step')
        :param timeout: timeout value for api call
        :return:
        """
        func = 'filter_data'
        args = {
            'data': data,
            'query': query,
            'method': method,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def aggregate_data(
            self,
            data: list,
            query: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Aggregate/summarise data based on input query, to make it easier to answer the query correctly
        :param data: data to be aggregated
        :param query: query to run on the data
        :param timeout: timeout value for api call
        :return:
        """
        func = 'aggregate_data'
        args = {
            'data': data,
            'query': query,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def standardise_data(
            self,
            data: list,
            cols_to_std: list = None,
            groupby_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Standardise data tablet
        :param data: data to be standardised
        :param cols_to_std: list of columns to standardise (if None, will consider all columns in standardisation)
        :param groupby_cols: list of columns to group data by; each group will be standardised independently
        :param timeout: timeout value for api call
        :return:
        """
        func = 'standardise_data'
        args = {
            'text_data': data,
            'cols_to_std': cols_to_std,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def standardise_names(
            self,
            data: list,
            text_col: str,
            groupby_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Standardise text values in a single column in the data
        :param data: data to be standardised
        :param text_col: name of the column to be standardised
        :param groupby_cols: columns to group the data by when standaridsing text_col; each group will be standardised independently
        :param timeout: timeout value for api call
        :return:
        """
        func = 'standardise_names'
        args = {
            'text_data': data,
            'text_col': text_col,
            'groupby_cols': groupby_cols,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def create_query_variants(
            self,
            metadata: list,
            query: str,
            n_variants: int = 3,
            timeout: int = 15 * 60,
    ):
        """
        Generate variants of input query
        :param metadata: meta-data of the data on which to run queries
        :param query: original query
        :param n_variants: number of variants to generate
        :param timeout: timeout value for api call
        :return:
        """
        func = 'standardise_names'
        args = {
            'metadata': metadata,
            'query': query,
            'n_variants': n_variants,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def create_dataset(
            self,
            data: list,
            attrs: list,
            cols_to_use: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Create a new dataset with desired features from input data
        :param data: input data
        :param attrs: attributes/columns to have in the new data
        :param cols_to_use: columns to consider in create the new dataset
        :param timeout: timeout value for the api call
        :return:
        """
        func = 'create_dataset'
        args = {
            'data': data,
            'attrs': attrs,
            'cols_to_use': cols_to_use,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp





