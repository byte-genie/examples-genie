"""
Utility functions for byte-genie API
"""

import os
import json
import time
import inspect
import requests
import numpy as np
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_random_exponential, wait_fixed, wait_exponential


class ByteGenie:

    def __init__(
            self,
            api_url: str = 'https://api.esgnie.com/execute',
            secrets_file: str = 'secrets.json',
            task_mode: str = 'sync',
            calc_mode: str = 'async',
            return_data: int = 1,
            overwrite: int = 0,
            overwrite_base_output: int = 0,
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
        :param overwrite: whether to overwrite the immediate task output, if it already exists
        :param overwrite_base_output: whether to overwrite the base task output, if it already exists
        :param verbose: whether to write logs from the task or not
        """
        self.api_url = api_url
        self.secrets_file = secrets_file
        self.task_mode = task_mode
        self.calc_mode = calc_mode
        self.return_data = return_data
        self.overwrite = overwrite
        self.overwrite_base_output = overwrite_base_output
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
                    'overwrite_base_output': self.overwrite_base_output,
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

    def get_response_data(
            self,
            resp: dict
    ):
        """
        Get output data from byte-genie API response. Returns None if no data is found (e.g. if the task is scheduled)
        :param resp:
        :return:
        """
        if not isinstance(resp, dict):
            raise ValueError('resp must be a dictionary')
        if 'response' in resp.keys():
            resp = resp['response']
            if isinstance(resp, dict):
                if 'task_1' in resp.keys():
                    resp = resp['task_1']
                    if isinstance(resp, dict):
                        if 'data' in resp.keys():
                            resp = resp['data']
                            for i in np.arange(0, 2, 1):
                                if isinstance(resp, dict):
                                    if 'data' in resp.keys():
                                        resp = resp['data']
                            return resp

    def get_response_output_file(
            self,
            resp: dict
    ):
        output_file = ''
        if not isinstance(resp, dict):
            raise ValueError('resp must be a dictionary')
        if 'response' in resp.keys():
            resp = resp['response']
        if isinstance(resp, dict):
            if 'task_1' in resp.keys():
                resp = resp['task_1']
        if isinstance(resp, dict):
            if 'task' in resp:
                resp = resp['task']
        if isinstance(resp, dict):
            if 'output_file' in resp:
                output_file = resp['output_file']
        if output_file == '':
            raise ValueError('No output_file found in response')
        else:
            return output_file

    def slugify(
            self,
            text: str,
            timeout: int = 15 * 60,
    ):
        """
        Slugify text
        :param text: text to slugify
        :param timeout:
        :return:
        """
        func = 'slugify'
        args = {
            'text': text,
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

    def list_doc_files(
            self,
            doc_name: str,
            file_pattern: str,
            timeout: int = 15 * 60,
    ):
        """
        List document files matching a file pattern
        :param doc_name: document name for which to list files
        :param file_pattern: file pattern to match when listing files
        :param timeout:
        :return:
        """
        func = 'list_doc_files'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
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

    def check_file_exists(
            self,
            file: str,
            timeout: int = 15 * 60,
    ):
        """
        Check if a file exists
        :param file: file to check
        :param timeout:
        :return:
        """
        func = 'check_file_exists'
        args = {
            'file': file,
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

    def read_file(
            self,
            file: str,
            timeout: int = 15 * 60,
    ):
        """
        Read a file
        :param file: file to read
        :param timeout:
        :return:
        """
        func = 'read_file'
        args = {
            'file': file,
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

    def find_homepage(
            self,
            entity_names: list,
            timeout: int = 15 * 60,
    ):
        """
        Find homepages for a set of entity names
        :param entity_names: list of entity names for which to find homepages
        :param timeout:
        :return:
        """
        func = 'find_homepage'
        args = {
            'entity_names': entity_names,
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

    def search_web(
            self,
            keyphrases: list,
            site: str = '',
            max_pagenum: int = 2,
            timeout: int = 15 * 60,
    ):
        """
        Search web for a given list of keyphrases from a given website
        :param keyphrases: list of keyphrases to search
        :param site: site to search (optional)
        :param max_pagenum: maximum number of pages to keep in search results
        :param timeout:
        :return:
        """
        func = 'search_web'
        args = {
            'keyphrases': keyphrases,
            'site': site,
            'max_pagenum': max_pagenum
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

    def download_file(
            self,
            urls: list,
            timeout: int = 15 * 60,
    ):
        """
        Download URL content as file
        :param urls: list of URLs to download
        :param timeout:
        :return:
        """
        func = 'download_file'
        args = {
            'urls': urls,
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

    def download_documents(
            self,
            entity_names: list,
            doc_keywords: list,
            timeout: int = 15 * 60,
    ):
        """
        Search and download documents matching given keywords from an entity's homepage
        :param entity_names: list of entities for which to find documents
        :param doc_keywords: list of keywords for which to search documents
        :param timeout:
        :return:
        """
        func = 'download_documents'
        args = {
            'entity_names': entity_names,
            'doc_keywords': doc_keywords,
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

    def extract_doc_year(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Extract document year
        :param doc_name: document name for which to extract info
        :param timeout:
        :return:
        """
        func = 'extract_doc_year'
        args = {
            'doc_name': doc_name,
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

    def extract_doc_info(
            self,
            doc_name: str,
            doc_type_choices: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Extract document information, including document year, author, organisation, type, number of pages, etc.
        :param doc_name: document name for which to extract info
        :param doc_type_choices: possible document types in which to classify the document
        :param timeout:
        :return:
        """
        func = 'extract_doc_info'
        args = {
            'doc_name': doc_name,
            'doc_type_choices': doc_type_choices,
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
            groupby_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Create a new dataset with desired features from input data
        :param data: input data
        :param attrs: attributes/columns to have in the new data
        :param cols_to_use: columns to consider in create the new dataset
        :param groupby_cols: columns to group data by
        :param timeout: timeout value for the api call
        :return:
        """
        func = 'create_dataset'
        args = {
            'data': data,
            'attrs': attrs,
            'cols_to_use': cols_to_use,
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

    def parse_numeric_string(
            self,
            text: str,
            method: str = '',
            timeout: int = 15 * 60,
    ):
        """
        Parse numeric string to get numeric value
        :param text: text to parse into numeric value
        :param method: method to use for parsing ('llm-first', or '')
        :param timeout:
        :return:
        """
        func = 'parse_numeric_string'
        args = {
            'text': text,
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

    def structure_quants_pipeline(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Trigger quant-structuring pipeline, which extracts text and quant metrics, and structures all quant metrics
        from passages and tables
        :param doc_name: document name for which to trigger quant extraction
        :param timeout:
        :return:
        """
        func = 'structure_quants_pipeline'
        args = {
            'doc_name': doc_name,
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

    def write_ranked_data(
            self,
            doc_name: str,
            attr: str,
            attr_type: str = None,
            frac_rows_to_keep: float = 0.1,
            timeout: int = 15 * 60,
    ):
        """
        Rank document data by relevance to an attribute
        :param doc_name: document from which to rank data
        :param attr: keyphrase by which to rank data
        :param attr_type: type of attribute/data to rank (quantitative or qualitative)
        :param timeout: how long to wait before timing out the api call
        :return:
        """
        func = 'write_ranked_data'
        args = {
            'doc_name': doc_name,
            'attr': attr,
            'attr_type': attr_type,
            'frac_rows_to_keep': frac_rows_to_keep,
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







