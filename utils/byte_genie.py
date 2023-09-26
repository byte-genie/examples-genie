"""
Utility functions for byte-genie API
"""

import os
import json
import time
import inspect
import requests
import numpy as np
from utils.logging import logger
from utils.async_utils import to_async
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_random_exponential, wait_fixed, wait_exponential


class ByteGenieResponse:

    def __init__(
            self,
            response: dict,
            verbose: int = 1,
    ):
        if not isinstance(response, dict):
            raise ValueError('response must be a dictionary')
        self.response = response
        self.verbose = verbose

    def get_status(self):
        """
        Get the status of the task.
        Note that this is the status of the task at the time API call was made.
        In case a task was scheduled initially, even when the task is complete, the output of this method will not change.
        For such tasks, use check_output_file_exists() to check whether a task has finished generating its output.
        :return:
        """
        status = 'scheduled'
        resp = self.response
        if 'response' in resp.keys():
            resp = resp['response']
            if isinstance(resp, dict):
                if 'task_1' in resp.keys():
                    resp = resp['task_1']
                    if isinstance(resp, dict):
                        if 'status' in resp.keys():
                            status = resp['status']
        return status

    def get_data(self):
        """
        Get data returned in ByteGenie response.
        Note that this method only gets data that was returned from the api call,
        and in case a task is scheduled, even when the task output is ready, output of this method will not change.
        Use read_output_data() to read the current output of such scheduled tasks.
        :return:
        """
        resp = self.response
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

    def get_output_file(
            self,
    ):
        """
        Get the output file of a task
        :return:
        """
        resp = self.response
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
                                    return output_file

    def check_output_file_exists(self):
        """
        Check if the output file exists.
        This is the recommended method to check if the output of a task is complete.
        :return:
        """
        bg = ByteGenie(
            task_mode='sync',
        )
        output_file = self.get_output_file()
        if output_file is not None:
            resp = bg.check_file_exists(output_file)
            file_exists = resp.get_data()
        else:
            file_exists = False
        return file_exists

    def read_output_data(self):
        """
        Read output data from the task output file.
        This is the recommended method to read output for tasks that were previously scheduled.
        :return:
        """
        bg = ByteGenie(
            task_mode='sync',
        )
        if self.check_output_file_exists():
            resp = bg.read_file(self.get_output_file())
            resp_data = resp.get_data()
            return resp_data
        else:
            logger.warning(f"output does not yet exist: wait some more")

    @to_async
    def async_read_output_data(self):
        try:
            resp = self.read_output_data()
            return resp
        except Exception as e:
            if self.verbose:
                logger.warning(f"Error in read_output_data(): {e}")


class ByteGenie:

    def __init__(
            self,
            api_url: str = 'https://api.esgnie.com/execute',
            secrets_file: str = 'secrets.json',
            task_mode: str = 'async',
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
        self.username = self.read_username()

    def read_api_key(self):
        filename = os.path.join(self.secrets_file)
        try:
            with open(filename, mode='r') as f:
                secrets = json.loads(f.read())
                api_key = secrets['BYTE_GENIE_KEY']
        except FileNotFoundError:
            api_key = ''
        return api_key

    def read_username(self):
        filename = os.path.join(self.secrets_file)
        try:
            with open(filename, mode='r') as f:
                secrets = json.loads(f.read())
                api_key = secrets['USERNAME']
        except FileNotFoundError:
            api_key = ''
        return api_key

    def create_api_payload(
            self,
            func: str,
            args: dict,
            cluster_args: dict = None,
    ):
        """
        Create payload for byte-genie API
        :param func: function/api-endpoint to call
        :param args: arguments for the function
        :param cluster_args: arguments for the type of cluster used to run the code
        :return:
        """
        if cluster_args is None:
            cluster_args = {}
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
                    'accelerators': cluster_args.get('accelerators'),
                    'n_cpu': cluster_args.get('n_cpu'),
                    'use_spot': cluster_args.get('use_spot'),
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
            ## convert to byte-genie resp
            bg_resp = ByteGenieResponse(json_resp)
        except Exception as e:
            json_resp = {'payload': payload, 'error': e}
            ## convert to byte-genie resp
            bg_resp = ByteGenieResponse(json_resp)
        return bg_resp

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

    def upload_data(
            self,
            contents: list,
            filenames: list,
            username: str,
            timeout: int = 15 * 60,
    ):
        """
        Upload files
        :param contents: file contents to upload
        :param filenames: names for uploaded file contents
        :param username: user name
        :param timeout:
        :return:
        """
        func = 'upload_data'
        args = {
            'contents': contents,
            'filenames': filenames,
            'username': username,
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

    @to_async
    def async_upload_data(
            self,
            contents: list,
            filenames: list,
            username: str,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.upload_data(
                contents=contents,
                filenames=filenames,
                username=username,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.info(f"Error in upload_data(): {e}")

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

    @to_async
    def async_list_doc_files(
            self,
            doc_name: str,
            file_pattern: str,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.list_doc_files(
                doc_name=doc_name,
                file_pattern=file_pattern,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                print(f"Error in list_doc_files(): {e}")

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

    @to_async
    def async_read_file(
            self,
            file: str,
            timeout: int = 15 * 60,
    ):
        """
        Read a file (asynchronous)
        :param file: file to read
        :param timeout:
        :return:
        """
        try:
            resp = self.read_file(
                file=file,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in read_file(): {e}")


    def read_synthesized_data(
            self,
            doc_name: str,
            data_type: str = 'quants',
            drop_embedding: bool = True,
            timeout: int = 15 * 60,
    ):
        """
        Read synthesized data, which synthesizes document-level info, with specific info extracted from the document
        :param doc_name: document name
        :param data_type: data type to read ('quants' for quantitative data, or 'text' for text data)
        :param drop_embedding: whether to drop embedding column or not when returning the data; embedding column contains embeddings for extracted text (useful for semantic search)
        :param timeout: time out for the api call
        :return:
        """
        func = 'read_synthesized_data'
        args = {
            'doc_name': doc_name,
            'data_type': data_type,
            'drop_embedding': drop_embedding,
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

    @to_async
    def async_extract_doc_info(
            self,
            doc_name: str,
            doc_type_choices: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.extract_doc_info(
                doc_name=doc_name,
                doc_type_choices=doc_type_choices,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                print(f"Error in extract_doc_info(): {e}")

    def extract_text_years(
            self,
            text: str,
            output_format: str = 'cleaned',
            timeout: int = 15 * 60,
    ):
        """
        Extract years from text
        :param text: text from which to extract years
        :param output_format: if output_format is 'cleaned', the years will be returned in a clean YYYY format
        :param timeout:
        :return:
        """
        func = 'extract_text_years'
        args = {
            'text': text,
            'output_format': output_format,
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
        func = 'create_query_variants'
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

    def rank_answers_to_query(
            self,
            data: list,
            query: str,
            answers: list,
            timeout: int = 15 * 60,
    ):
        """
        Rank candidate answers to a given query, in the context of an input data
        :param data: data to use as context to rank answers
        :param query: input query to answer
        :param answers: candidate answers to rank
        :param timeout:
        :return:
        """
        func = 'rank_answers_to_query'
        args = {
            'data': data,
            'query': query,
            'answers': answers,
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

    def write_pdf_img(
            self,
            doc_name: str,
            dpi: int = 500,
            timeout: int = 5 * 60,
    ):
        """
        Take page images from a PDF file
        :param doc_name: document name for which to write mages
        :param dpi: dots per inch
        :param timeout:
        :return:
        """
        func = 'write_pdf_img'
        args = {
            'doc_name': doc_name,
            'dpi': dpi,
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

    @to_async
    def async_write_pdf_img(
            self,
            doc_name: str,
            dpi: int = 500,
            timeout: int = 5 * 60,
    ):
        try:
            resp = self.write_pdf_img(
                doc_name=doc_name,
                dpi=dpi,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.info(f"Error in write_pdf_img(): {e}")

    def extract_text(
            self,
            doc_name: str,
            file_pattern: str = '*.png',
            timeout: int = 15 * 60,
    ):
        """
        Extract text from images
        :param doc_name: document name for which to extract text
        :param file_pattern: file pattern to match for input files
        :param timeout:
        :return:
        """
        func = 'extract_text'
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

    def async_extract_text(
            self,
            doc_name: str,
            file_pattern: str = '*.png',
            timeout: int = 15 * 60,
    ):
        """
        Extract text from images (asynchronous)
        :param doc_name: document name for which to extract text
        :param file_pattern: file pattern to match for input files
        :param timeout:
        :return:
        """
        try:
            resp = self.extract_text(
                doc_name=doc_name,
                file_pattern=file_pattern,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in extract_text(): {e}")

    def segment_text(
            self,
            doc_name: str,
            file_pattern: str = 'variable_desc=text-blocks/**.csv',
            timeout: int = 15 * 60,
    ):
        """
        Segment extracted text using OCR
        :param doc_name: document name for which to segment text
        :param file_pattern: file pattern to match for input files
        :param timeout:
        :return:
        """
        func = 'segment_text'
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

    def async_segment_text(
            self,
            doc_name: str,
            file_pattern: str = 'variable_desc=text-blocks/**.csv',
            timeout: int = 15 * 60,
    ):
        """
        Segment extracted text using OCR (asynchronous)
        :param doc_name: document name for which to segment text
        :param file_pattern: file pattern to match for input files
        :param timeout:
        :return:
        """
        try:
            resp = self.segment_text(
                doc_name=doc_name,
                file_pattern=file_pattern,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in segment_text(): {e}")

    def convert_pdf_to_markdown(
            self,
            doc_name: str,
            cluster_args: dict = None,
            timeout: int = 15 * 60,
    ):
        """
        Convert pdf documents to markdown
        :param doc_name: document name to be converted to latex
        :param cluster_args: cluster specifications
        :param timeout:
        :return:
        """
        func = 'convert_pdf_to_markdown'
        args = {
            'doc_name': doc_name,
        }
        payload = self.create_api_payload(
            func=func,
            args=args,
            cluster_args=cluster_args,
        )
        resp = self.call_api(
            payload=payload,
            timeout=timeout,
        )
        return resp

    def async_convert_pdf_to_markdown(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Convert pdf documents to markdown
        :param doc_name: document name to be converted to latex
        :param timeout:
        :return:
        """
        try:
            resp = self.convert_pdf_to_markdown(
                doc_name=doc_name,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in convert_pdf_to_markdown(): {e}")

    def reconstruct_orig_tables(
            self,
            doc_name: str,
            file_pattern: str = 'variable_desc=table-cells/**.csv',
            timeout: int = 15 * 60,
    ):
        """
        Reconstructs original tables form table cells files
        :param doc_name: document name for which to reconstruct original tables
        :param file_pattern: file pattern to seelct input files
        :param timeout:
        :return:
        """
        func = 'reconstruct_orig_tables'
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

    def translate_text_pipeline(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Trigger text translation pipeline, which extracts text and tables, and translates them
        :param doc_name: document name for which to trigger translation pipeline
        :param timeout:
        :return:
        """
        func = 'translate_text_pipeline'
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

    def structure_passage_quants(
            self,
            doc_name: str,
            file_pattern: str = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            text_col: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Structure quants contained in text passages extracted from documents
        :param doc_name:
        :param file_pattern:
        :param base_attrs:
        :param base_attr_names:
        :param custom_attrs:
        :param custom_attr_names:
        :param text_col: column of the column containing text
        :param timeout:
        :return:
        """
        func = 'structure_passage_quants'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'base_attrs': base_attrs,
            'base_attr_names': base_attr_names,
            'custom_attrs': custom_attrs,
            'custom_attr_names': custom_attr_names,
            'text_col': text_col,
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

    @to_async
    def async_structure_passage_quants(
            self,
            doc_name: str,
            file_pattern: str = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            text_col: str = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.structure_passage_quants(
                doc_name=doc_name,
                file_pattern=file_pattern,
                base_attrs=base_attrs,
                base_attr_names=base_attr_names,
                custom_attrs=custom_attrs,
                custom_attr_names=custom_attr_names,
                text_col=text_col,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in structure_passage_quants: {e}")

    def structure_tabular_quants(
            self,
            doc_name: str,
            file_pattern: str = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Structure quants contained in tables extracted from documents
        :param doc_name:
        :param file_pattern:
        :param base_attrs:
        :param base_attr_names:
        :param custom_attrs:
        :param custom_attr_names:
        :param timeout:
        :return:
        """
        func = 'structure_tabular_quants'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'base_attrs': base_attrs,
            'base_attr_names': base_attr_names,
            'custom_attrs': custom_attrs,
            'custom_attr_names': custom_attr_names,
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

    @to_async
    def async_structure_tabular_quants(
            self,
            doc_name: str,
            file_pattern: str = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.structure_tabular_quants(
                doc_name=doc_name,
                file_pattern=file_pattern,
                base_attrs=base_attrs,
                base_attr_names=base_attr_names,
                custom_attrs=custom_attrs,
                custom_attr_names=custom_attr_names,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in structure_tabular_quants: {e}")

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

    def rank_data(
            self,
            doc_name: str,
            attr: str,
            file_pattern: str = None,
            attr_type: str = None,
            frac_rows_to_keep: float = 0.1,
            timeout: int = 15 * 60,
    ):
        """
        Rank document data by relevance to an attribute
        :param doc_name: document from which to rank data
        :param file_pattern: file pattern to use to select input files
        :param attr: keyphrase by which to rank data
        :param attr_type: type of attribute/data to rank (quantitative or qualitative)
        :param timeout: how long to wait before timing out the api call
        :return:
        """
        func = 'rank_data'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
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

    def generate_training_data(
            self,
            doc_name: str,
            data_format: str,
            timeout: int = 15 * 60,
    ):
        """
        Generate training data in a given format
        :param doc_name: document from which to generate training data
        :param data_format: training data format ('masked-structured-data', 'masked-original-tables', 'generative-question-answering')
        :param timeout:
        :return:
        """
        func = 'generate_training_data'
        args = {
            'doc_name': doc_name,
            'data_format': data_format,
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

    def train_llm(
            self,
            username: str,
            model_name: str,
            doc_names: list,
            training_formats: list = None,
            timeout: int = 5 * 60,
    ):
        """
        Train an LLM on selected documents
        :param username: user name
        :param model_name: model name to use for the trained model
        :param doc_names: list of documents to train model on
        :param training_formats: training data formats, e.g. masked-modelling, generative QA, etc
        :return:
        """
        func = 'train_llm'
        args = {
            'username': username,
            'model_name': model_name,
            'doc_names': doc_names,
            'training_formats': training_formats,
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

    def show_uploads(
            self,
            username: str,
            timeout: int = 5 * 60,
    ):
        """
        List uploaded files
        :param username: user name
        :return:
        """
        func = 'show_uploads'
        args = {
            'username': username,
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









