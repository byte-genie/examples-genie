"""
Utility functions for byte-genie API
"""

import os
import json
import time
import inspect

import pandas as pd
import requests
import numpy as np
import utils.common
from utils.logging import logger
from utils.async_utils import to_async
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_random_exponential, wait_fixed, wait_exponential


class ByteGenieResponse:

    def __init__(
            self,
            response: dict = None,
            verbose: int = 1,
    ):
        # if not isinstance(response, dict):
        #     raise ValueError('response must be a dictionary')
        self.response = response
        self.verbose = verbose

    def get_task_attr(self, attr: str):
        resp = self.response
        if isinstance(resp, dict):
            if 'response' in resp.keys():
                resp = resp['response']
                if isinstance(resp, dict):
                    if 'task_1' in resp.keys():
                        resp = resp['task_1']
                        if isinstance(resp, dict):
                            if 'task' in resp:
                                resp = resp['task']
                                if isinstance(resp, dict):
                                    if attr in resp:
                                        attr_val = resp[attr]
                                        return attr_val

    def get_response_attr(self, attr: str):
        resp = self.response
        if isinstance(resp, dict):
            if 'response' in resp.keys():
                resp = resp['response']
                if isinstance(resp, dict):
                    if 'task_1' in resp.keys():
                        resp = resp['task_1']
                        if isinstance(resp, dict):
                            if attr in resp:
                                attr_val = resp[attr]
                                return attr_val

    def set_response_attr(self, attr: str, attr_val):
        try:
            if isinstance(self.response, dict):
                if 'response' in self.response.keys():
                    if isinstance(self.response['response'], dict):
                        if 'task_1' in self.response['response'].keys():
                            if isinstance(self.response['response']['task_1'], dict):
                                self.response['response']['task_1'][attr] = attr_val
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in set_response_attr: {e}")

    def get_status(self):
        """
        Get the status of the task.
        Note that this is the status of the task at the time API call was made.
        In case a task was scheduled initially, even when the task is complete, the output of this method will not change.
        For such tasks, use check_output_file_exists() to check whether a task has finished generating its output.
        :return:
        """
        status = self.get_response_attr(attr='status')
        return status

    def get_data(self):
        """
        Get data returned in ByteGenie response.
        Note that this method only gets data that was returned from the api call,
        and in case a task is scheduled, even when the task output is ready, output of this method will not change.
        Use read_output_data() to read the current output of such scheduled tasks.
        :return:
        """
        data = self.get_response_attr(attr='data')
        for i in np.arange(0, 2, 1):
            if isinstance(data, dict):
                if 'data' in data.keys():
                    data = data['data']
        return data

    def get_output_file(
            self,
    ):
        """
        Get the output file of a task
        :return:
        """
        output_file = self.get_task_attr(attr='output_file')
        return output_file

    def get_start_time(self):
        """
        Get start time of a task
        :return:
        """
        start_time = self.get_task_attr(attr='start_time')
        return start_time

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

    def get_output(self):
        """
        Returns the output data from the response if it is not None, otherwise reads it from the output file
        :return:
        """
        if self.get_data() is not None:
            return self.get_data()
        else:
            output_data = self.read_output_data()
            if output_data is not None:
                self.set_response_attr(attr='data', attr_val=output_data)
            return output_data

    def get_output_attr(self, attr: str):
        """
        Get a specific attribute from output, e.g. doc_name
        :param attr:
        :return:
        """
        output_data = self.get_output()
        if utils.common.is_convertible_to_df(output_data):
            output_data = pd.DataFrame(output_data)
            if attr in output_data.columns:
                attr_vals = output_data[attr].unique().tolist()
                return attr_vals
            else:
                logger.error(f"Attribute, {attr}, not found in output data; "
                             f"available attributes are: {list(output_data.columns)}")
        elif isinstance(output_data, dict):
            if attr in output_data.keys():
                attr_vals = output_data[attr]
                return attr_vals
            else:
                logger.error(f"Attribute, {attr}, not found in output data; "
                             f"available attributes are: {list(output_data.keys())}")


class ByteGenieResponses:

    def __init__(
            self,
            responses: list = None,
    ):
        if responses is not None:
            self.responses = [
                ByteGenieResponse(response=response.response)
                for response in responses
            ]
        else:
            self.responses = []

    def __getitem__(self, index):
        if isinstance(index, slice):
            # Handle slicing if needed
            return self.responses[index]
        else:
            return self.responses[index]

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            # Handle slicing if needed
            self.responses[index] = value
        else:
            self.responses[index] = value

    def __len__(self):
        return len(self.responses)

    @staticmethod
    def concatenate_dict_output(outputs: list):
        keys = [list(out.keys()) for out in outputs]
        keys = [key for keys_ in keys for key in keys_]
        keys = list(set(keys))
        concatenated_dict = {}
        for key in keys:
            key_output = [out[key] for out in outputs]
            if all([isinstance(out, list) for out in key_output]):
                key_output = [out for outs in key_output for out in outs]
            concatenated_dict[key] = key_output
        return concatenated_dict

    def append(self, item):
        self.responses.append(item)

    def extend(self, items):
        self.responses.extend(items)

    def __repr__(self):
        return repr(self.responses)

    def __add__(self, other):
        if isinstance(other, ByteGenieResponses):
            return [resp for resp in self.responses] + [resp for resp in other.responses]
        else:
            logger.error(f"Responses to add are not of type `ByteGenieResponses`")
            raise TypeError("Unsupported operand type for +: object to add must be of type `ByteGenieResponses`")

    def read_output_data(self):
        tasks = [resp.async_read_output_data() for resp in self.responses]
        outputs = utils.async_utils.run_async_tasks(tasks)
        return outputs

    def get_output(self, concat: int = 0):
        outputs = [resp.get_output() for resp in self.responses]
        if concat:
            outputs = [outs for outs in outputs if outs is not None]
            if all([isinstance(out, list) for out in outputs]):
                outputs = [out for outs in outputs for out in outs]
            elif all([isinstance(out, dict) for out in outputs]):
                outputs = self.concatenate_dict_output(outputs=outputs)
        return outputs

    def get_output_attr(self, attr: str, concat: int = 0):
        attr_vals = [resp.get_output_attr(attr=attr) for resp in self.responses]
        if concat:
            attr_vals = [val for vals in attr_vals for val in vals]
        return attr_vals


class ByteGenie:

    def __init__(
            self,
            api_url: str = 'https://api.byte-genie.com/execute',
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

    def do_nothing(self):
        return ByteGenieResponse()

    @to_async
    def async_do_nothing(self):
        return self.do_nothing()

    def slugify(
            self,
            text: str,
            timeout: int = 15 * 60,
    ):
        """
        Slugify text
        :param text: text to slugify
        :param timeout: timeout value for api call
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
            username: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Upload files
        :param contents: file contents to upload
        :param filenames: file names for uploaded file contents
        :param username: user name
        :param timeout: timeout value for api call
        :return:
        """
        if username is None:
            username = self.read_username()
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
        :param timeout: timeout value for api call
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

    def list_corresponding_files(
            self,
            files: list,
            data_type: str = None,
            variable_desc: str = None,
            source: str = None,
            pagenum: str = None,
            file_format: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Get corresponding target files given input files
        :param files:
        :param data_type:
        :param variable_desc:
        :param source:
        :param pagenum:
        :param file_format:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'list_corresponding_files'
        args = {
            'files': files,
            'data_type': data_type,
            'variable_desc': variable_desc,
            'source': source,
            'file_format': file_format,
            'pagenum': pagenum,
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
    def async_list_corresponding_files(
            self,
            files: list,
            data_type: str = None,
            variable_desc: str = None,
            source: str = None,
            pagenum: str = None,
            file_format: str = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.list_corresponding_files(
                files=files,
                data_type=data_type,
                variable_desc=variable_desc,
                source=source,
                file_format=file_format,
                pagenum=pagenum,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in list_corresponding_files(): {e}")

    def check_file_exists(
            self,
            file: str,
            timeout: int = 15 * 60,
    ):
        """
        Check if a file exists
        :param file: file to check
        :param timeout: timeout value for api call
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
            add_file: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Read a file
        :param file: file to read
        :param timeout: timeout value for api call
        :return:
        """
        func = 'read_file'
        args = {
            'file': file,
            'add_file': add_file,
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
            add_file: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Read a file (asynchronous)
        :param file: file to read
        :param timeout: timeout value for api call
        :return:
        """
        try:
            resp = self.read_file(
                file=file,
                add_file=add_file,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in read_file(): {e}")

    def read_files(
            self,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            add_file: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Read files
        :param files: files to read
        :param doc_name: document name
        :param file_pattern: file pattern to match when listing files
        :param add_file: whether to add file path in the returned data
        :param timeout: time out for the api call
        :return:
        """
        func = 'read_files'
        args = {
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'add_file': add_file,
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
    def async_read_files(
            self,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            add_file: int = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.read_files(
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                add_file=add_file,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in read_files(): {e}")

    def read_page_data(
            self,
            doc_name: str,
            page_numbers: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Read page data
        :param doc_name: document name
        :param page_numbers: page numbers
        :param timeout: time out for the api call
        :return:
        """
        func = 'read_page_data'
        args = {
            'doc_name': doc_name,
            'page_numbers': page_numbers,
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
    def async_read_page_data(
            self,
            doc_name: str,
            page_numbers: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.read_page_data(
                doc_name=doc_name,
                page_numbers=page_numbers,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in read_page_data(): {e}")

    def read_quants(
            self,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            add_file: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Read quants
        :param doc_name: document name
        :param file_pattern: file pattern to match when listing files
        :param timeout: time out for the api call
        :return:
        """
        func = 'read_quants'
        args = {
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'add_file': add_file,
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
    def async_read_quants(
            self,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            add_file: int = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.read_quants(
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                add_file=add_file,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in read_qaunts(): {e}")

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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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

    @utils.async_utils.to_async
    def async_search_web(
            self,
            keyphrases: list,
            site: str = '',
            max_pagenum: int = 2,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.search_web(
                keyphrases=keyphrases,
                site=site,
                max_pagenum=max_pagenum,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in search_web(): {e}")

    def download_file(
            self,
            urls: list,
            timeout: int = 15 * 60,
    ):
        """
        Download URL content as file
        :param urls: list of URLs to download
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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

    @to_async
    def async_download_documents(
            self,
            entity_names: list,
            doc_keywords: list,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.download_documents(
                entity_names=entity_names,
                doc_keywords=doc_keywords,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in download_documents(): {e}")

    def extract_doc_year(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Extract document year
        :param doc_name: document name for which to extract info
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
            name_keyword: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Standardise text values in a single column in the data
        :param data: data to be standardised
        :param text_col: name of the column to be standardised
        :param name_keyword: word to use to to refer to values in text_col, e.g. metrics, compnay names, etc
        :param groupby_cols: columns to group the data by when standardising text_col; each group will be standardised independently
        :param timeout: timeout value for api call
        :return:
        """
        func = 'standardise_names'
        args = {
            'text_data': data,
            'text_col': text_col,
            'groupby_cols': groupby_cols,
            'name_keyword': name_keyword,
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

    def standardise_years(
            self,
            data: list,
            time_cols: list,
            groupby_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Standardise years
        :param data:
        :param time_cols:
        :param groupby_cols:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'standardise_years'
        args = {
            'data': data,
            'time_cols': time_cols,
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
        :param timeout: timeout value for api call
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
            attrs: list,
            attrs_metadata: list = None,
            file: str = None,
            data: list = None,
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
            'file': file,
            'data': data,
            'attrs': attrs,
            'attrs_metadata': attrs_metadata,
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

    @to_async
    def async_create_dataset(
            self,
            attrs: list,
            attrs_metadata: list = None,
            file: str = None,
            data: list = None,
            cols_to_use: list = None,
            groupby_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Create a new dataset with desired features from input data (asynchronous)
        :param data: input data
        :param attrs: attributes/columns to have in the new data
        :param cols_to_use: columns to consider in create the new dataset
        :param groupby_cols: columns to group data by
        :param timeout: timeout value for the api call
        :return:
        """
        try:
            resp = self.create_dataset(
                file=file,
                data=data,
                attrs=attrs,
                cols_to_use=cols_to_use,
                groupby_cols=groupby_cols,
                timeout=timeout
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in create_dataset(): {e}")

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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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
        :param timeout: timeout value for api call
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

    def filter_pages_pipeline(
            self,
            doc_name: str,
            keyphrases: list = None,
            page_rank_max: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Trigger page filtering pipeline
        :param doc_name: document name
        :param queries: list of queries
        :param timeout: timeout value for api call
        :return:
        """
        func = 'filter_pages_pipeline'
        args = {
            'doc_name': doc_name,
            'keyphrases': keyphrases,
            'page_rank_max': page_rank_max,
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
    def async_filter_pages_pipeline(
            self,
            doc_name: str,
            keyphrases: list = None,
            page_rank_max: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Trigger page filtering pipeline (async)
        :param doc_name: document name
        :param queries: list of queries
        :param timeout: timeout value for api call
        :return:
        """
        try:
            resp = self.filter_pages_pipeline(
                doc_name=doc_name,
                keyphrases=keyphrases,
                page_rank_max=page_rank_max,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in filter_pages_pipeline(): {e}")

    def translate_text_pipeline(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Trigger text translation pipeline, which extracts text and tables, and translates them
        :param doc_name: document name for which to trigger translation pipeline
        :param timeout: timeout value for api call
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

    def summarise_pages(
            self,
            doc_name: str = None,
            page_numbers: list = None,
            summary_type: str = None,
            summary_topics: list = None,
            text_col: str = None,
            groupby_cols: list = None,
            context_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Summarise pages
        :param doc_name:
        :param page_numbers:
        :param summary_type:
        :param summary_topics:
        :param text_col:
        :param groupby_cols:
        :param context_cols:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'summarise_pages'
        args = {
            'doc_name': doc_name,
            'page_numbers': page_numbers,
            'summary_type': summary_type,
            'summary_topics': summary_topics,
            'text_col': text_col,
            'groupby_cols': groupby_cols,
            'context_cols': context_cols,
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
    def async_summarise_pages(
            self,
            doc_name: str = None,
            page_numbers: list = None,
            summary_type: str = None,
            summary_topics: list = None,
            text_col: str = None,
            groupby_cols: list = None,
            context_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Summarise pages (asynchronous)
        See `summarise_pages` for available arguments
        :return:
        """
        try:
            resp = self.summarise_pages(
                doc_name=doc_name,
                page_numbers=page_numbers,
                summary_type=summary_type,
                summary_topics=summary_topics,
                text_col=text_col,
                groupby_cols=groupby_cols,
                context_cols=context_cols,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in summarise_pages: {e}")

    def structure_passage_quants(
            self,
            files: list = None,
            doc_name: str = None,
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
        :param files:
        :param doc_name:
        :param file_pattern:
        :param base_attrs:
        :param base_attr_names:
        :param custom_attrs:
        :param custom_attr_names:
        :param text_col: column of the column containing text
        :param timeout: timeout value for api call
        :return:
        """
        func = 'structure_passage_quants'
        args = {
            'files': files,
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
            files: list = None,
            doc_name: str = None,
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
                files=files,
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
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Structure quants contained in tables extracted from documents
        :param files:
        :param doc_name:
        :param file_pattern:
        :param base_attrs:
        :param base_attr_names:
        :param custom_attrs:
        :param custom_attr_names:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'structure_tabular_quants'
        args = {
            'files': files,
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
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.structure_tabular_quants(
                files=files,
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

    def structure_page_quants(
            self,
            files: list = None,
            doc_name: str = None,
            page_numbers: list = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Structure quants contained in tables extracted from documents
        :param files:
        :param doc_name:
        :param page_numbers:
        :param base_attrs:
        :param base_attr_names:
        :param custom_attrs:
        :param custom_attr_names:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'structure_page_quants'
        args = {
            'files': files,
            'doc_name': doc_name,
            'page_numbers': page_numbers,
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
    def async_structure_page_quants(
            self,
            files: list = None,
            doc_name: str = None,
            page_numbers: list = None,
            base_attrs: list = None,
            base_attr_names: list = None,
            custom_attrs: list = None,
            custom_attr_names: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.structure_page_quants(
                files=files,
                doc_name=doc_name,
                page_numbers=page_numbers,
                base_attrs=base_attrs,
                base_attr_names=base_attr_names,
                custom_attrs=custom_attrs,
                custom_attr_names=custom_attr_names,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in structure_page_quants: {e}")

    def extract_text_pipeline(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Trigger text extraction pipeline, which extracts text and tables from each page of the document
        :param doc_name: document name
        :param timeout: timeout value for api call
        :return:
        """
        func = 'extract_text_pipeline'
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

    @to_async
    def async_extract_text_pipeline(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.extract_text_pipeline(
                doc_name=doc_name,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in extract_text_pipeline(): {e}")

    def structure_quants_pipeline(
            self,
            doc_name: str,
            queries: list = None,
            file_rank_max: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Trigger quant-structuring pipeline, which extracts text and quant metrics, and structures all quant metrics
        from passages and tables
        :param doc_name: document name for which to trigger quant extraction
        :param timeout: timeout value for api call
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

    @to_async
    def async_structure_quants_pipeline(
            self,
            doc_name: str,
            queries: list = None,
            file_rank_max: int = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.structure_quants_pipeline(
                doc_name=doc_name,
                queries=queries,
                file_rank_max=file_rank_max,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in structure_quants_pipeline(): {e}")

    def verify_data(
            self,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            var_col: str = None,
            val_col: str = None,
            context_col: str = None,
            val_desc: str = None,
            verification_type: str = None,
            verification_method: str = None,
            vars_to_verify: list = None,
            fuzzy_match_min: float = None,
            output_data_type: str = 'verification',
            timeout: int = 15 * 60,
    ):
        """
        Verify extracted data
        :param files: input files
        :param doc_name: document name
        :param file_pattern: file pattern to select input files
        :param var_col: name for variable column
        :param val_col: name for value column
        :param val_desc: short description of value column
        :param verification_type: type of verification
        :param context_col: name for context column
        :param fuzzy_match_min: minimum match threshold for fuzzy match
        :param output_data_type: data type to use for output file path
        :param timeout: time out for api call
        :return:
        """
        func = 'verify_data'
        args = {
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'var_col': var_col,
            'val_col': val_col,
            'context_col': context_col,
            'val_desc': val_desc,
            'verification_type': verification_type,
            'verification_method': verification_method,
            'vars_to_verify': vars_to_verify,
            'fuzzy_match_min': fuzzy_match_min,
            'output_data_type': output_data_type,
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
    def async_verify_data(
            self,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            var_col: str = None,
            val_col: str = None,
            val_desc: str = None,
            context_col: str = None,
            verification_type: str = None,
            verification_method: str = None,
            vars_to_verify: list = None,
            fuzzy_match_min: float = None,
            output_data_type: str = 'verification',
            timeout: int = 15 * 60,
    ):
        """
        Verify extracted data (asynchronous)
        :param files: input files
        :param doc_name: document name
        :param file_pattern: file pattern to select input files
        :param var_col: name for variable column
        :param val_col: name for value column
        :param val_desc: short description of value column
        :param verification_type: type of verification
        :param context_col: name for context column
        :param fuzzy_match_min: minimum match threshold for fuzzy match
        :param output_data_type: data type to use for output file path
        :param timeout: time out for api call
        :return:
        """
        try:
            resp = self.verify_data(
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                var_col=var_col,
                val_col=val_col,
                context_col=context_col,
                val_desc=val_desc,
                verification_type=verification_type,
                verification_method=verification_method,
                vars_to_verify=vars_to_verify,
                fuzzy_match_min=fuzzy_match_min,
                output_data_type=output_data_type,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in verify_data(): {e}")

    def synthesize_quant_data(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Synthesize document meta-data with quantitative info extracted from the document
        :param doc_name: document name
        :param timeout: timeout value for api call
        :return:
        """
        func = 'synthesize_quant_data'
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

    @to_async
    def async_synthesize_quant_data(
            self,
            doc_name: str,
            timeout: int = 15 * 60,
    ):
        """
        Synthesize document meta-data with quantitative info extracted from the document (asynchronous)
        :param doc_name: document name
        :param timeout: timeout value for api call
        :return:
        """
        func = 'synthesize_quant_data'
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
            attr: str,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            attr_type: str = None,
            method: str = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            groupby_cols: list = None,
            frac_rows_to_keep: float = 0.1,
            timeout: int = 15 * 60,
    ):
        """
        Rank document data by relevance to an attribute
        :param doc_name: document from which to rank data
        :param file_pattern: file pattern to use to select input files
        :param attr: keyphrase by which to rank data
        :param attr_type: type of attribute/data to rank ('quantitative' or 'qualitative')
        :param timeout: how long to wait before timing out the api call
        :return:
        """
        func = 'rank_data'
        args = {
            'attr': attr,
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'attr_type': attr_type,
            'method': method,
            'cols_to_use': cols_to_use,
            'cols_not_use': cols_not_use,
            'non_null_cols': non_null_cols,
            'groupby_cols': groupby_cols,
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

    @to_async
    def async_rank_data(
            self,
            attr: str,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            attr_type: str = None,
            method: str = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            groupby_cols: list = None,
            frac_rows_to_keep: float = 0.1,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.rank_data(
                attr=attr,
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                attr_type=attr_type,
                method=method,
                cols_to_use=cols_to_use,
                cols_not_use=cols_not_use,
                non_null_cols=non_null_cols,
                groupby_cols=groupby_cols,
                frac_rows_to_keep=frac_rows_to_keep,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in rank_data(): {e}")

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
        :param timeout: timeout value for api call
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

    def add_embeddings_to_data(
            self,
            data: list,
            cols_to_use: list,
            model: str = None,
            chunk_size: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Add embeddings column to data based on a pre-defined set of columns
        :param data: data for which to create embeddings
        :param cols_to_use: columns to use when creating embeddings
        :param model: model for generating embeddings (optional)
        :param chunk_size: chunk size for creating embeddings in one go (optional)
        :param timeout: timeout value for api call
        :return:
        """
        func = 'add_embeddings_to_data'
        args = {
            'data': data,
            'cols_to_use': cols_to_use,
            'model': model,
            'chunk_size': chunk_size,
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
    def async_add_embeddings_to_data(
            self,
            data: list,
            cols_to_use: list,
            model: str = None,
            chunk_size: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Add embeddings column to data based on a pre-defined set of columns (asynchronous)
        :param data: data for which to create embeddings
        :param cols_to_use: columns to use when creating embeddings
        :param model: model for generating embeddings (optional)
        :param chunk_size: chunk size for creating embeddings in one go (optional)
        :param timeout: timeout value for api call
        :return:
        """
        try:
            resp = self.add_embeddings_to_data(
                data=data,
                cols_to_use=cols_to_use,
                model=model,
                chunk_size=chunk_size,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in add_embeddings_to_data(): {e}")

    def embed_doc_data(
            self,
            doc_name: str = None,
            file_pattern: str = None,
            page_numbers: list = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            model: str = None,
            chunk_size: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Embed document data
        :param data: data for which to create embeddings
        :param cols_to_use: columns to use when creating embeddings
        :param model: model for generating embeddings (optional)
        :param chunk_size: chunk size for creating embeddings in one go (optional)
        :param timeout: timeout value for api call
        :return:
        """
        func = 'embed_doc_data'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'page_numbers': page_numbers,
            'cols_to_use': cols_to_use,
            'cols_not_use': cols_not_use,
            'non_null_cols': non_null_cols,
            'model': model,
            'chunk_size': chunk_size,
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
    def async_embed_doc_data(
            self,
            doc_name: str,
            file_pattern: str,
            page_numbers: list = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            model: str = None,
            chunk_size: int = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.embed_doc_data(
                doc_name=doc_name,
                file_pattern=file_pattern,
                page_numbers=page_numbers,
                cols_to_use=cols_to_use,
                cols_not_use=cols_not_use,
                non_null_cols=non_null_cols,
                model=model,
                chunk_size=chunk_size,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in embed_doc_data(): {e}")

    def score_doc_text_similarity(
            self,
            doc_name: str,
            file_pattern: str,
            query: str,
            model: str = None,
            chunk_size: int = None,
            timeout: int = 15 * 60,
    ):
        """
        score_doc_text_similarity
        :param data: data for which to create embeddings
        :param cols_to_use: columns to use when creating embeddings
        :param model: model for generating embeddings (optional)
        :param chunk_size: chunk size for creating embeddings in one go (optional)
        :param timeout: timeout value for api call
        :return:
        """
        func = 'score_doc_text_similarity'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'query': query,
            'model': model,
            'chunk_size': chunk_size,
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
    def async_score_doc_text_similarity(
            self,
            doc_name: str,
            file_pattern: str,
            query: str,
            model: str = None,
            chunk_size: int = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.score_doc_text_similarity(
                doc_name=doc_name,
                file_pattern=file_pattern,
                query=query,
                model=model,
                chunk_size=chunk_size,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in score_doc_text_similarity(): {e}")

    def filter_similarity_scored_data(
            self,
            doc_name: str,
            file_pattern: str,
            filter_what: str = 'data',
            non_null_cols: list = None,
            groupby_cols: list = None,
            max_rows_to_keep: int = None,
            max_frac_rows_to_keep: float = None,
            filename_sfx: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Filter similarity scored data to keep only the most relevant rows
        :param doc_name: document name
        :param file_pattern: file pattern to select input files
        :param filter_what: what to filter ('data' or 'files')
        :param non_null_cols: only rows where all these columns are non-null will be returned
        :param groupby_cols: group data by these columns when filtering over rows to keep
        :param frac_rows_to_keep: fraction of top rows to keep, sorted by similarity score
        :param filename_sfx: filename suffix to add to output file
        :param timeout: timeout value for api call
        :return:
        """
        func = 'filter_similarity_scored_data'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'filter_what': filter_what,
            'non_null_cols': non_null_cols,
            'groupby_cols': groupby_cols,
            'max_rows_to_keep': max_rows_to_keep,
            'max_frac_rows_to_keep': max_frac_rows_to_keep,
            'filename_sfx': filename_sfx,
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
    def async_filter_similarity_scored_data(
            self,
            doc_name: str,
            file_pattern: str,
            filter_what: str = 'data',
            non_null_cols: list = None,
            groupby_cols: list = None,
            max_rows_to_keep: int = None,
            max_frac_rows_to_keep: float = None,
            filename_sfx: str = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.filter_similarity_scored_data(
                doc_name=doc_name,
                file_pattern=file_pattern,
                filter_what=filter_what,
                non_null_cols=non_null_cols,
                groupby_cols=groupby_cols,
                max_rows_to_keep=max_rows_to_keep,
                max_frac_rows_to_keep=max_frac_rows_to_keep,
                filename_sfx=filename_sfx,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in filter_similarity_scored_data(): {e}")

    def trace_evidence(
            self,
            doc_name: str,
            file_pattern: str,
            destination: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Trace evidence for extracted data
        :param doc_name: document name
        :param file_pattern: file pattern to select input files
        :param destination: destination until which to trace evidence (optional)
        :param timeout: timeout value for api call
        :return:
        """
        func = 'trace_evidence'
        args = {
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'destination': destination,
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
    def async_trace_evidence(
            self,
            doc_name: str,
            file_pattern: str,
            destination: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Trace evidence for extracted data (asynchronous)
        :param doc_name: document name
        :param file_pattern: file pattern to select input files
        :param destination: destination until which to trace evidence (optional)
        :param timeout: timeout value for api call
        :return:
        """
        try:
            resp = self.trace_evidence(
                doc_name=doc_name,
                file_pattern=file_pattern,
                destination=destination,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in trace_evidence(): {e}")

    def estimate_values(
            self,
            metrics_to_estimate: list,
            attrs_to_estimate: list = None,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            groupby_cols: list = None,
            context_cols: list = None,
            non_null_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Estimate values for a set of metrics
        :param metrics_to_estimate:
        :param attrs_to_estimate:
        :param files:
        :param doc_name:
        :param file_pattern:
        :param cols_to_use:
        :param cols_not_use:
        :param groupby_cols:
        :param context_cols:
        :param non_null_cols:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'estimate_values'
        args = {
            'metrics_to_estimate': metrics_to_estimate,
            'attrs_to_estimate': attrs_to_estimate,
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'cols_to_use': cols_to_use,
            'cols_not_use': cols_not_use,
            'groupby_cols': groupby_cols,
            'context_cols': context_cols,
            'non_null_cols': non_null_cols,
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
    def async_estimate_values(
            self,
            metrics_to_estimate: list,
            attrs_to_estimate: list = None,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            groupby_cols: list = None,
            context_cols: list = None,
            non_null_cols: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.estimate_values(
                metrics_to_estimate=metrics_to_estimate,
                attrs_to_estimate=attrs_to_estimate,
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                cols_to_use=cols_to_use,
                cols_not_use=cols_not_use,
                groupby_cols=groupby_cols,
                context_cols=context_cols,
                non_null_cols=non_null_cols,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in estimate_values(): {e}")

    def get_usage_summary(
            self,
            username: str = None,
            start_time: str = None,
            end_time: str = None,
            route: str = None,
            timeout: int = 15 * 60,
    ):
        """
        score_doc_text_similarity
        :param data: data for which to create embeddings
        :param cols_to_use: columns to use when creating embeddings
        :param model: model for generating embeddings (optional)
        :param chunk_size: chunk size for creating embeddings in one go (optional)
        :param timeout: timeout value for api call
        :return:
        """
        if username is None:
            username = self.read_username()
        func = 'get_usage_summary'
        args = {
            'username': username,
            'start_time': start_time,
            'end_time': end_time,
            'route': route,
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

    def classify_texts(
            self,
            texts: list,
            labels: list = None,
            multi_class: int = 0,
            timeout: int = 15 * 60,
    ):
        """
        Classify text into a set of labels
        :param text:
        :param lables:
        :param multi_class:
        :param timeout: timeout value for api call
        :return:
        """
        func = 'classify_texts'
        args = {
            'texts': texts,
            'labels': labels,
            'multi_class': multi_class,
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
    def async_classify_texts(
            self,
            texts: list,
            labels: list = None,
            multi_class: int = 0,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.classify_texts(
                texts=texts,
                labels=labels,
                multi_class=multi_class,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in classify_text(): {e}")

    def extract_worldviews(
            self,
            data: list = None,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            page_numbers: list = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            extraction_format: str = None,
            min_text_len: int = None,
            timeout: int = 15 * 60,
    ):
        """
        Extract worldviews augmented question-answers from data
        :param data:
        :param files:
        :param doc_name:
        :param file_pattern:
        :param extraction_format:
        :param min_text_len:
        :param timeout:
        :return:
        """
        func = 'extract_worldviews'
        args = {
            'data': data,
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'page_numbers': page_numbers,
            'extraction_format': extraction_format,
            'cols_to_use': cols_to_use,
            'cols_not_use': cols_not_use,
            'non_null_cols': non_null_cols,
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

    @utils.async_utils.to_async
    def async_extract_worldviews(
            self,
            data: list = None,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            page_numbers: list = None,
            extraction_format: str = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.extract_worldviews(
                data=data,
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                page_numbers=page_numbers,
                extraction_format=extraction_format,
                cols_to_use=cols_to_use,
                cols_not_use=cols_not_use,
                non_null_cols=non_null_cols,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in extract_worldviews(): {e}")

    def write_to_sql_table(
            self,
            table_name: str,
            data: list = None,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            page_numbers: list = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            timeout: int = 15 * 60,
    ):
        """
        Write data to sql table
        :param data:
        :param files:
        :param doc_name:
        :param file_pattern:
        :param timeout:
        :return:
        """
        func = 'write_to_sql_table'
        args = {
            'table_name': table_name,
            'data': data,
            'files': files,
            'doc_name': doc_name,
            'file_pattern': file_pattern,
            'page_numbers': page_numbers,
            'cols_to_use': cols_to_use,
            'cols_not_use': cols_not_use,
            'non_null_cols': non_null_cols,
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

    @utils.async_utils.to_async
    def async_write_to_sql_table(
            self,
            table_name: str,
            data: list = None,
            files: list = None,
            doc_name: str = None,
            file_pattern: str = None,
            page_numbers: list = None,
            cols_to_use: list = None,
            cols_not_use: list = None,
            non_null_cols: list = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.write_to_sql_table(
                table_name=table_name,
                data=data,
                files=files,
                doc_name=doc_name,
                file_pattern=file_pattern,
                page_numbers=page_numbers,
                cols_to_use=cols_to_use,
                cols_not_use=cols_not_use,
                non_null_cols=non_null_cols,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in write_to_sql_table(): {e}")

    def query_model(
            self,
            query: str,
            context: str = None,
            model: str = None,
            timeout: int = 15 * 60,
    ):
        """
        Query model
        :return:
        """
        func = 'query_litellm'
        args = {
            'query': query,
            'context': context,
            'model': model,
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

    @utils.async_utils.to_async
    def async_query_model(
            self,
            query: str,
            context: str = None,
            model: str = None,
            timeout: int = 15 * 60,
    ):
        try:
            resp = self.query_model(
                query=query,
                context=context,
                model=model,
                timeout=timeout,
            )
            return resp
        except Exception as e:
            if self.verbose:
                logger.error(f"Error in query_model(): {e}")

