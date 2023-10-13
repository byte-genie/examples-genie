"""
Common utility functions
"""

import re
import os
import json
import base64
import unicodedata
import pandas as pd


def read_secrets(secrets_file: str = 'secrets.json') -> dict:
    filename = os.path.join(secrets_file)
    try:
        with open(filename, mode='r') as f:
            return json.loads(f.read())
    except FileNotFoundError:
        return {}


def convert_file_content_to_bytes(file_path: str):
    with open(file_path, 'rb') as file:
        content = file.read()
        data = base64.b64encode(content)
        data_with_prefix = "data:;base64," + data.decode('utf-8')
    return data_with_prefix


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


def extract_nested_brackets(s, bracket='()'):
    results = set()
    for start in range(len(s)):
        string_ = s[start:]
        if bracket == '()':
            results.update(re.findall('\(.*?\)', string_))
        if bracket == '[]':
            results.update(re.findall('\[.*?\]', string_))
        if bracket == '{}':
            results.update(re.findall('\{.*?\}', string_))
    return list(results)


def convert_list_cols_to_str(df, cols: list, sep: str = '; ', verbose: int = 1):
    cols = [col for col in cols if col in df.columns]
    for col in cols:
        try:
            if col in df.columns:
                l_val = []
                for val in df[col].tolist():
                    if isinstance(val, list):
                        val = [str(v) for v in val]
                        l_val = l_val + [sep.join(val)]
                    elif isinstance(val, str):
                        if extract_nested_brackets(val, '[]'):
                            try:
                                val_eval = eval(val)
                                if isinstance(val_eval, list):
                                    val_eval = [str(v) for v in val_eval]
                                    l_val = l_val + [sep.join(val_eval)]
                            except Exception as e:
                                l_val = l_val + [val]
                        else:
                            l_val = l_val + [val]
                    else:
                        l_val = l_val + [val]
                df[col] = l_val
        except Exception as e:
            if verbose:
                print(f"Error in convert_list_cols_to_str() for {col}: {e}")
    return df


def read_file_contents(directory: str):
    contents = []
    file_paths = []
    file_names = os.listdir(directory)
    for file_name in file_names:
        file_path = os.path.join(directory, file_name)
        with open(file_path, 'rb') as file:
            content = file.read()
            data = base64.b64encode(content)
            data_with_prefix = "data:;base64," + data.decode('utf-8')
            contents.append(data_with_prefix)
            file_paths.append(file_path)
    df = pd.DataFrame()
    df['file'] = file_paths
    df['filename'] = [file.split('/')[-1] for file in file_paths]
    df['content'] = contents
    return df


def get_doc_name(file: str):
    if 'entity=' not in file:
        return None
    else:
        doc_name = file.split('entity=')[-1].split('/')[0]
        return doc_name


