"""
Common utility functions
"""

import re
import os
import json
import base64


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
