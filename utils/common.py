"""
Common utility functions
"""

import os
import json


def read_secrets(secrets_file: str = 'secrets.json') -> dict:
    filename = os.path.join(secrets_file)
    try:
        with open(filename, mode='r') as f:
            return json.loads(f.read())
    except FileNotFoundError:
        return {}

