"""
Source company documents
"""

import pandas as pd
from utils.byte_genie import ByteGenie

## init byte-genie in async mode (tasks will run in the background)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    verbose=1,
)
## init byte-genie in sync mode (tasks will run in the foreground)
bg_sync = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    verbose=1,
)
## set company names to download documents for
company_names = [
    'Rosneft Oil Co.',
    'Petroleo Brasileiro SA',
    'American Electric Power Company, Inc.'
    'Vedanta Limited',
    'Shell PLC',
    'UltraTech Cement Limited',
]
## set documnet keywords to search documents for
doc_keywords = ['sustainability report']

## trigger document download
resp = bg_async.download_documents(
    entity_names=company_names,
    doc_keywords=doc_keywords,
)
## check resp
status = resp['response']['task_1']['status'] ## scheduled
## file where output will be written (gs://db-genie/entity_type=api-tasks/entity=593a5370f106bf174d115e2fc2c2a3c9/data_type=structured/format=pickle/variable_desc=download_documents/source=api-genie/593a5370f106bf174d115e2fc2c2a3c9.pickle)
output_file = resp['response']['task_1']['task']['output_file']
## check output_file status (exists or not)
resp = bg_sync.check_file_exists(output_file)
## False if file does not yet exist; True when file exists
output_file_exists = resp['response']['task_1']['data']
## read output
resp = bg_sync.read_file(output_file)
df_reports = resp['response']['task_1']['data']['data']
df_reports = pd.DataFrame(df_reports)
## check df columns
df_reports.columns ## ['entity_name', 'href', 'href_text', 'keyphrase', 'page_summary', 'result_html', 'result_text']
## check the document urls found
df_reports['href'].unique()
"""
The documents in df_reports have already been download.
We can now move on to processing the ones we would like to.
See company_research/document_processing.py to see how to process documents.
"""