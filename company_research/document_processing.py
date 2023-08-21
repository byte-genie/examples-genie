"""
Process documents to create structured data files
"""

import time
import pandas as pd
from utils.logging import logger
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
## set file containing documents
docs_file = 'gs://db-genie/entity_type=api-tasks/entity=593a5370f106bf174d115e2fc2c2a3c9/data_type=structured/format=pickle/variable_desc=download_documents/source=api-genie/593a5370f106bf174d115e2fc2c2a3c9.pickle'
## read documents
resp = bg_sync.read_file(docs_file)
df_reports = bg_sync.get_response_data(resp) # resp['response']['task_1']['data']['data']
df_reports = pd.DataFrame(df_reports)
## get doc names (slugified urls)
doc_names = [
    bg_sync.slugify(href)['response']['task_1']['data']
    for href in df_reports['href'].unique().tolist()
]
## extract document info
resp = {}
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"{doc_num}/{len(doc_names)}: {doc_name}")
    resp[doc_name] = bg_async.extract_doc_info(
        doc_name=doc_name,
    )
## wait for output to be ready
time.sleep(15 * 60)
## get output files for doc_info
doc_info_files = [
    bg_sync.get_response_output_file(resp_)
    for resp_ in resp
]
## check if the output is ready
resp = [
    bg_sync.check_file_exists(file)
    for file in doc_info_files
]
doc_info_files_exist = [
    bg_sync.get_response_data(resp_)
    for resp_ in resp
]