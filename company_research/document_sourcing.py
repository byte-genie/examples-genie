"""
Source company documents
"""

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

"""
We can search for the relevant documents in two wasy:
1) by using /download_documents endpoint, which first finds the company's homepage and then searches documents from it
2) by using /find_homepage endpoint to first find the company homepage, and then use /donwload_documents endpoing with 
"""
## trigger document download
resp = bg.download_documents(
    entity_names=company_names,
    doc_keywords=doc_keywords,
)
## check resp
status = resp['response']['task_1']['status'] ## scheduled
output_file = resp['response']['task_1']['task']['output_file'] ## file where output will be written (gs://db-genie/entity_type=api-tasks/entity=593a5370f106bf174d115e2fc2c2a3c9/data_type=structured/format=pickle/variable_desc=download_documents/source=api-genie/593a5370f106bf174d115e2fc2c2a3c9.pickle)
resp = bg_sync.check_file_exists(output_file)
output_file_exists = resp['response']['task_1']['data'] ## False if file does not yet exist; True when file exists
