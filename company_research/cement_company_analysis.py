# # Analyse data for cement companies

import time
import pandas as pd
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground)
bg_sync = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    verbose=1,
)

# ## set inputs

# ### set company names
company_names = [
    'Ultratech Cement',
    'Cemex Inc',
    'ACC Limited',
    'Heidelberg Materials Inc',
    'JK Cement',
    'Shree Cement',
    'China Resources Cement',
    'Eurocement Group',
    'Birla Corporation',
    'Lafarge Inc',
]

# ## set document keywords to search
doc_keywords = [
    'sustainability reports',
    'annual reports',
]

# ## Data sourcing

# ### trigger document download
resp = bg_async.download_documents(
    entity_names=company_names,
    doc_keywords=doc_keywords,
)

# ### wait for output to exist
time.sleep(60 * 60)

# ### check download output status
resp.check_output_file_exists()

# ### read output file
df_document_urls = pd.DataFrame(resp.read_output_data())

# ### Get unique doc_name
doc_names = df_document_urls['doc_name'].unique().tolist()

# ## Extract document info for downloaded documents

# ### make api calls
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"Extracting document info for ({doc_num}/{len(doc_names)}): {doc_name}")
    resp_ = bg_async.extract_doc_info(
        doc_name=doc_name
    )
    responses = responses + [resp_]
