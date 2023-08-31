# # Analyse data for cement companies


import pandas as pd
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

# ### check download output status
resp.check_output_file_exists()
