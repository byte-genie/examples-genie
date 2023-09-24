# # Search prices for relevant B2B solutions


import time
import pandas as pd
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background, i.e. API call will return a response before completing the task)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground, i.e. API call will keep running until the task is finished)
bg_sync = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    verbose=1,
)

# ## set inputs

# ### set keyphrases to search
keyphrases = [
    'on-prem data management solution pricing',
    'on-prem data analytics solution pricing',
    'on-prem ETL solution pricing',
    'on-prem data solution pricing',
    'data analytics SaaS pricing',
    'data management SaaS pricing',
    'ETL SaaS pricing',
    'ETL API pricing',
]

# ## search web for selected keyphrases

# ### trigger web search
search_response = bg_async.search_web(
    keyphrases=keyphrases
)

# ### check search output
if search_response.check_output_file_exists():
    df_search = search_response.read_output_data()
else:
    logger.info(f"search output is not complete yet: wait some more")

# ## Process search results
