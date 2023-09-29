# # Track API Usage (UNDER DEVELOPMENT, FOR DEMO ONLY)


import time
import datetime
import pandas as pd
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background, i.e. API call will return a response before completing the task)
bg_async = ByteGenie(
    secrets_file='secrets_mcp.json',
    task_mode='async',
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground, i.e. API call will keep running until the task is finished)
bg_sync = ByteGenie(
    secrets_file='secrets_mcp.json',
    task_mode='sync',
    verbose=1,
)

# ## Get usage summary
"""
To track api usage, we can use `/get_usage_summary` endpoint. This endpoint provides daily usage between a `start_time` and an `end_time`
"""

# ### call /get_usage_summary endpoint
usage_resp = bg_async.get_usage_summary(
    username=bg_sync.read_username(),
    start_time='2023-09-29',
    end_time='2023-09-15',
    route='api',
)

# ### get response data
df_usage = usage_resp.read_output_data()
df_usage = pd.DataFrame(df_usage)

# ### calculate total number of tasks by date
df_usage.groupby(['last_mod_date'])['n_tasks'].sum()
