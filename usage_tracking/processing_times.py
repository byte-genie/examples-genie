# <h1>Estimate processing times for various API tasks</h1>
# <p>This example shows how to use information from api responses
# to estimate the amount of time taken by each api call.
# Each api call tracks the time when the api payload is received at the backend,
# and each api call produces an output file.
# Hence, we can estimate the processing time as the difference between the time api payload was received,
# and the time the output file was generated.</p>


import time
import datetime
import pandas as pd
from utils.logging import logger
from utils.byte_genie import ByteGenie

# <h2> Initialise ByteGenie </h2>

# <h3> Initialise ByteGenie in async mode (tasks will run in the background, i.e. API call will return a response before completing the task) </h3>
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    verbose=1,
)

# <h3> Initialise ByteGenie in sync mode (tasks will run in the foreground, i.e. API call will keep running until the task is finished) </h3>
bg_sync = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    verbose=1,
)


# <h2> Get usage summary </h2>
# <p> To track api usage, we can use `/get_usage_summary` endpoint.
# This endpoint provides daily usage between a `start_time` and an `end_time`
# </p>


# <h3> call /get_usage_summary endpoint </h3>
usage_resp = bg_async.get_usage_summary(
    start_time='2023-09-21',
    end_time='2023-10-21',
    agg_lvl='file',
    route='api',
)

# <h3> get response data </h3>
df_usage = usage_resp.get_output()
df_usage = pd.DataFrame(df_usage)

# <h3> Check processing time for each task </h3>
logger.info(df_usage[['start_time', 'processing_time']])
"""
<p>
</p>
"""


# <h3> Check average processing time by API endpoints </h3>
df_usage.groupby(['func'])[['n_tasks', 'processing_time', 'cost']].mean()


# <h3> Check total number of tasks by date </h3>
df_usage['date'] = [
    datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d')
    for time in df_usage['start_time']
]
df_usage.groupby(['date'])['n_tasks'].sum()