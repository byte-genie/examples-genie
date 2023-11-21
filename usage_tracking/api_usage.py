# # Estimate processing times for various API tasks
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
    # agg_lvl='file',
    route='api',
)

# <h3> get response data </h3>
df_usage = usage_resp.get_output()
df_usage = pd.DataFrame(df_usage)
df_usage = df_usage[df_usage['start_time'] != ''].reset_index(drop=True)


# <h3> Check processing time for each task </h3>
logger.info(df_usage[['usage_date', 'start_time', 'processing_time', 'file']].to_dict('records'))
"""
A sample of processing times for usage: `df_usage[['usage_date', 'start_time', 'processing_time', 'file']].head().to_dict('records')`
<p>
[
    {'usage_date': '2023-09-29', 'start_time': 1695986266.7920444, 'processing_time': 13.095955610275269,
     'file': 'gs://db-genie/entity_type=api-tasks/entity=556938a63276f9b933241de19431d3cd/data_type=structured/format=pickle/variable_desc=write_and_execute_code/source=demo-genie/556938a63276f9b933241de19431d3cd.pickle'},
    {'usage_date': '2023-09-29', 'start_time': 1695993193.0123582, 'processing_time': 10.197641849517822,
     'file': 'gs://db-genie/entity_type=api-tasks/entity=5b4889607efeaf69c7d2c738c0a003f7/data_type=structured/format=pickle/variable_desc=write_and_execute_code/source=demo-genie/5b4889607efeaf69c7d2c738c0a003f7.pickle'},
    {'usage_date': '2023-09-29', 'start_time': 1695992131.7515397, 'processing_time': 19.38746023178101,
     'file': 'gs://db-genie/entity_type=api-tasks/entity=6341de8820fd9bd16349dfb60d03a285/data_type=structured/format=pickle/variable_desc=write_and_execute_code/source=demo-genie/6341de8820fd9bd16349dfb60d03a285.pickle'},
    {'usage_date': '2023-09-29', 'start_time': 1695983936.4336717, 'processing_time': 27.400328636169437,
     'file': 'gs://db-genie/entity_type=api-tasks/entity=f39ca218084d5b33b3354fd95ffb3188/data_type=structured/format=pickle/variable_desc=write_and_execute_code/source=demo-genie/f39ca218084d5b33b3354fd95ffb3188.pickle'},
    {'usage_date': '2023-10-14', 'start_time': 1697254511.178348, 'processing_time': 2.172652006149292,
     'file': 'gs://db-genie/entity_type=api-tasks/entity=98f425ed1e7e18ffee3e279a2f7c7cd2/data_type=structured/format=pickle/variable_desc=extract_text_years/source=demo-genie/98f425ed1e7e18ffee3e279a2f7c7cd2.pickle'}
]
The processing time is in seconds.
</p>
"""


# <h3> Check average processing time by API endpoints </h3>
logger.info(df_usage.groupby(['func'])[['processing_time', 'usage']].mean().reset_index().to_dict('records'))
"""
Average processing time and usage by API endpoint, `df_usage.groupby(['func'])[['processing_time', 'usage']].mean().reset_index().to_dict('records')`
<p>
[
    {'func': 'classify_text', 'processing_time': 5.691191911697388, 'usage': 0.0006484985351562},
    {'func': 'classify_texts', 'processing_time': 3.966008901596069, 'usage': 0.0010223388671875},
    {'func': 'create_dataset', 'processing_time': 23.938680410385132, 'usage': 2.051443576812744},
    {'func': 'download_documents', 'processing_time': 558.6726469993591, 'usage': 0.109130859375},
    {'func': 'extract_text_years', 'processing_time': 2.297140955924988, 'usage': 0.0004730224609375},
    {'func': 'filter_pages_pipeline', 'processing_time': 765.0258118510246, 'usage': 340.1848633289337},
    {'func': 'standardise_years', 'processing_time': 4.605401039123535, 'usage': 0.0048713684082031},
    {'func': 'structure_quants_pipeline', 'processing_time': 842.2113270759583, 'usage': 88.72238190968831},
    {'func': 'write_and_execute_code', 'processing_time': 17.520346581935883, 'usage': 0.002090454101562475}
]
`processing_time` is the time between when the task is received at the backend, to when the output file is generated. 
`usage` is the volume of output generated (in MegaBytes).
</p>
<p>
Pipeline tasks such as, `filter_pages_pipeline` and `structure_quants_pipeline`, 
which combine multiple steps in one endpoint, typically take the longest, and product a larger volume of output. 
</p>
"""


# <h3> Check total usage by date </h3>
logger.info(df_usage.groupby(['usage_date'])[['n_tasks', 'processing_time', 'usage']].sum().reset_index().to_dict('records'))
"""
Total usage by date, `df_usage.groupby(['usage_date'])[['n_tasks', 'processing_time', 'usage']].sum().reset_index().to_dict('records')`
<p>
[
    {'usage_date': '2023-09-29', 'n_tasks': 4, 'processing_time': 70.08138632774353, 'usage': 0.0083618164062499},
    {'usage_date': '2023-10-14', 'n_tasks': 4, 'processing_time': 13.805083990097046, 'usage': 0.0106887817382812},
    {'usage_date': '2023-10-15', 'n_tasks': 2, 'processing_time': 9.657200813293457, 'usage': 0.0016708374023437001},
    {'usage_date': '2023-10-17', 'n_tasks': 1, 'processing_time': 822.0982067584991, 'usage': 623.9474687576294},
    {'usage_date': '2023-10-18', 'n_tasks': 4, 'processing_time': 2743.1195018291473, 'usage': 189.51886749267578},
    {'usage_date': '2023-10-19', 'n_tasks': 3, 'processing_time': 2549.1925797462463, 'usage': 4.803284645080566},
    {'usage_date': '2023-10-20', 'n_tasks': 2, 'processing_time': 2118.3646898269653, 'usage': 549.5814867019653},
    {'usage_date': '2023-10-21', 'n_tasks': 4, 'processing_time': 1045.8188865184784, 'usage': 529.5437860488892}
]
</p>
"""

