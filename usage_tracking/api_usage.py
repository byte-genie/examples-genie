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
df_usage = df_usage[df_usage['start_time'] != '']

# <h3> add task data </h3>
df_usage['date'] = [
    datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d')
    for time in df_usage['start_time']
]

# <h3> Check processing time for each task </h3>
logger.info(df_usage[['date', 'start_time', 'processing_time']].to_dict('records'))
"""
<p>
[{'date': '2023-10-21', 'start_time': 1697864060.795667, 'processing_time': 112.52133297920227},
 {'date': '2023-10-18', 'start_time': 1697627252.315056, 'processing_time': 196.1289439201355},
 {'date': '2023-10-16', 'start_time': 1697388101.372991, 'processing_time': 3.9660089015960693},
 {'date': '2023-10-22', 'start_time': 1697943363.768728, 'processing_time': 7.410271883010864},
 {'date': '2023-10-18', 'start_time': 1697562809.9097931, 'processing_time': 822.0982067584991},
 {'date': '2023-10-21', 'start_time': 1697818846.179687, 'processing_time': 869.0803129673004},
 {'date': '2023-10-22', 'start_time': 1697972366.371352, 'processing_time': 440.2356481552124},
 {'date': '2023-10-18', 'start_time': 1697635267.456074, 'processing_time': 800.3819260597229},
 {'date': '2023-10-19', 'start_time': 1697726095.941306, 'processing_time': 18.177693843841553},
 {'date': '2023-10-21', 'start_time': 1697856182.846554, 'processing_time': 543.5324459075928},
 {'date': '2023-09-29', 'start_time': 1695986266.7920444, 'processing_time': 13.095955610275269},
 {'date': '2023-09-29', 'start_time': 1695993193.0123582, 'processing_time': 10.197641849517822},
 {'date': '2023-10-14', 'start_time': 1697268974.523259, 'processing_time': 4.4487409591674805},
 {'date': '2023-09-29', 'start_time': 1695992131.7515397, 'processing_time': 19.387460231781006},
 {'date': '2023-10-19', 'start_time': 1697684837.646781, 'processing_time': 2501.315218925476},
 {'date': '2023-10-14', 'start_time': 1697268072.217939, 'processing_time': 4.76206111907959},
 {'date': '2023-10-22', 'start_time': 1697974058.518997, 'processing_time': 403.058002948761},
 {'date': '2023-10-14', 'start_time': 1697254511.178348, 'processing_time': 2.172652006149292},
 {'date': '2023-10-21', 'start_time': 1697864638.4567301, 'processing_time': 193.4132697582245},
 {'date': '2023-10-19', 'start_time': 1697711794.294333, 'processing_time': 29.69966697692871},
 {'date': '2023-10-21', 'start_time': 1697864924.383162, 'processing_time': 196.35183787345886},
 {'date': '2023-10-22', 'start_time': 1697966493.79529, 'processing_time': 421.4657099246979},
 {'date': '2023-10-14', 'start_time': 1697254700.73337, 'processing_time': 2.4216299057006836},
 {'date': '2023-10-15', 'start_time': 1697383011.986808, 'processing_time': 5.691191911697388},
 {'date': '2023-10-18', 'start_time': 1697604531.6591518, 'processing_time': 573.8128480911255},
 {'date': '2023-10-21', 'start_time': 1697820154.844623, 'processing_time': 1249.284376859665},
 {'date': '2023-09-29', 'start_time': 1695983936.4336715, 'processing_time': 27.400328636169434},
 {'date': '2023-10-18', 'start_time': 1697612150.6792161, 'processing_time': 1172.7957837581635}
]
The processing time is in seconds.
</p>
"""


# <h3> Check average processing time by API endpoints </h3>
df_usage.groupby(['func'])[['processing_time', 'usage']].mean().reset_index().to_dict('records')
"""
<p>
[
    {'func': 'classify_text', 'processing_time': 5.691191911697388, 'usage': 0.00064849853515625},
    {'func': 'classify_texts', 'processing_time': 3.9660089015960693, 'usage': 0.0010223388671875},
    {'func': 'create_dataset', 'processing_time': 23.938680410385132, 'usage': 2.051443576812744},
    {'func': 'download_documents', 'processing_time': 558.6726469993591, 'usage': 0.109130859375},
    {'func': 'extract_text_years', 'processing_time': 2.297140955924988, 'usage': 0.0004730224609375},
    {'func': 'filter_pages_pipeline', 'processing_time': 765.0258118510246, 'usage': 340.1848633289337},
    {'func': 'get_usage_summary', 'processing_time': 421.58645367622375, 'usage': 0.020090738932291668},
    {'func': 'read_handbook', 'processing_time': 7.410271883010864, 'usage': 0.001373291015625},
    {'func': 'standardise_years', 'processing_time': 4.605401039123535, 'usage': 0.004871368408203125},
    {'func': 'structure_quants_pipeline', 'processing_time': 842.2113270759583, 'usage': 88.72238190968831},
    {'func': 'write_and_execute_code', 'processing_time': 17.520346581935883, 'usage': 0.0020904541015625}
]
`processing_time` is the time between when the task is received at the backend, to when the output file is generated. 
`usage` is the volume of output generated (in MegaBytes).
</p>
<p>Pipeline tasks such as, `filter_pages_pipeline` and `structure_quants_pipeline`, 
which combine multiple steps in one endpoint, typically take the longest, and product a larger volume of output. </p>
"""

# <h3> Check total usage by date </h3>
logger.info(df_usage.groupby(['date'])[['n_tasks', 'processing_time', 'usage']].sum().reset_index().to_dict('records'))
"""
<p>
[
    {'date': '2023-09-29', 'n_tasks': 4, 'processing_time': 70.08138632774353, 'usage': 0.00836181640625},
    {'date': '2023-10-14', 'n_tasks': 4, 'processing_time': 13.805083990097046, 'usage': 0.01068878173828125},
    {'date': '2023-10-15', 'n_tasks': 1, 'processing_time': 5.691191911697388, 'usage': 0.00064849853515625},
    {'date': '2023-10-16', 'n_tasks': 1, 'processing_time': 3.9660089015960693, 'usage': 0.0010223388671875},
    {'date': '2023-10-18', 'n_tasks': 5, 'processing_time': 3565.2177085876465, 'usage': 813.4663362503052},
    {'date': '2023-10-19', 'n_tasks': 3, 'processing_time': 2549.1925797462463, 'usage': 4.803284645080566},
    {'date': '2023-10-21', 'n_tasks': 6, 'processing_time': 3164.1835763454437, 'usage': 1079.1252727508545},
    {'date': '2023-10-22', 'n_tasks': 4, 'processing_time': 1272.1696329116821, 'usage': 0.0616455078125}
]
</p>
"""
