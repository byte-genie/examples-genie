# # Find an organisation's homepage, and search relevant info from it

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

"""
'async' mode is suitable for long-running tasks, so that api calls can be run in the background, 
while the rest of the code can continue doing other things.

'sync' mode is suitable for short-lived tasks, where we need some output, before we can move on to anything else.
"""

# ## Define inputs

# ### set organisation names to download documents for
org_names = [
    'Vedanta Limited',
    'GHG protocol',
    'World Bank',
]

# ## Find homepage
resp = bg_async.find_homepage(
    entity_names=org_names
)

# ### file where output will be written
output_file = resp['response']['task_1']['task']['output_file']
"""
output_file
gs://db-genie/entity_type=api-tasks/entity=22daacbd638ff9c859bc350ba3e48c3b/data_type=structured/format=pickle/variable_desc=find_homepage/source=api-genie/22daacbd638ff9c859bc350ba3e48c3b.pickle
"""

# ### check whether output_file exists or not
output_file_exists = bg_sync.get_response_data(bg_sync.check_file_exists(output_file))

# ### read output_file
if output_file_exists:
    resp = bg_sync.read_file(output_file)
    df_homepage = bg_sync.get_response_data(resp)
    df_homepage = pd.DataFrame(df_homepage)
"""
df_homepage[['entity_name', 'url']].to_dict('records')
[{'entity_name': 'Vedanta Limited', 'url': 'www.vedantalimited.com'}, {'entity_name': 'GHG protocol', 'url': 'www.ghgprotocol.org'}, {'entity_name': 'World Bank', 'url': 'www.worldbank.org'}, {'entity_name': 'World Bank', 'url': 'data.worldbank.org'}]
Notice that in case an organisation has multiple websites that show up in the search, as is often the case, 
the results will have multiple homepages, as is the case with World Bank in this example.
"""

# ## Search organisation homepage

# ### set keyphrases to search
keyphrases = [
    'sustainability',
    'materiality assessment',
]

# ### select urls to search from
selected_urls = df_homepage['url'].unique().tolist()[:1]

# ### trigger search
responses = []
for url_num, selected_url in enumerate(selected_urls):
    logger.info(f"searching {selected_url} ({url_num}/{len(selected_urls)})")
    resp = bg_async.search_web(
        keyphrases=keyphrases,
        site=selected_url
    )
    responses = responses + [resp]

# ### wait for output to be ready
time.sleep(15 * 60)

# ### read search results
df_search = pd.DataFrame()
missing_files = []
for resp_num, resp in enumerate(responses):
    logger.info(f"processing response: {resp_num}/{len(responses)}")
    ## get output file
    output_file = bg_sync.get_response_output_file(resp)
    ## check if output file exists
    output_file_exists = bg_sync.check_file_exists(output_file)
    ## if output file already exists
    if output_file_exists:
        logger.info(f"{output_file} exists: reading it")
        ## read output file
        df_search_ = bg_sync.get_response_data(bg_sync.read_file(output_file))
        ## convert output to df
        df_search_ = pd.DataFrame(df_search_)
        ## add to df_search
        df_search = pd.concat([df_search, df_search_])
    ## if output file does not yet exist
    else:
        logger.warning(f"{output_file} does not exists: storing it to missing files")
        ## add it to missing files
        missing_files = missing_files + [output_file]