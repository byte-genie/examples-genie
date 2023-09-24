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
    df_search = pd.DataFrame(df_search)
else:
    logger.info(f"search output is not complete yet: wait some more")

# ### check df_search
logger.info(f"shape of df_search: {df_search.shape}")
logger.info(f"df_search.columns: {list(df_search.columns)}")
logger.info(f"keyphrases in search results: {df_search['keyphrase'].unique().tolist()}")
"""
list(df_search.columns)
['href', 'href_type', 'keyphrase', 'result_html', 'result_text']
"""

# ## Download search results

# ### download html pages from result URLs
download_url_resp = bg_async.download_file(
    urls=list(df_search['href'].unique()),
)

# ### get document names for downloaded files
if download_url_resp.check_output_file_exists():
    urls = download_url_resp.read_output_data()
else:
    logger.info(f"url download output is not yet complete: wait some more")

# ### check df_urls
logger.info(f"downloaded urls: {urls}")
"""
First 4 downloaded URL files, urls[:4]:
[
    'gs://db-genie/entity_type=url/entity=httpswwwmanageenginecommobile-device-managementpricinghtml/data_type=unstructured/format=html/variable_desc=document/source=manageengine.com/httpswwwmanageenginecommobile-device-managementpricinghtml.html', 
    'gs://db-genie/entity_type=url/entity=httpswwwzohocomanalyticsonpremise-pricinghtml/data_type=unstructured/format=html/variable_desc=document/source=zoho.com/httpswwwzohocomanalyticsonpremise-pricinghtml.html', 
    'gs://db-genie/entity_type=url/entity=httpswwwneenopalcompricinganalyticshtml/data_type=unstructured/format=html/variable_desc=document/source=neenopal.com/httpswwwneenopalcompricinganalyticshtml.html', 
    'gs://db-genie/entity_type=url/entity=httpswwwsapcomcanadaproductscrmcustomer-data-platformpricinghtml/data_type=unstructured/format=html/variable_desc=document/source=sap.com/httpswwwsapcomcanadaproductscrmcustomer-data-platformpricinghtml.html'
]
"""

# ## Process downloaded URL files
for file_num, file in enumerate(urls):
    logger.info(f"processing ({file_num}/{len(urls)}): {file}")
    ## get doc_name
    doc_name = file.split('entity=')[-1].split('/')[0]
    ## extract text
    bg_async.extract_text(
        doc_name=doc_name
    )