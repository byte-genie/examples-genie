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

# ### get doc_info data
missing_files = []
df_doc_info = pd.DataFrame()
for resp_num, resp in enumerate(responses):
    logger.info(f"Reading document info for ({resp_num}/{len(responses)})")
    if resp.check_output_file_exists():
        df_doc_info_ = pd.DataFrame(resp.read_output_data())
        df_doc_info = pd.concat([df_doc_info, df_doc_info_])
    else:
        missing_files = missing_files + resp.get_output_file()

# ### check df_doc_info
logger.info(f"{len(df_doc_info)} rows found in df_doc_info")
logger.info(f"{len(missing_files)} files with missing document info")

# ## save document info
bg_sync.upload_data(
    username=bg_sync.read,
    contents=[df_doc_info],
    filenames=['downloaded-docs_cement-companies.csv']
)

# ## Filter documents

# ### get unique doc_year
doc_years = df_doc_info['doc_year'].unique().tolist()

# ### convert years to numeric
num_years = []
for yr in doc_years:
    logger.info(f"convrting {yr} to numeric")
    if isinstance(yr, float) or isinstance(yr, int):
        yr_num = yr
    else:
        yr_num = bg_sync.extract_text_years(str(yr)).get_data()
    num_years = num_years + [yr_num]


# ### add numeric years to df_doc_info
df_doc_info['doc_year_num'] = num_years

# ### filter over recent years
df_doc_info = df_doc_info[df_doc_info['doc_year_num'] > 2021]

# ## trigger processing