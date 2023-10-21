# <h1> Filter pages from documents most relevant to KPIs of interest </h1>
"""
<p>In this example, we will identify the most relevant text and table files extracted from a few documents,
using the page filtering pipeline, which combines a number of steps into a single API call.
To see a step-by-step approach for filtering pages, see `document_processing/filter_relevant_pages.py`</p>
"""


# ## import necessary libraries

import os
import time
import uuid
import numpy as np
import pandas as pd
import utils.common
import utils.async_utils
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    overwrite=0,
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground)
bg_sync = ByteGenie(
    secrets_file='secrets_mcp.json',
    task_mode='sync',
    overwrite=0,
    verbose=1,
)


# <h2> Set inputs <h2>

# <h3> Set documents to process <h3>
doc_names = [
    'httpsmultimedia3mcommwsmedia2006066o2021-sustainability-reportpdf',
    'httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf',
    'httpsmultimedia3mcommwsmedia2292786o3m-2023-global-impact-reportpdf',
]

# <h3> Set keyphrases by which to filter pages <h3>

keyphrases = [
    'emission targets',
    'emission reductions',
    'hazardous waste',
    'gender diversity',
    'renewable energy',
    'sustainable revenue'
]

# <h3> Set maximum rank of pages to keep </h3>
file_rank_max = 3
# <p> `file_rank_max=3` will mean that after the files are ranked by relevance to keyphrases,
# only the top 3 ranked files will be kept for each keyphrase. </p>

# <h2> Filter pages </h2>

tasks = [
    bg_async.async_filter_pages_pipeline(
        doc_name=doc_name,
        keyphrases=keyphrases,
        file_rank_max=file_rank_max
    )
    for doc_name in doc_names
]
filter_pages_responses = utils.async_utils.run_async_tasks(tasks)
df_filtered_pages = [resp.get_output() for resp in filter_pages_responses]
df_filtered_pages = [pd.DataFrame(df) for df in df_filtered_pages]
df_filtered_pages = pd.concat(df_filtered_pages)