# <h1> Extract quants from pages, and estimate KPI values </h1>
"""
<p> In this example, we will use a quant structuring pipeline to extract quant metrics,
and estimate values for our KPIs of interest.
The quant structuring pipeline combines a number of steps into a single API call.
See `document_processing/extract_quants_and_estimate_kpis.py` for a step-by-step approach to extracting quants,
and estimate values for relevant KPIs. </p>
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
    # 'httpsmultimedia3mcommwsmedia2006066o2021-sustainability-reportpdf',
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

# <h3> Attributes to estimate for each KPI </h3>
"""
<p>These are the attributes that will be extract for each KPI from the documents.</p>
"""
attrs_to_estimate = [
    'company name',
    'quantity name',
    'quantity description',
    'quantitative value',
    'unit or currency of value',
    'date'
]


# <h3> Set maximum rank of pages to keep </h3>
file_rank_max = 3
# <p> `file_rank_max=3` will mean that after the files are ranked by relevance to keyphrases,
# only the top 3 ranked files will be kept for each keyphrase. </p>


# <h2> Estimate KPI values </h2>

tasks = [
    bg_async.async_structure_quants_pipeline(
        doc_name=doc_name,
        keyphrases=keyphrases,
        file_rank_max=file_rank_max,
        attrs_to_estimate=attrs_to_estimate,
    )
    for doc_name in doc_names
]
structure_quants_responses = utils.async_utils.run_async_tasks(tasks)
structured_quants = [resp.get_output() for resp in structure_quants_responses]
