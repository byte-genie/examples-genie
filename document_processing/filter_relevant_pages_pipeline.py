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
filtered_pages = [resp.get_output() for resp in filter_pages_responses]

# <h2> Check filtered_pages </h2>
# <p> Because `filtered_pages` is the output of a pipeline, it retains output from all the steps in that pipeline.
# It's output will be in the form a of dictionary, with each element of the dictionary containing output files
# from one step of the pipeline. </p>

# <h3> Types of output available in filtered_pages </h3>
logger.info(f"Output keys: {list(filtered_pages[0].keys())}")
"""
<div>
<p> 
Output keys in `filtered_pages` for one of the documents, `list(filtered_pages[0].keys())}`
[
    'filtered_files', 'filtered_table_files', 'filtered_text_files', 'img_files', 'table_embedding_files',
    'table_files', 'table_similarity_files', 'text_embedding_files', 'text_files', 'text_similarity_files'
]
Different keys in this dictionary contain outputs from different steps. 
For example, 
<ul>
    <li> 'img_files' contains output from converting PDF document to page images; </li>
    <li> 'text_files' contains text output files extracted via OCR and layout parsing; </li> 
    <li> 'text_embedding_files' contains text embedding files; </li>
</ul>
</p>
<p>
Sample of image files, `filtered_pages[0]['img_files']`
[
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-0.png',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1.png',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2.png',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3.png',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-4.png'
]
</p>
<p> Sample of text files, `filtered_pages[0]['text_files']`
[
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-emission-reductions.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-emission-targets.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-gender-diversity.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-renewable-energy.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-sustainable-revenue.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-emission-reductions.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-emission-targets.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-gender-diversity.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-renewable-energy.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-sustainable-revenue.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-emission-reductions.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-emission-targets.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-gender-diversity.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-renewable-energy.csv',
    'gs://db-genie/entity_type=url/entity=httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/httpsmultimedia3mcommwsmedia2053960o3m-pulp-and-paper-sourcing-policy-progress-report-may-2021-finalpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-sustainable-revenue.csv'
]
</p>
</div>
"""

