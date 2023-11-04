# # Extract quants from pages, and estimate KPI values
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
    'httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf',
    'httpsinvestorcapitalandcomnewsroom20200528_064125_c31_fr0qtlme4idq6s8c1pdf',
    'httpsinvestorcapitalandcomnewsroom20190529_171331_c31_yt1rnqd01ipsmsa11pdf',
]

# <h3> Set keyphrases by which to filter pages <h3>

keyphrases = [
    'emission targets',
    'emission reductions',
    'emissions by scope',
    'hazardous waste',
    'gender diversity',
    'renewable energy consumption',
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
structured_quants = [resp.get_output(refresh=1) for resp in structure_quants_responses]


# <h2> Check structured_quants </h2>
# <p> Because `structured_quants` is the output of a pipeline, it retains output from all the steps in that pipeline.
# Its output will be in the form a of dictionary, with each element of the dictionary containing output files
# from one step of the pipeline. </p>

# <h3> Types of output available in filtered_pages </h3>
logger.info(f"Output keys: {list(structured_quants[0].keys())}")
"""
<div>
<p> 
Output keys in `structured_quants` for one of the documents, `list(structured_quants[0].keys())`
[
    'estimated_value_files', 
    'filtered_files', 
    'filtered_table_files', 
    'filtered_text_files', 
    'img_files',
    'page_data_files', 
    'page_quants_files', 
    'table_embedding_files', 
    'table_files', 
    'table_similarity_files',
    'text_embedding_files', 
    'text_files', 
    'text_similarity_files'
]
Different keys in this dictionary contain outputs from different steps. 
For example, 
<ul>
    <li> 'img_files' contains output from converting PDF document to page images; </li>
    <li> 'text_files' contains text output files extracted via OCR and layout parsing; </li> 
    <li> 'text_embedding_files' contains text embedding files; </li>
    <li> 'page_quants_files' contains structured quants data; </li>
    <li> 'estimated_value_files' contains estimates for specific keyphrses (KPIs). </li>
</ul>
</p>
<p>
Sample of image files, `structured_quants[0]['img_files']`
[
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-0.png',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-1.png',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-2.png',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-3.png',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-4.png'
]
</p>
<p> Sample of text files, `structured_quants[0]['page_quants_files']`
[
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-0_page-quants_structured-quant-summary.csv',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-1_page-quants_structured-quant-summary.csv',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-2_page-quants_structured-quant-summary.csv',
    'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-3_page-quants_structured-quant-summary.csv'
]
</p>
<p> Sample of estimated values files, `structured_quants[0]['estimated_value_files']`
[
    [
        'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-0_page-quants_structured-quant-summary_estimated-values_metrics-emission-reductions_emission-targets_emissions-by-scope_renewable-energy-consumption_sustainable-revenue.csv'
    ],
    [
        'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-1_page-quants_structured-quant-summary_estimated-values_metrics-emission-reductions_emission-targets_emissions-by-scope_gender-diversity_hazardous-waste_renewable-energy-consumption_sustainable-revenue.csv'
    ],
    [
        'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-2_page-quants_structured-quant-summary_estimated-values_metrics-emission-reductions_emission-targets_emissions-by-scope_gender-diversity_hazardous-waste_renewable-energy-consumption_sustainable-revenue.csv',
    ],
    [
        'gs://db-genie/entity_type=url/entity=httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/httpswwwcapitalandcomcontentdamcapitaland-newsroominternational2023maycl-gsr-smpcapitaland_investment_elevates_its_esg_efforts_with_refreshed_2030_sustainability_master_planpdf_pagenum-3_page-quants_structured-quant-summary_estimated-values_metrics-gender-diversity_hazardous-waste.csv'
    ]
]
`estimated_value_files` are organised by page number, with each page containing the estimates for keyphrases to which it was sufficiently relevant to.
</p>
</div>
"""