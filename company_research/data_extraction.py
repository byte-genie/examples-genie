"""
Extract relevant data from process documents
"""

import time
import uuid
import numpy as np
import pandas as pd
from utils.logging import logger
from utils.byte_genie import ByteGenie


## init byte-genie in async mode (tasks will run in the background)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    verbose=1,
)
## init byte-genie in sync mode (tasks will run in the foreground)
bg_sync = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    verbose=1,
)
## read reports from saved file
df_reports = pd.read_csv(f"/tmp/reports_data_2023-08-21.csv")
## filter documents from 2021 onward
df_reports = df_reports[df_reports['doc_year'].map(float) > 2021].reset_index(drop=True)
## trigger processing for a couple of documents
doc_names = df_reports['doc_name'].tolist()[:5]
## define a set of relevant keyphrases to search in extracted quants from documents
keyphrases = ['emissions by scope', 'energy consumption', 'water consumption']
## set type of data to rank (quantitative or qualitative)
attr_type = 'quantitative'
## set the fraction of rows to keep in ranked data
frac_rows_to_keep = 0.1
## from each document, rank quants by relevance to set of keyphrases
responses = []
for doc_num, doc_name in enumerate(doc_names):
    for keyphrase_num, keyphrase in enumerate(keyphrases):
        logger.info(f"{doc_name} ({doc_num}/{len(doc_names)}); "
                    f"{keyphrase} ({keyphrase_num}/{len(keyphrases)})")
        resp = bg_async.write_ranked_data(
            doc_name=doc_name,
            attr=keyphrase,
            attr_type=attr_type,
            # frac_rows_to_keep=frac_rows_to_keep,
        )
        responses = responses + [resp]
## check status of output files
df_ranked = pd.DataFrame()
files_not_exist = []
for resp_num, resp in enumerate(responses):
    logger.info(f"reading response number {resp_num}")
    ## get output file
    output_file = bg_sync.get_response_output_file(resp)
    ## check if the file exists
    output_file_exists = bg_sync.get_response_data(
        bg_sync.check_file_exists(
            output_file
        )
    )
    ## if the file exists
    if output_file_exists:
        ## get ranked data file
        ranked_data_file = bg_sync.get_response_data(bg_sync.read_file(output_file))
        ## read ranked data file
        df_ = bg_sync.get_response_data(bg_sync.read_file(ranked_data_file))
        ## convert to dataframe
        df_ = pd.DataFrame(df_)
        ## add data to df_ranked
        df_ranked = pd.concat([df_ranked, df_])
    ## if the file does not exist
    else:
        ## save file to files_not_exist
        files_not_exist = files_not_exist + [output_file]
## check df_ranked
logger.info(f"df_ranked columns: {list(df_ranked.columns)}")
"""
list(df_ranked.columns)
['category', 'company name', 'context', 'date', 'doc_name', 'doc_org', 'doc_type', 'doc_year', 'pagenum', 'query', 'row_id', 'score', 'unit', 'value', 'variable', 'variable description', 'payload', 'error']
"""
logger.info(f"df_ranked documents: {list(df_ranked['doc_name'].unique().tolist())}")
"""
list(df_ranked['doc_name'].unique().tolist())
['httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', nan, 'httpspetrobrascombrdatafiles3755fb5bb3438810819c6568b8e99ea8cdhcc_2022_engpdf', 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf']
"""
logger.info(f"df_ranked input queries: {list(df_ranked['query'].unique().tolist())}")
"""
list(df_ranked['query'].unique().tolist())
['emissions by scope', 'energy consumption', 'water consumption', nan]
"""
logger.info(f"df_ranked data sample: {df_ranked[['company name', 'category', 'variable description', 'variable', 'value', 'unit', 'date', 'doc_name']].tail().to_dict('records')}")
"""
df_ranked[['company name', 'category', 'variable description', 'variable', 'value', 'unit', 'date', 'doc_name']].tail().to_dict('records')
[{'company name': 'Vedanta', 'category': 'Governance Body Diversity', 'variable description': 'Female representation in Total (Governance Body)', 'variable': 'Total', 'value': '204', 'unit': '', 'date': 'May 2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Vedanta', 'category': 'Environment', 'variable description': 'Installed capacity of renewable energy through a Power Distribution Agreement', 'variable': 'Renewable Energy', 'value': '580 MW', 'unit': 'Megawatts (MW)', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Vedanta', 'category': 'Energy Efficiency', 'variable description': '20% intensity reduction in metals and mining segment from a FY2021 baseline', 'variable': 'Energy Intensity Reduction', 'value': '20%', 'unit': '%', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Vedanta', 'category': 'Crude Oil Production', 'variable description': "Operates c.25% of India's crude oil production", 'variable': 'Average Daily Gross', 'value': '161 kboepd', 'unit': '', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Thermal', 'category': 'Avoid', 'variable description': 'Acquire up to 20% of biomass-based plants', 'variable': 'Biomass-based plants', 'value': 'Up to 20%', 'unit': '', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}]
"""
## filter data over 'emissions by scope'
df_emissions = df_ranked[df_ranked['query'] == 'emissions by scope'].reset_index(drop=True)
## remove rows with empty values
df_emissions = df_emissions[~df_emissions['value'].isin(['', np.nan, None, 'nan', 'n/a'])]
## fill na values
df_emissions = df_emissions.fillna('')
## set custom attributes to extract for emissions
emission_attrs = [
    'description of emissions',
    'scope of emissions',
    'unit of emissions',
    'amount of emissions',
    'source of emissions',
    'date of emissions',
    'company name'
]
## create a customised dataset for emissions
resp = bg_sync.create_dataset(
    data=df_emissions[:10].to_dict('records'),
    attrs=emission_attrs,
    cols_to_use=['category', 'company name', 'doc_org', 'date', 'unit', 'value', 'variable', 'variable description']
)
df_ = pd.DataFrame(bg_sync.get_response_data(resp))
df_.values
df_[['row_num', 'value', 'variable']].values
custom_emissions_output_file = bg_sync.get_response_output_file(resp)