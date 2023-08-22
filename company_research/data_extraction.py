# # Extract relevant data from processed documents

# ## import necessary libraries
import time
import uuid
import numpy as np
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

# ## read processed documents (input data)

# ### read reports from saved file
df_reports = pd.read_csv(f"/tmp/reports_data_2023-08-21.csv")

# ### filter documents from 2021 onward
df_reports = df_reports[df_reports['doc_year'].map(float) > 2021].reset_index(drop=True)

# ### select first 5 documents
doc_names = df_reports['doc_name'].tolist()[:5]

# ## Define inputs

# ### define a set of relevant keyphrases to search in extracted quants from documents
keyphrases = ['emissions by scope', 'energy consumption', 'water consumption']
# ### set type of data to rank (quantitative or qualitative)
attr_type = 'quantitative'
# ### set the fraction of rows to keep in ranked data
frac_rows_to_keep = 0.1

# ## Search/rank data

# ### from each document, rank quants by relevance to set of keyphrases
responses = []
for doc_num, doc_name in enumerate(doc_names):
    for keyphrase_num, keyphrase in enumerate(keyphrases):
        logger.info(f"{doc_name} ({doc_num}/{len(doc_names)}); "
                    f"{keyphrase} ({keyphrase_num}/{len(keyphrases)})")
        resp = bg_async.write_ranked_data(
            doc_name=doc_name,
            attr=keyphrase,
            attr_type=attr_type,
            frac_rows_to_keep=frac_rows_to_keep,
        )
        responses = responses + [resp]

# ### wait for output to be ready
time.sleep(15 * 60)

# ### read ranked data output
df_ranked = pd.DataFrame()
files_not_exist = []
for resp_num, resp in enumerate(responses):
    logger.info(f"reading response number {resp_num}/{len(responses)}")
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

# ### check files that do not yet exist
files_not_exist
"""
files_not_exist
[]
"""

# ### check df_ranked
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

logger.info(
    f"df_ranked data sample: {df_ranked[['company name', 'category', 'variable description', 'variable', 'value', 'unit', 'date', 'doc_name']].tail().to_dict('records')}")
"""
df_ranked[['company name', 'category', 'variable description', 'variable', 'value', 'unit', 'date', 'doc_name']].tail().to_dict('records')
[{'company name': 'Vedanta', 'category': 'Governance Body Diversity', 'variable description': 'Female representation in Total (Governance Body)', 'variable': 'Total', 'value': '204', 'unit': '', 'date': 'May 2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Vedanta', 'category': 'Environment', 'variable description': 'Installed capacity of renewable energy through a Power Distribution Agreement', 'variable': 'Renewable Energy', 'value': '580 MW', 'unit': 'Megawatts (MW)', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Vedanta', 'category': 'Energy Efficiency', 'variable description': '20% intensity reduction in metals and mining segment from a FY2021 baseline', 'variable': 'Energy Intensity Reduction', 'value': '20%', 'unit': '%', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Vedanta', 'category': 'Crude Oil Production', 'variable description': "Operates c.25% of India's crude oil production", 'variable': 'Average Daily Gross', 'value': '161 kboepd', 'unit': '', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}, 
{'company name': 'Thermal', 'category': 'Avoid', 'variable description': 'Acquire up to 20% of biomass-based plants', 'variable': 'Biomass-based plants', 'value': 'Up to 20%', 'unit': '', 'date': '2022', 'doc_name': 'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf'}]
"""

# ## Create emissions dataset

# ### filter data over 'emissions by scope'
df_emissions = df_ranked[df_ranked['query'] == 'emissions by scope'].reset_index(drop=True)

# ### remove rows with empty values
df_emissions = df_emissions[~df_emissions['value'].isin(['', np.nan, None, 'nan', 'n/a'])]

# ### fill na values
df_emissions = df_emissions.fillna('')

# ## Save data

# ### save data locally
df_emissions.to_csv('/tmp/emissions_sample.csv', index=False)

# ## Create custom emissions dataset

# ## read locally saved data
df_emissions = pd.read_csv('/tmp/emissions_sample.csv')

# ## fill na values
df_emissions = df_emissions.fillna('')

# ### set custom attributes to extract for emissions
emission_attrs = [
    'amount or value of emissions',
    'description of emissions',
    'scope of emissions',
    'unit of measurement',
    'source of emissions',
    'date of emissions',
    'company name'
]

# ### create a customised dataset for emissions
resp = bg_sync.create_dataset(
    data=df_emissions.to_dict('records'),
    attrs=emission_attrs,
    cols_to_use=['category', 'company name', 'doc_org', 'date', 'unit', 'value', 'variable', 'variable description'],
    groupby_cols=['doc_name'],
)

# ### get output data
df_emissions_custom = pd.DataFrame(bg_sync.get_response_data(resp))

# ### pivot data
df_emissions_custom = df_emissions_custom.pivot(
    index=['context', 'row_num'],
    columns='variable',
    values='value',
).reset_index()

# ### check columns
logger.info(f"df_emissions_custom.columns: {list(df_emissions_custom.columns)}")
"""
list(df_emissions_custom.columns)
['context', 'row_num', 'amount of emissions', 'company name', 'date of emissions', 'description of emissions', 'relevant quote', 'scope of emissions', 'source of emissions', 'unit of measurement']
"""

# ### check data sample
logger.info(f"df_emissions_custom sample: "
            f"{df_emissions_custom.drop(columns=['context', 'row_num']).head().to_dict('records')}")
"""
df_emissions_custom.drop(columns=['context', 'row_num']).head().to_dict('records')
[{'amount of emissions': '-18%', 'company name': 'BR PETROBRAS', 'date of emissions': '2015.0', 'description of emissions': 'Total operating emissions (Scope 1 and 2) from our O&G activities', 'relevant quote': 'Total operating emissions (Scope 1 and 2) from our O&G activities have shown a continuous downward trend over the last few years', 'scope of emissions': 'Scope 1 and 2', 'source of emissions': 'BR PETROBRAS', 'unit of measurement': 'tCO2e'}, 
{'amount of emissions': '61.8', 'company name': 'BR PETROBRAS', 'date of emissions': '2019.0', 'description of emissions': 'Indirect emissions related to the use of sold products', 'relevant quote': 'Indirect emissions related to the use of sold products reported for the value chain', 'scope of emissions': 'Scope 3', 'source of emissions': 'BR PETROBRAS', 'unit of measurement': 'n/a'}, 
{'amount of emissions': '400.2', 'company name': 'BR PETROBRAS', 'date of emissions': '2017.0', 'description of emissions': 'Indirect emissions related to the use of sold products', 'relevant quote': 'Indirect emissions related to the use of sold products reported for the value chain', 'scope of emissions': 'Scope 3', 'source of emissions': 'BR PETROBRAS', 'unit of measurement': 'n/a'}, 
{'amount of emissions': '436 million', 'company name': 'BR PETROBRAS', 'date of emissions': '2021.0', 'description of emissions': 'Combined emissions from categories 10 and 11', 'relevant quote': 'Combined emissions from categories 10 and 11', 'scope of emissions': 'Scope 3', 'source of emissions': 'BR PETROBRAS', 'unit of measurement': 'tCO2e'}, 
{'amount of emissions': 'n/a', 'company name': 'BR PETROBRAS', 'date of emissions': '2022.0', 'description of emissions': 'Scope 2 emissions have low materiality', 'relevant quote': 'Scope 2 emissions have low materiality', 'scope of emissions': 'Scope 2', 'source of emissions': 'BR PETROBRAS', 'unit of measurement': 'n/a'}]
"""

# ### check original context for each row of data
logger.info(f"df_emissions_custom original context: "
            f"{df_emissions_custom['context'].unique().tolist()}")
"""
df_emissions_custom['context'].unique().tolist()
['
    [
        ["category", "company name", "doc_org", "date", "unit", "value", "variable", "variable description"], 
        ["Operating Emissions", "BR PETROBRAS", "BR PETROBRAS", 2015.0, "", "-18%", "Scope 1 and 2", "Total operating emissions (Scope 1 and 2) from our O&G activities have shown a continuous downward trend over the last few years"], 
        ["Scope 3", "BR PETROBRAS", "BR PETROBRAS", 2019.0, "", "61.8", "Emissions-Category 11", "Indirect emissions related to the use of sold products reported for the value chain"], 
        ["Scope 3", "BR PETROBRAS", "BR PETROBRAS", 2017.0, "", "400.2", "Emissions-Category 11", "Indirect emissions related to the use of sold products reported for the value chain"], 
        ["Scope 3", "BR PETROBRAS", "BR PETROBRAS", 2021.0, "tCO2e", "436 million", "Emissions", "Combined emissions from categories 10 and 11"], 
        ["Emissions", "BR PETROBRAS", "BR PETROBRAS", 2022.0, "", "Low materiality", "Scope 2", "Scope 2 emissions have low materiality"], 
        ["Emissions", "BR PETROBRAS", "BR PETROBRAS", 2021.0, "", "99%", "Scope 1", "Scope 1 emissions represented 99% of operational emissions in 2021"], 
        ["Scope 3", "BR PETROBRAS", "BR PETROBRAS", 2015.0, "", "459.9", "Emissions-Category 11", "Indirect emissions related to the use of sold products reported for the value chain"], 
        ["Operational Emissions (Scope 1 and 2)", "BR PETROBRAS", "BR PETROBRAS", 2015.0, "", "18%", "Reduction in absolute emissions", "Reduction in operational emissions without thermoelectricity by 18% since 2015"], 
        ["Scope 3", "BR PETROBRAS", "BR PETROBRAS", 2016.0, "", "437.2", "Emissions-Category 10", "Indirect emissions from processing sold products reported for the value chain"], 
        ["GHG Emissions", "BR PETROBRAS", "BR PETROBRAS", 2030.0, "kgCO2e/boe", "15.0", "Intensity (2030)", "Likely GHG emissions intensity target for an unspecified company in 2030"]
    ]', 
]
"""

# ## Save custom emissions data
df_emissions_custom.to_csv('/tmp/custom_emissions_data.csv', index=False)