# # Extract specific KPIs from documents ((UNDER DEVELOPMENT)

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
    secrets_file='secrets_mcp.json',
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

# ## Set inputs

# ### Set documents
doc_names = ['userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'userid_stuartcullinan_uploadfilename_jason_09_srpdf', 'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf', 'userid_stuartcullinan_uploadfilename_anastasia_19_china_east_education_ltd_20211228164502_62371643_enpdf', 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf', 'userid_stuartcullinan_uploadfilename_28_kim_cartapdf', 'userid_stuartcullinan_uploadfilename_karishma-03-lse_rav_2020pdf', 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf', 'userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf', 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf', 'userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf', 'userid_stuartcullinan_uploadfilename_jeon_22_boliden_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf', 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'userid_stuartcullinan_uploadfilename_jaime_allianz-group_sustainability-reportpdf', 'userid_stuartcullinan_uploadfilename_jason_14_srpdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-annual-financial-resultspdf', 'userid_stuartcullinan_uploadfilename_jeon_27_ecolab_corporate-responsibility-report_2021pdf', 'userid_stuartcullinan_uploadfilename_16_samsung_sdspdf', 'userid_stuartcullinan_uploadfilename_jeon_26_bayer_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_al_9_webuild_ethics_code_1pdf', 'userid_stuartcullinan_uploadfilename_anastasia_4_-2020-aggreko-annual-reportpdf', 'userid_stuartcullinan_uploadfilename_12_ashteadgroup_mrpdf', 'userid_stuartcullinan_uploadfilename_al_6_kier-2021-ara-finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf', 'userid_stuartcullinan_uploadfilename_4_kim_cartapdfpdf', 'userid_stuartcullinan_uploadfilename_3_cgcpdf', 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_1_adesso_sepdfpdf', 'userid_stuartcullinan_uploadfilename_jason_08_srpdf', 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf', 'userid_stuartcullinan_uploadfilename_12_argo_blockchainpdfpdf', 'userid_stuartcullinan_uploadfilename_13_capita_mrpdf', 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf', 'userid_stuartcullinan_uploadfilename_5_compass-group_mrpdf', 'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_uk-pay-gap-reportpdf', 'userid_stuartcullinan_uploadfilename_karishma-04-sustainability-highlights-report-2021-19-finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-01-des-annualreport-2021-e-spdf', 'userid_stuartcullinan_uploadfilename_al_9_relazione-governance-2021-final_eng-con-tabellepdf', 'userid_stuartcullinan_uploadfilename_jeon_07_a2-milk-company_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jason_14_gpgpdf', 'userid_stuartcullinan_uploadfilename_karishma-04-savills-plc-ar21pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-greenhouse-gas-inventory-report_finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-new-zealand-sustainability-report-2020pdf']

# ### Set KPIs
kpis = [
    '% of female representation on the board',
    'hazardous waste',
    'gender pay gap',
    'GHG Scope 1 emissions',
    'GHG Scope 2 emissions',
    'GHG Scope 3 emissions',
    'Non-renewable energy consumption',
    'Emissions to water',
    'Percentage of non-renewable energy production',
    'anti-corruption policies',
    'anti-bribery policies',
]

# ### Read filtered table similarity files
"""
See `document_processing/filter_relevant_pages.py` to see how these pages were filtered
"""
df_filtered_table_sim_files = pd.read_csv(f"/tmp/df_filtered_table_sim_files.csv")
# df_filtered_table_sim_files = pd.read_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_filtered_table_sim_files.csv")


# ## Create datasets for each KPI
"""
Now, we will use the filtered tabular files to estimate the value for each one of our KPIs from each document, 
based on the data contained in the filtered file.
"""

# ### Set attributes to extract for each KPI
kpi_attrs = {
    '% of female representation on the board': [
        'company name', 'date', '% of female representation on the board',
        'any details about the female representation on the board'
    ],
    'hazardous waste': [
        'company name', 'date', 'hazardous waste amount', 'unit of measurement',
        'any details of hazardous waste'
    ],
    'gender pay gap': [
        'company name', 'date', 'gender pay gap', 'any description of gender pay gap'
    ],
    'GHG Scope 1 emissions': [
        'company name', 'date', 'amount of emissions', 'scope of emissions',
        'unit of measurement', 'any details of emissions'
    ],
    'GHG Scope 2 emissions': [
        'company name', 'date', 'amount of emissions', 'scope of emissions',
        'unit of measurement', 'any details of emissions'
    ],
    'GHG Scope 3 emissions': [
        'company name', 'date', 'amount of emissions', 'scope of emissions',
        'unit of measurement', 'any details of emissions'
    ],
    'Non-renewable energy consumption': [
        'company name', 'date', 'amount of energy consumption',
        'renewable or non-renewable flag', 'unit of measurement',
        'any details of energy consumption'
    ],
    'Emissions to water': [
        'company name', 'date', 'amount of emissions to water', 'unit of measurement',
        'any details of emissions to water'
    ],
    'Percentage of non-renewable energy production': [
        'company name', 'date', 'amount of energy production',
        'renewable or non-renewable flag', 'unit of measurement',
        'any details of energy production'
    ],
    'anti-corruption policies': [
        'company name', 'complete description of anti-corruption policies',
        'summary of anti-corruption policies'
    ],
    'anti-bribery policies': [
        'company name', 'complete description of anti-bribery policies',
        'summary of anti-bribery policies'
    ],
}

# ### for each doc_org_std, identify the top 2 orig_table_file with highest score
df_filtered_table_sim_files['file_rank'] = df_filtered_table_sim_files.groupby(
    by=['doc_org_std', 'query'],
    group_keys=False,
)['score'].rank('dense', ascending=False)

# ### save df_filtered_table_sim_files locally
df_filtered_table_sim_files.to_csv(f"/tmp/df_filtered_table_sim_files.csv", index=False)

# ### process top 2 ranked files first
files_to_process = \
    df_filtered_table_sim_files[
        df_filtered_table_sim_files['file_rank'] <= 2
        ]['orig_table_file'].unique().tolist()
logger.info(f"Number of files to process: {len(files_to_process)}")
"""
Number of files to process: `len(files_to_process)`: 380
"""
for file_num, file in enumerate(files_to_process):
    logger.info(f"processing ({file_num}/{len(files_to_process)}): {file}")
    ## get queries matching this file
    queries = \
        df_filtered_table_sim_files[df_filtered_table_sim_files['orig_table_file'] == file]['query'].unique().tolist()
    ## get corresponding attributes to extract for each query
    attrs_to_extract = [
        kpi_attrs[kpi] for kpi in queries
    ]
    attrs_to_extract = pd.DataFrame(attrs_to_extract)
    ## extract the same set of attrs only once
    attrs_to_extract = attrs_to_extract.drop_duplicates().reset_index(drop=True)
    attrs_to_extract = attrs_to_extract.values.tolist()
    ## remove None attributes
    attrs_to_extract = [[value for value in sublist if value is not None] for sublist in attrs_to_extract]
    ## create tasks to extract all possible attributes from this file
    tasks = [
        bg_async.async_create_dataset(
            file=file,
            attrs=attrs_to_extract_,
        )
        for attrs_to_extract_ in attrs_to_extract
    ]
    ## run tasks
    attr_extraction_responses = utils.async_utils.run_async_tasks(tasks)
    ## wait for 15 sec to avoid rate limits
    time.sleep(30)
"""
`/create_dataset` will write dataset file with extracted attributes in files with path `.../data_type=dataset/...csv`
"""

# ## Verify extracted datasets
"""
Extracted dataset files may have some errors, so we can run one layer of verification to remove such errors. 
For verification, we will use `/verify_data` endpoint, which allows to verify (variable, value) pair, given a context. 
We will focus on verifying the most important attributes in each dataset, such as 'hazardous waste amount', 'amount of emissions', etc. 
"""

# ### define tasks
tasks = [
    bg_async.async_verify_data(
        doc_name=doc_name,
        file_pattern='data_type=dataset/**/variable_desc=orig-table/**.csv',
        var_col='variable',
        val_col='value',
        context_col='context',
        verification_method='lm-verification',
        verification_type='variable-value',
        output_data_type='verification',
        vars_to_verify=[
            'gender pay gap',
            '% of female representation on the board',
            'hazardous waste amount',
            'amount of emissions to water',
            'amount of emissions',
            'amount of energy production',
        ],
    )
    for doc_name in doc_names
]
verify_dataset_responses = utils.async_utils.run_async_tasks(tasks)

# ### list dataset files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=dataset/**/variable_desc=orig-table/**.csv"
    )
    for doc_name in doc_names
]
tabular_dataset_files = utils.async_utils.run_async_tasks(tasks)
tabular_dataset_files = [resp.get_output() for resp in tabular_dataset_files if resp.get_output() is not None]
"""
Number of documents for which dataset files are available, `len(tabular_dataset_files)`: 48
Dataset files for the first document, `tabular_dataset_files[0]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=dataset/format=csv/variable_desc=orig-table/source=0ce43925c073c45c71d798c13d00d781/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=dataset/format=csv/variable_desc=orig-table/source=2bbdd7d5532b826b7542438b805b1b7a/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-0.csv'
]
"""
# ### Flatten tabular_dataset_files
tabular_dataset_files = [file for files in tabular_dataset_files for file in files]
tabular_dataset_files = list(set(tabular_dataset_files))
tabular_dataset_docnames = [file.split('entity=')[-1].split('/')[0] for file in tabular_dataset_files]
df_tabular_dataset_files = pd.DataFrame()
df_tabular_dataset_files['file'] = tabular_dataset_files
df_tabular_dataset_files['doc_name'] = tabular_dataset_docnames

# ## Read structured datasets extracted from table files
tasks = [
    bg_async.async_read_files(
        files=df_tabular_dataset_files[df_tabular_dataset_files['doc_name'] == doc_name]['file'].unique().tolist(),
        # tabular_dataset_files,
        add_file=1,  ## this will add file path in the returned dataframe
    )
    for doc_name in doc_names
]
df_tabular_datasets = utils.async_utils.run_async_tasks(tasks)
df_tabular_datasets = [resp.get_output() for resp in df_tabular_datasets]
df_tabular_datasets = [pd.DataFrame(df) for df in df_tabular_datasets]
df_tabular_datasets = pd.concat(df_tabular_datasets)
## add doc_name to df_tabular_datasets
df_tabular_datasets['doc_name'] = [file.split('entity=')[-1].split('/')[0] for file in df_tabular_datasets['file']]
"""
Number of documents for which tabular datasets are available, `len(df_tabular_datasets['doc_name'].unique())`: 48
df_tabular_datasets columns, `list(df_tabular_datasets.columns)`
['context', 'file', 'row_num', 'value', 'variable', 'doc_name']  
"""
## save df_tabular_datasets locally
df_tabular_datasets.to_csv(f"/tmp/df_tabular_datasets.csv", index=False)
# df_tabular_datasets.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_tabular_datasets.csv", index=False)

# ### Merge file scores and document info with df_tabular_datasets
## add pagenum
df_tabular_datasets['pagenum'] = [
    file.split('/')[-1].split('pagenum-')[-1].split('_')[0]
    for file in df_tabular_datasets['file']
]
df_filtered_table_sim_files['pagenum'] = [
    file.split('/')[-1].split('pagenum-')[-1].split('_')[0]
    for file in df_filtered_table_sim_files['file']
]
## add pagenum
df_tabular_datasets['tablenum'] = [
    file.split('/')[-1].split('tablenum-')[-1].split('.csv')[0] for file in df_tabular_datasets['file']
]
df_filtered_table_sim_files['tablenum'] = [
    file.split('/')[-1].split('tablenum-')[-1].split('.csv')[0]
    for file in df_filtered_table_sim_files['orig_table_file']
]
## cols to add from df_filtered_table_sim_files
cols_to_add = [
    'doc_name', 'pagenum', 'tablenum', 'orig_table_file',
    'doc_org', 'doc_org_std', 'doc_year', 'num_pages',
]
## merge dataframes
df_tabular_datasets = pd.merge(
    left=df_tabular_datasets,
    right=df_filtered_table_sim_files[cols_to_add].drop_duplicates(),
    on=['doc_name', 'pagenum', 'tablenum'],
    how='left'
)
## save df_tabular_datasets locally
df_tabular_datasets.to_csv(f"/tmp/df_tabular_datasets.csv", index=False)
# df_tabular_datasets.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_tabular_datasets.csv", index=False)


# ### Get datasets for each KPI
dataset_dict = {}
for kpi_num, kpi in enumerate(kpi_attrs.keys()):
    logger.info(f"Filtering datasets for ({kpi_num}/{len(kpi_attrs.keys())}): {kpi}")
    df_ = df_tabular_datasets[df_tabular_datasets['variable'].isin(kpi_attrs[kpi])]
    df_ = df_[df_['variable'].notnull() & df_['value'].notnull() & (df_['value'] != '')].reset_index(drop=True)
    dataset_kpi = df_.pivot(
        index=[
            'doc_org', 'doc_org_std', 'doc_year', 'num_pages', 'doc_name', 'pagenum', 'tablenum',
            'file', 'orig_table_file', 'context', 'row_num',
        ],
        columns=['variable'],
        values='value'
    ).reset_index()
    dataset_dict[kpi] = dataset_kpi

# ### define non-null columns for each KPI to remove irrelevant rows
"""
We can now define a dictionary of non-null columns for each KPI, so that that the rows where the column is empty, 
can be dropped. For example, for 'hazardous waste', we are only interested in rows that provide a 'hazardous waste amount', 
and if this column is null, the rest of the details are largely irrelevant, so we will drop such rows. 
"""
kpi_attrs_nonnull = {
    '% of female representation on the board': ['% of female representation on the board'],
    'hazardous waste': ['hazardous waste amount'],
    'gender pay gap': ['gender pay gap'],
    'GHG Scope 1 emissions': ['amount of emissions'],
    'GHG Scope 2 emissions': ['amount of emissions'],
    'GHG Scope 3 emissions': ['amount of emissions'],
    'Non-renewable energy consumption': ['amount of energy consumption'],
    'Emissions to water': ['amount of emissions to water'],
    'Percentage of non-renewable energy production': ['amount of energy production'],
    'anti-corruption policies': ['summary of anti-corruption policies'],
    'anti-bribery policies': ['summary of anti-bribery policies'],
}
for kpi_num, kpi in enumerate(kpi_attrs.keys()):
    logger.info(f"Filtering datasets for ({kpi_num}/{len(kpi_attrs.keys())}): {kpi}")
    for col in kpi_attrs_nonnull[kpi]:
        df_ = dataset_dict[kpi].copy()
        df_ = df_[(df_[col].notnull()) & (df_[col] != '') & (df_[col] != 'nan')]
        df_ = df_.fillna('')
        dataset_dict[kpi] = df_

# ### check dataset for a few KPIs
"""
Available KPIs, `list(dataset_dict.keys())`
[
    '% of female representation on the board', 
    'hazardous waste', 
    'gender pay gap', 
    'GHG Scope 1 emissions', 
    'GHG Scope 2 emissions', 
    'GHG Scope 3 emissions', 
    'Non-renewable energy consumption', 
    'Emissions to water', 
    'Percentage of non-renewable energy production', 
    'anti-corruption policies', 
    'anti-bribery policies'
]
Dataset columns for 'hazardous waste', `list(dataset_dict['hazardous waste'].columns)`
['doc_org', 'doc_org_std', 'doc_year', 'num_pages', 'doc_name', 'pagenum', 'tablenum', 'file', 'orig_table_file', 'context', 'row_num', 'any details of hazardous waste', 'company name', 'date', 'hazardous waste amount', 'unit of measurement']
dataset_dict['hazardous waste'][['doc_org_std', 'company name', 'date', 'hazardous waste amount', 'unit of measurement', 'any details of hazardous waste', 'context']].head().to_dict('records')
[
    {'company name': nan, 'date': nan, 'hazardous waste amount': '30,099', 'unit of measurement': 'tCO2e/year*', 'any details of hazardous waste': nan, 'context': '[["2022; UK", "2022; Total", "2021; UK", "2021; Total", "nan", "nan_2"], ["Scope 1", "tCO2e/year*", "30,099", "302,843", "30,610", "288,438"], ["Scope 2", "tCO2e/year*", "357", "26,977", "2,409", "30,532"], ["Total", "tCO2e/year*", "30,456", "329,820", "33,019", "318,970"], [NaN, NaN, NaN, NaN, NaN, NaN], ["Energy consumption used to calculate emissions", "mWh", "131,148", "1,317,129", "139,912", "1,266,179"]]'}, 
    {'company name': nan, 'date': nan, 'hazardous waste amount': '357', 'unit of measurement': 'tCO2e/year*', 'any details of hazardous waste': nan, 'context': '[["2022; UK", "2022; Total", "2021; UK", "2021; Total", "nan", "nan_2"], ["Scope 1", "tCO2e/year*", "30,099", "302,843", "30,610", "288,438"], ["Scope 2", "tCO2e/year*", "357", "26,977", "2,409", "30,532"], ["Total", "tCO2e/year*", "30,456", "329,820", "33,019", "318,970"], [NaN, NaN, NaN, NaN, NaN, NaN], ["Energy consumption used to calculate emissions", "mWh", "131,148", "1,317,129", "139,912", "1,266,179"]]'}, 
    {'company name': 'US', 'date': 'Recordable accidents', 'hazardous waste amount': '190.0', 'unit of measurement': nan, 'any details of hazardous waste': nan, 'context': '[["2022; OSHA", "2022; RIDDOR", "2021; OSHA", "2021; RIDDOR", "nan"], ["US Recordable accidents", 190.0, 74.0, 194.0, 114.0], ["Incident rate", 0.9, 0.17, 1.07, 0.31], ["Canada Recordable accidents", 25.0, 5.0, 29.0, 8.0], ["Incident rate", 1.49, 0.15, 2.12, 0.29], ["UK Recordable accidents", NaN, 18.0, NaN, 21.0], ["Incident rate", NaN, 0.22, NaN, 0.27], [NaN, NaN, NaN, NaN, NaN]]'}, 
    {'company name': 'US', 'date': 'Incident rate', 'hazardous waste amount': '0.9', 'unit of measurement': nan, 'any details of hazardous waste': nan, 'context': '[["2022; OSHA", "2022; RIDDOR", "2021; OSHA", "2021; RIDDOR", "nan"], ["US Recordable accidents", 190.0, 74.0, 194.0, 114.0], ["Incident rate", 0.9, 0.17, 1.07, 0.31], ["Canada Recordable accidents", 25.0, 5.0, 29.0, 8.0], ["Incident rate", 1.49, 0.15, 2.12, 0.29], ["UK Recordable accidents", NaN, 18.0, NaN, 21.0], ["Incident rate", NaN, 0.22, NaN, 0.27], [NaN, NaN, NaN, NaN, NaN]]'}, 
    {'company name': 'Canada', 'date': 'Recordable accidents', 'hazardous waste amount': '25.0', 'unit of measurement': nan, 'any details of hazardous waste': nan, 'context': '[["2022; OSHA", "2022; RIDDOR", "2021; OSHA", "2021; RIDDOR", "nan"], ["US Recordable accidents", 190.0, 74.0, 194.0, 114.0], ["Incident rate", 0.9, 0.17, 1.07, 0.31], ["Canada Recordable accidents", 25.0, 5.0, 29.0, 8.0], ["Incident rate", 1.49, 0.15, 2.12, 0.29], ["UK Recordable accidents", NaN, 18.0, NaN, 21.0], ["Incident rate", NaN, 0.22, NaN, 0.27], [NaN, NaN, NaN, NaN, NaN]]'}
]
"""

# ## filter most relevant text files
"""
Now, we can repeat the same procedure on text files to extract info relevant to the KPIs. 
For this example, we will only use text files to extract information about qualitative KPIs, 
such as anti-corruption and anti-bribery policies.
"""

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=text-segments/**.csv',
        filter_what='files',
        groupby_cols=['query'],
        max_rows_to_keep=5,
        filename_sfx='filtered-text',
    )
    for doc_name in doc_names
]
## run tasks
filtered_text_sim_responses = utils.async_utils.run_async_tasks(tasks)

# ### get filtered text files
filtered_text_sim_files = [resp.get_output() for resp in filtered_text_sim_responses if resp.get_output() is not None]
"""
Number of documents for which filtered text files exist, `len(filtered_text_sim_files)`: 49
First 5 filtered text files, `filtered_text_sim_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_srpdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jason_09_srpdf_filtered-text.csv'
]
"""

# ### Read filtered text similarity files
tasks = [
    bg_sync.async_read_file(
        file=file
    )
    for file in filtered_text_sim_files
]
df_filtered_text_sim_files = utils.async_utils.run_async_tasks(tasks)
df_filtered_text_sim_files = [resp.get_output() for resp in df_filtered_text_sim_files]
df_filtered_text_sim_files = [pd.DataFrame(df) for df in df_filtered_text_sim_files]
df_filtered_text_sim_files = pd.concat(df_filtered_text_sim_files)
## add doc_name to df
df_filtered_text_sim_files['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_filtered_text_sim_files['file']
]
## drop duplicates
df_filtered_text_sim_files = df_filtered_text_sim_files.drop_duplicates().reset_index(drop=True)
"""
Number of documents available in df_filtered_text_sim_files, `len(df_filtered_text_sim_files['doc_name'].unique())`: 49
df_filtered_text_sim_files.head().to_dict('records')
[
    {'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 'query': 'hazardous waste', 'score': 0.7243718383028761, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 'query': 'hazardous waste', 'score': 0.7184264913346579, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-7_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 'query': 'hazardous waste', 'score': 0.7183537716707852, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 'query': 'hazardous waste', 'score': 0.7015001997580329, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-4_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 'query': 'hazardous waste', 'score': 0.6981161775811571, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""

# ### Get underlying text-segment files
"""
Once, we have filtered the similarity-scored files, we need to get the underlying text-segments that contain the text data. 
We can retrieve these files using `/list_corresponding_files` endpoint.
"""
tasks = [
    bg_sync.async_list_corresponding_files(
        files=df_filtered_text_sim_files['file'].unique().tolist(),
        data_type='semi-structured',
        variable_desc='text-segments',
        file_format='csv',
    )
]
filtered_text_segments_files = utils.async_utils.run_async_tasks(tasks)
filtered_text_segments_files = [resp.get_output() for resp in filtered_text_segments_files]
## flatten filtered_text_segments_files
filtered_text_segments_files = [file for files in filtered_text_segments_files for file in files]
## add to df_filtered_text_sim_files
df_filtered_text_sim_files['text_segment_file'] = filtered_text_segments_files
## check df_filtered_text_sim_files
mask = (df_filtered_text_sim_files['doc_name'] == doc_names[0]) & \
       (df_filtered_text_sim_files['query'] == kpis[0])
logger.info(
    f"First 5 rows of df_filtered_text_sim_files for first document and first kpi: "
    f"{df_filtered_text_sim_files[mask][['query', 'score', 'doc_name', 'text_segment_file']].head().to_dict('records')}"
)
"""
Text segment files for first document and first KPI, 
[
    {'query': '% of female representation on the board', 'score': 0.841910903716324, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'text_segment_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-2_text-blocks_text-segments.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.827191327421238, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'text_segment_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-1_text-blocks_text-segments.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.7952882407662112, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'text_segment_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-7_text-blocks_text-segments.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.7950848086745431, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'text_segment_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-6_text-blocks_text-segments.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.7891511878288711, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'text_segment_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-3_text-blocks_text-segments.csv'}
]
`df_filtered_text_sim_files` contains the most similar text files for each KPI and document, 
so we will use this dataframe to access the most relevant text files, and extract info from them.
"""
## save df_filtered_text_sim_files locally
df_filtered_text_sim_files.to_csv(f"/tmp/df_filtered_text_sim_files.csv", index=False)

# ## Extract quants from most similar files

# ### trigger quant structuring from filtered table files
tasks = [
    bg_async.async_structure_tabular_quants(
        files=df_filtered_table_sim_files['orig_table_file'].unique().tolist(),
    )
]
tabular_quant_structuring_responses = utils.async_utils.run_async_tasks(tasks)

# ### Get structured tabular quants files
structured_tabular_quants_files = [
    resp.get_output() for resp in tabular_quant_structuring_responses
    if resp.get_output() is not None
]
## flatten structured tabular quant files
structured_tabular_quants_files = [file for files in structured_tabular_quants_files for file in files]
## read one structured tabular quant file
df_structured_quants = bg_sync.read_file(structured_tabular_quants_files[0]).get_output()
df_structured_quants = pd.DataFrame(df_structured_quants)

# ## ToDo: Merge tabular quants with doc info

# ### ToDo: merge tabular quants and with df_doc_info

# ## ToDo: Verify extracted tabular quants

# ## ToDo: standardise company names

# ## ToDo: Estimate values for desired KPIs for tabular quants

# ## ToDo: Extract quants from filtered passages

# ### ToDo: Estimate values for desired KPIs for passage quants

# ## ToDo: synthesize values across tables and passages

# ### ToDo: for quant KPIs, whenever a KPI is available in estimate tabular quants, keep it, otherwise take it from passages

# ### ToDo: for qual KPIs, whenever a KPI is available in estimate tabular quants, keep it, otherwise take it from passages


# ## Extract quant metrics
"""
ByteGenie API has dedicated endpoints for extracting quants in a structured form, from text passages and tables. 
The endpoints allow user to specify any specific attributes to extract in the quantitative dataset. 
By default, these attributes are set to be generic attributes needed to understand quantitative values, i.e. 
(company name, variable description, category, variable, value, unit, date, pagenum, doc_name).  
"""

# ### quant extraction start time
quant_extraction_start_time = time.time()
"""
`quant_extraction_start_time: 1695704723.746251`
"""

# ### Extract quant metrics from passages
tasks = [
    bg_async.async_structure_passage_quants(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=text-segments/**.csv',
        text_col='text',
    )
    for doc_name in doc_names
]
passage_quant_extraction_responses = utils.async_utils.run_async_tasks(tasks)

# ### extract quant metrics from tables
tasks = [
    bg_async.async_structure_tabular_quants(
        doc_name=doc_name,
    )
    for doc_name in doc_names
]
tabular_quant_extraction_responses = utils.async_utils.run_async_tasks(tasks)

# ### check extracted passage quant files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=structured/**/source=passage-quants/**.csv"
    )
    for doc_name in doc_names
]
passage_quant_files = utils.async_utils.run_async_tasks(tasks)
passage_quant_files = [resp.get_data() for resp in passage_quant_files if resp.get_data() is not None]
"""
Documents with passage quant files: `len(passage_quant_files)`: 49
First 5 **passage_quant_files for the first document**
passage_quant_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-4_contextnum-0_passage-quants_structured-quant-summary.csv'
]
"""

# ### check extracted tabular quant files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=structured/**/source=tabular-quants/**.csv"
    )
    for doc_name in doc_names
]
tabular_quant_files = utils.async_utils.run_async_tasks(tasks)
tabular_quant_files = [resp.get_data() for resp in tabular_quant_files if resp.get_data() is not None]
"""
Number of documents with tabular quant files, `len(tabular_quant_files)`: 48
First 5 **tabular_quant_files for the first document**
tabular_quant_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-1_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-2_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-5_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary.csv'
]
"""

# ### read a passage quant file
df_passage_quants_sample = bg_sync.read_file(
    file=passage_quant_files[0][1]
).get_data()
df_passage_quants_sample = pd.DataFrame(df_passage_quants_sample)
df_passage_quants_sample = df_passage_quants_sample[df_passage_quants_sample['value'] != '']
"""
df_passage_quants_sample columns: `list(df_passage_quants_sample.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'pagenum', 'relevant quote', 'unit', 'value', 'variable', 'variable description']
check a **short sample of df_passage_quants_sample**
`df_passage_quants_sample[['company name', 'category', 'variable description', 'variable', 'unit', 'value', 'date', 'pagenum', 'doc_name']].head().to_dict('records')`
[
    {'company name': 'American Express', 'category': 'Number of women in', 'variable description': 'The number of women in first level manager roles has increased by one-third in the last five years.', 'variable': 'First level manager', 'unit': '', 'value': '32%', 'date': '', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': 'American Express', 'category': 'Gender pay gap', 'variable description': 'There has been almost a 2 percentage point improvement year-on-year in the gender pay gap.', 'variable': 'Improvement', 'unit': '', 'value': '2 percentage', 'date': 'year-on-year', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': 'American Express', 'category': 'Gender diversity', 'variable description': 'The number of women in senior management positions now stands at 47%.', 'variable': 'Senior management', 'unit': '', 'value': '47%', 'date': '', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""

# ### read a tabular quant file
df_tabular_quants_sample = bg_sync.read_file(
    file=tabular_quant_files[0][1]
).get_data()
df_tabular_quants_sample = pd.DataFrame(df_tabular_quants_sample)
df_tabular_quants_sample = df_tabular_quants_sample[df_tabular_quants_sample['value'] != '']
"""
df_tabular_quants_sample columns: `list(df_tabular_quants_sample.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'pagenum', 'relevant quote from text', 'unit', 'value', 'variable', 'variable description']
check a **short sample of df_tabular_quants_sample**
`df_tabular_quants_sample[['company name', 'category', 'variable description', 'variable', 'unit', 'value', 'date', 'pagenum', 'doc_name', 'context']].head().to_dict('records')`
[
    {'company name': '', 'category': 'GENDER', 'variable description': '', 'variable': 'MEAN', 'unit': '', 'value': '14.7%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': 'GENDER', 'variable description': '', 'variable': 'MEDIAN', 'unit': '', 'value': '16.7%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': '% W/M', 'variable description': '', 'variable': 'WOMEN', 'unit': '', 'value': '55%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': '% W/M', 'variable description': '', 'variable': 'MEN', 'unit': '', 'value': '45%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""

# ## Vectorise quant data for semantic searching

# ### set columns to embed
cols_to_embed = ['category', 'company name', 'date', 'unit', 'value', 'variable', 'variable description']

# ### embed quant files
bg_async.overwrite = 1
bg_async.overwrite_base_output = 1
tasks = [
    bg_async.async_embed_doc_data(
        doc_name=doc_name,
        file_pattern='data_type=structured/**/variable_desc=structured-quant-summary/**.csv',
        cols_to_use=cols_to_embed,
    )
    for doc_name in doc_names
]
## run tasks in batches of 10 documents at a time to avoid rate limit errors
batch_size = 10
wait_time = 2 * 60
doc_emb_responses = []
for task_num, task in enumerate(tasks):
    logger.info(f"running task: {task_num}/{len(tasks)}")
    doc_emb_response_ = utils.async_utils.run_async_tasks([task])
    doc_emb_responses.append(doc_emb_response_)
    if (task_num % batch_size == 0) and (task_num > 0):
        time.sleep(wait_time)

# ### list embedding files for quants
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=embeddings/**/variable_desc=structured-quant-summary/**.csv',
    )
    for doc_name in doc_names
]
embed_doc_files = utils.async_utils.run_async_tasks(tasks)
embed_doc_files = [resp.get_data() for resp in embed_doc_files if resp.get_data() is not None]
"""
Number of documents with embedding files, len(embed_doc_files): 49
First 5 embedding files for the first document: embed_doc_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_similarity_query-emissions-by-scope_embeddings.csv'
]
"""

# ## Vectorise text segments for semantic searching

# ### set columns to embed
cols_to_embed = ['text']

# ### embed text files
tasks = [
    bg_async.async_embed_doc_data(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=text-segments/**.csv',
        cols_to_use=cols_to_embed,
    )
    for doc_name in doc_names
]
text_emb_responses = utils.async_utils.run_async_tasks(tasks)

# ### list embedding files for text segments
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=embeddings/**/variable_desc=text-segments/**.csv',
    )
    for doc_name in doc_names
]
embed_text_files = utils.async_utils.run_async_tasks(tasks)
embed_text_files = [resp.get_data() for resp in embed_text_files if resp.get_data() is not None]
"""
Number of documents with embedding files: len(embed_text_files): 49
First 5 embedding files for the first documnet: embed_text_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-1_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-2_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-3_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-4_text-blocks_text-segments_embeddings.csv'
]
"""

# ## Filter out quant data most relevant to KPIs

# ### read similarity scored files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=structured-quant-summary/**.csv',
    )
    for doc_name in doc_names
]
sim_score_files = utils.async_utils.run_async_tasks(tasks)
sim_score_files = [resp.get_data() for resp in sim_score_files if resp.get_data() is not None]
"""
Number of documents with sim_score_files, `len(sim_score_files)`: 49
First 5 sim_score_files for the first document, `sim_score_files[0][:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-anti-corruption-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-emissions-to-water.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-gender-pay-gap.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-ghg-scope-1-emissions.csv'
]
"""
## flatten sim_score_files
sim_score_files = [file for doc_files in sim_score_files for file in doc_files]
"""
Total number of sim_score_files across all documents, `len(sim_score_files)`: 119732
"""

# ### create a dataframe of files by KPI
df_sim_files = pd.DataFrame()
df_sim_files['file'] = sim_score_files
df_sim_files['query'] = [os.path.splitext(file.split('/')[-1].split('query-')[-1])[0] for file in sim_score_files]
"""
Unique queries/KPIs for which we have similarity scored files, `list(df_sim_files['query'].unique())`
[
    'anti-bribery-policies', 
    'anti-corruption-policies', 
    'emissions-to-water', 
    'gender-pay-gap', 
    'ghg-scope-1-emissions', 
    'ghg-scope-2-emissions', 
    'ghg-scope-3-emissions', 
    'hazardous-waste', 
    'non-renewable-energy-consumption', 
    'of-female-representation-on-the-board', 
    'percentage-of-non-renewable-energy-production', 
    'emissions-by-scope'
]
First 5 files for 'ghg-scope-1-emissions', df_sim_files[df_sim_files['query'] == 'ghg-scope-1-emissions']['file'].unique().tolist()[:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-4_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv'
]
"""

# ### save files locally
df_sim_files.to_csv(f"/tmp/df_sim_files.csv", index=False)

# ### read from local file
df_sim_files = pd.read_csv(f"/tmp/df_sim_files.csv")

# ### filter similarity scored quant files to keep the most relevant rows

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=structured-quant-summary/**.csv',
        non_null_cols=['value'],
        groupby_cols=['query'],
        max_rows_to_keep=20,
        max_frac_rows_to_keep=0.1,
        filename_sfx='quant-kpi-01',
    )
    for doc_name in doc_names
]
## run tasks
filtered_quant_responses = utils.async_utils.run_async_tasks(tasks)
## read filtered quants
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=similarity/**/variable_desc=filtered-data/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quants_filtered = utils.async_utils.run_async_tasks(tasks)
df_quants_filtered = [resp.get_output() for resp in df_quants_filtered]
df_quants_filtered = [pd.DataFrame(df) for df in df_quants_filtered]
df_quants_filtered = pd.concat(df_quants_filtered)
## drop unwanted columns
if 'context' in df_quants_filtered.columns:
    df_quants_filtered = df_quants_filtered.drop(columns=['context'])
## sort by (query, score)
df_quants_filtered = \
    df_quants_filtered.sort_values(['query', 'score'], ascending=False).reset_index(drop=True)
## re-arrange columns
df_quants_filtered = df_quants_filtered[[
    'query', 'score', 'category', 'company name',
    'variable description', 'variable', 'value', 'date', 'unit',
    'pagenum', 'doc_name', 'file',
]]
## save filtered_text_data_dict locally
df_quants_filtered.to_csv(f"/tmp/df_quants_filtered.csv", index=False)
"""
Number of rows in df_quants_filtered, len(df_quants_filtered): 61518
df_quants_filtered.head().to_dict('records')
[
    {'query': 'hazardous waste', 'score': 0.8145144484413344, 'category': 'Waste', 'company name': nan, 'variable description': 'Hazardous waste generated from the manufacturing process in tonnes', 'variable': 'Hazardous waste', 'value': '27.0', 'date': '2021', 'unit': 'tonnes', 'pagenum': 112, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-112_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.8074640482800609, 'category': 'Waste Recycling', 'company name': 'VINCI Energies', 'variable description': '"VINCI Energies divisions that were part of the reporting scope in 2021 achieved recycling rates of 69% for hazardous waste."', 'variable': 'Hazardous Waste Rate', 'value': '69%', 'date': '2021.0', 'unit': nan, 'pagenum': 120, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf_pagenum-120_contextnum-1_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.798367934167774, 'category': 'Waste', 'company name': nan, 'variable description': 'Non-hazardous waste generated from the manufacturing process in tonnes', 'variable': 'Process waste', 'value': '135.0', 'date': '2021', 'unit': 'tonnes', 'pagenum': 112, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-112_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.7958005310728269, 'category': 'Waste Recovery', 'company name': 'VINCI Construction Central Europe', 'variable description': '"At VINCI Construction, only the Central Europe division is included in the scope for waste recovered, with recovery rates of 31% for hazardous waste."', 'variable': 'Hazardous Waste Rate', 'value': '31%', 'date': nan, 'unit': nan, 'pagenum': 120, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf_pagenum-120_contextnum-1_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.7920064789552598, 'category': 'Environment', 'company name': 'Aker Carbon', 'variable description': 'Total amount of hazardous waste generated in 2021', 'variable': 'Hazardous waste generated', 'value': '0.002', 'date': '2021.0', 'unit': 'Tons', 'pagenum': 104, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf_pagenum-104_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}
]
"""

# ### filter similarity-scored text files to keep the most relevant rows

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=text-segments/**.csv',
        non_null_cols=['text'],
        groupby_cols=['query'],
        max_rows_to_keep=20,
        max_frac_rows_to_keep=0.1,
        filename_sfx='qual-kpi-01',
    )
    for doc_name in doc_names
]
## run tasks
filtered_text_responses = utils.async_utils.run_async_tasks(tasks)
## read filtered text
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=similarity/**/variable_desc=filtered-data/**qual-kpi-01.csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_text_filtered = utils.async_utils.run_async_tasks(tasks)
df_text_filtered = [resp.get_output() for resp in df_text_filtered]
df_text_filtered = [pd.DataFrame(df) for df in df_text_filtered]
df_text_filtered = pd.concat(df_text_filtered)
## drop unwanted columns
if 'context' in df_text_filtered.columns:
    df_text_filtered = df_text_filtered.drop(columns=['context'])
## filter over relevant queries
df_text_filtered = df_text_filtered[df_text_filtered['query'].isin(kpis)]
## sort by score
df_text_filtered = df_text_filtered.sort_values(['query', 'score'], ascending=False).reset_index(drop=True)
## drop unwanted columns
if 'context' in df_text_filtered.columns:
    df_text_filtered = df_text_filtered.drop(columns=['context'])
## re-arrange columns
df_text_filtered = df_text_filtered[['query', 'score', 'text', 'pagenum', 'doc_name', 'file']]
## save data locally
df_text_filtered.to_csv(f"/tmp/df_text_filtered.csv", index=False)
"""
Number of rows in df_quants_filtered, len(df_text_filtered): 30206
df_text_filtered.head().to_dict('records')
[
    {'query': 'anti-corruption policies', 'score': 0.871208445921234, 'text': 'Anti- corruption Policy', 'pagenum': 112, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_9_2021-annual-report_compressedpdf_pagenum-112_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.8698307926193798, 'text': 'Anti-corruption: the anti-corruption principles to be adhered to by employees, based on the fundamental tenet of "zero tolerance".', 'pagenum': 113, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_9_2021-annual-report_compressedpdf_pagenum-113_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.863275482062681, 'text': 'Anti-Corruption', 'pagenum': 106, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_srpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_srpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_09_srpdf_pagenum-106_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.8528518716714095, 'text': "VINCI's anti-corruption arrangements", 'pagenum': 110, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_8_vinci-2021-universal-registration-documentpdf_pagenum-110_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.8526271061198671, 'text': 'Anti-corruption Code of Conduct', 'pagenum': 110, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_8_vinci-2021-universal-registration-documentpdf_pagenum-110_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}
]
"""

# ## Retrieve evidence for all filtered values
"""
We can use `/trace_evidence` endpoint to trace evidence for any extracted or derived data. `/trace_evidence` takes a document name and file pattern as inputs,  
and determines where these files lie in the processing pipeline, and which previous output is relevant for contextualising the data in these files. 
For example, for similarity-scored data, it will fetch the base structured data (before any vectorisation and similarity scoring), 
and original page image, from which all the data in the similarity-score files are derived. 
`/trace_evidence` call will write new files with `data_type=evidence`. These files will contain the relevant evidence for each extracted row of the data.  
"""

# ### trace evidence for quant files

## define tasks
tasks = [
    bg_async.async_trace_evidence(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=filtered-data/**quant-kpi-01*csv',
    )
    for doc_name in doc_names
]
## run tasks
quant_evidence_responses = utils.async_utils.run_async_tasks(tasks)

# ### read quant evidence files
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=evidence/**/variable_desc=filtered-data/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quant_evidence = utils.async_utils.run_async_tasks(tasks)
df_quant_evidence = [resp.get_output() for resp in df_quant_evidence]
df_quant_evidence = [pd.DataFrame(df) for df in df_quant_evidence]
df_quant_evidence = pd.concat(df_quant_evidence)
"""
Number of documents for which quant evidence files are available, `len(df_quant_evidence['doc_name'].unique())`: 49
df_quant_evidence columns, `list(df_quant_evidence.columns)`
[
    'query', 'score', 'company name',  'category',  
    'variable description', 'variable', 'date', 'unit', 'value',   
    'context', 'context_file', 'img_file', 'pagenum', 'doc_name', 
]
AS we can see, /trace_evidence has added `context`, `context_file`, and `img_file` columns to the data. 
* `context` contains the text extracted from the page (before structuring); 
* `context_file` contains the file path for the file containing the extracted text;
* `img_file` is the path to the page image. 
This will allow us to see the full details of the context from the relevant was extracted, and use these additional details to correct any mistakes in the extraction.
"""
## re-arrange columns
df_quant_evidence = df_quant_evidence[[
    'query', 'score', 'category', 'date',
    'unit', 'value', 'variable', 'variable description',
    'context', 'context_file', 'img_file', 'pagenum', 'doc_name',
]]
"""
Snapshot of quants data merged with evidence from where it was extracted, `df_quant_evidence.head().to_dict('records')`
[
    {'query': 'anti-bribery policies', 'score': 0.7446372728941727, 'category': 'Inclusion and Recognition', 'date': nan, 'unit': nan, 'value': '', 'variable': 'Awards', 'variable description': 'Recognition by Working Families as a Top 10 Family Friendly Employer, for the tenth year running', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7374088264758053, 'category': 'Inclusion and Recognition', 'date': nan, 'unit': nan, 'value': '', 'variable': 'Awards', 'variable description': 'Recognition by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7311989710999165, 'category': 'Gender Pay Gap', 'date': nan, 'unit': nan, 'value': '', 'variable': 'Gender Pay Gap', 'variable description': "Company's commitment to achieving a greater gender balance and the primary reason for the gender pay gap", 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7240244921672689, 'category': 'Gender Pay Gap Improvement', 'date': nan, 'unit': nan, 'value': '2% improvement', 'variable': 'Gender Pay Gap', 'variable description': 'Year-on-year improvement in the gender pay gap', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7239204026679752, 'category': 'Gender Diversity', 'date': nan, 'unit': nan, 'value': '47%', 'variable': 'Women in Manager', 'variable description': 'Number of women in senior management positions', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""
## write data locally
df_quant_evidence.to_csv(f"/tmp/df_quant_evidence.csv", index=False)

# ### evidence tracing for text files

## tasks
tasks = [
    bg_async.async_trace_evidence(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=filtered-data/**qual-kpi-01*csv',
    )
    for doc_name in doc_names
]
## run tasks
text_evidence_responses = utils.async_utils.run_async_tasks(tasks)

# ### read text evidence files
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=evidence/**/variable_desc=filtered-data/**qual-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_text_evidence = utils.async_utils.run_async_tasks(tasks)
df_text_evidence = [resp.get_output() for resp in df_text_evidence]
df_text_evidence = [pd.DataFrame(df) for df in df_text_evidence]
df_text_evidence = pd.concat(df_text_evidence)
## re-arrange columns
df_text_evidence = df_text_evidence[
    ['query', 'score', 'text', 'context_file', 'img_file', 'pagenum', 'doc_name']
]
"""
Number of documents for which text evidence files are available, `len(df_text_evidence['doc_name'].unique())`: 49
df_text_evidence columns, `list(df_text_evidence.columns)`
['query', 'score', 'text', 'context_file', 'img_file', 'pagenum', 'doc_name']
df_text_evidence.head().to_dict('records')
[
    {'query': 'anti-bribery policies', 'score': 0.7144075502491216, 'text': 'AMERICAN', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7003961798659838, 'text': 'EXPRESS', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7341672165364279, 'text': 'UK Gender Pay Gap REPORT 2021', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-corruption policies', 'score': 0.7057159443441187, 'text': 'AMERICAN', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-corruption policies', 'score': 0.6886003988493039, 'text': 'EXPRESS', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""
## write data locally
df_text_evidence.to_csv(f"/tmp/df_text_evidence.csv", index=False)

# ## Verify filtered data
"""
To verify any extracted data, we can use `/verify_data` endpoint, which takes variable, value and context column names as inputs, allows us to verify whether the extracted (variable, value) pair is consistent with what's in the context column. 
That is the context column is essentially the original source data from which (variable, value) pair was extracted, but due to extraction errors, (variable, value) pair may have been extracted incorrectly. 
`/verify_data` is meant to flag such errors, so we can handle these errors separately.
"""

# ### Verify extracted quant values
tasks = [
    bg_async.async_verify_data(
        doc_name=doc_name,
        file_pattern='data_type=evidence/**/variable_desc=filtered-data/**quant-kpi-01*csv',
        var_col='variable',
        val_col='value',
        verification_type='variable-value',
        context_col='context',
    )
    for doc_name in doc_names
]
verify_quants_responses = utils.async_utils.run_async_tasks(tasks)

# ### read verified quant files
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=verification/**/variable_desc=verified-variable-values/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quant_verified = utils.async_utils.run_async_tasks(tasks)
df_quant_verified = [resp.get_output() for resp in df_quant_verified]
df_quant_verified = [pd.DataFrame(df) for df in df_quant_verified]
df_quant_verified = pd.concat(df_quant_verified)
## sort data by score
df_quant_verified = df_quant_verified.sort_values(['query', 'score'], ascending=False).reset_index(drop=True)
## drop unwanted columns
if 'row_id' in df_quant_verified.columns:
    df_quant_verified = df_quant_verified.drop(columns=['row_id'])
## re-arrange columns
df_quant_verified = df_quant_verified[[
    'query', 'score', 'company name', 'category', 'variable description', 'variable', 'value', 'date', 'unit',
    'fuzzy_verification_flag', 'lm_verification_flag',
    'context', 'context_file', 'img_file', 'pagenum', 'doc_name'
]]
"""
Number of documents for which verified quant files are available, `len(df_quant_verified['doc_name'].unique())`: 48
Number of rows in df_quant_verified, `len(df_quant_verified)`: 10600
Number of documents for each KPI 
`df_quant_verified.groupby(['query']).apply(lambda x: len(x['doc_name'].unique())).reset_index().values.tolist()`
[
    ['% of female representation on the board', 40], 
    ['Emissions to water', 41], 
    ['GHG Scope 1 emissions', 38], 
    ['GHG Scope 2 emissions', 37], 
    ['GHG Scope 3 emissions', 40], 
    ['Non-renewable energy consumption', 38],
    ['Percentage of non-renewable energy production', 39], 
    ['anti-bribery policies', 47], 
    ['anti-corruption policies', 45], 
    ['emissions by scope', 1], 
    ['gender pay gap', 41], 
    ['hazardous waste', 38]
]
Verified quants for a specific KPI, df_quant_verified[df_quant_verified['query'] == 'Percentage of non-renewable energy production'].head().to_dict('records')
[
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8317343036066032, 'company name': '', 'category': 'Renewable Energy', 'variable description': 'At the Noginsk warehouse, at least 25% of electricity consumption comes from renewable sources, resulting in a significant positive impact on net carbon emissions.', 'variable': 'Carbon Emissions', 'value': 'At least 25%', 'date': '', 'unit': '', 'fuzzy_verification_flag': False, 'lm_verification_flag': True, 'context': '"On site renewable energy case studies \\n\\n The Rostov solar farm and hydro electric power case studies outlined in Case Study 1 and 2 below highlight some of the positive steps the Group is taking to explore the adoption of renewable energy sources in its portfolio. \\n\\n 44 \\n\\n 1. ROSTOV SOLAR FARM \\n\\n Meteorological conditions in Russia are such that, in the majority of locations, the number of sunlight hours and levels of snowfall mean that solar generated power is not practical or financially viable. Our Rostov property is geographically far enough south to have limited snowfall and sufficient sun light hours to make solar energy feasible and we have designed a pilot scheme which will generate \\n\\n 1,257MW PER ANNUM, OR 12% OF OUR TENANTS\' POWER REQUIREMENT, EQUIVALENT TO POWERING 550 AVERAGE FAMILY HOMES IN THE LOCAL ROSTOV AREA. \\n\\n There have been numerous challenges in developing a framework to implement the project, but we hope to be on site in the next month and have commenced power generation by early summer. We anticipate this investment will pay back within ten years. \\n\\n If the project is successful we will explore further opportunities across the portfolio, to roll out similar or smaller systems to supplement existing power consumption. \\n\\n 2. HYDRO ELECTRICITY - NOGINSK, MOSCOW \\n\\n We have transferred the electrical supply to our Noginsk warehouse to RusHydro, meaning that \\n\\n AT LEAST 25% OF THE ELECTRICITY WE CONSUME AT THE PROJECT WILL COME FROM RENEWABLE SOURCES. \\n\\n 60% of the Noginsk warehouse is used for chilled and cold storage meaning its energy requirements per square metre is one of the highest in our portfolio. Changing supplier on this site to a renewable provider has had a significant positive impact on net carbon emissions. \\n\\n Changing power supply in Russia is a complex and time consuming task particularly as renewable power for our properties is only available via the wholesale electricity market. Moving to the wholesale market can take up to eight months with requisite notice periods and the necessity for the new supplier to perform a detailed audit and inspection of the electrical distribution and metering system which in turn is then subject to validation by the wholesale market trading administrator. \\n\\n ENVIRONMENTAL AND SOCIAL REPORT \\n\\n Rostov Solar Farm \\n\\n Rostov Solar Farm \\n\\n Noginsk fit out works \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf_pagenum-3_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf_pagenum-3.png', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8249588034888888, 'company name': '', 'category': 'Renewable Energy', 'variable description': 'Percentage of electricity purchased from renewable sources for the reporting period.', 'variable': 'Electricity Source', 'value': '64%', 'date': 'FY2021', 'unit': '%', 'fuzzy_verification_flag': False, 'lm_verification_flag': True, 'context': '"Streamlined Energy and Carbon Reporting \\n\\n This statement has been prepared in accordance with our regulatory obligation to report greenhouse gas (GHG) emissions pursuant to the Companies (Directors\' Report) and Limited Liability Partnerships (Energy and Carbon Report) Regulations 2018 which implement the government\'s policy on Streamlined Energy and Carbon Reporting. \\n\\n During the reporting period January 2021 to December 2021, our measured Scope 1 and 2 emissions (location-based) totalled 4,516 tCO2 \\n\\n 114 \\n\\n FY2020 Scope UK Rest of world Total Scope 1 1,121 Scope 2 - location-based 2,074 1,712 3,786 Scope 2 - market-based 0 1,798 1,798 Total Scope 1 & 2 (Location-Based) 2,074 1,712 4,907 Total Scope 1 & 2 (Market-Based) 0 1,798 2,919 Scope & 2 intensity per FTE (Location-Based) * * 0.4 Scope 3 * * 535 \\n\\n Overall, our Scope 1 and 2 emissions have decreased by 8% against last year. This was due to improved control in our Building Management Systems in our largest locations. We purchase 64% of our electricity from renewable sources, meaning our Scope 1 and 2 market- based emissions were 2,834 tCO2e, a decrease of 3% from last year. \\n\\n Complete energy data for the whole New emissions reporting - tracking progress group so that verified data isn\'t towards target. based on assumptions: Identify carbon heavy and/or identifying - Fugitives assets and their life cycle for replacement. - Water - Commuting Engage Arup to assist with mapping the route to net zero carbon. 2021 2022 \\n\\n Environmental Policy put in place to better measure, record and reduce the company Greenhouse Gas emissions. Responsible Investment Policy integrated in place to put the achieve more sustainable long-term returns. Admiral Baseline Emission verified by Carbon Trust. To be re-visited part of the Science Based Target initiative. as \\n\\n 2019 \\n\\n Action areas 2019 \\n\\n 2020 \\n\\n Energy \\n\\n Admiral Group plc Annual Report and Accounts 2021 \\n\\n FY2021 UK Rest of world Total 1,285 192 1,477 1,768 1,272 3,039 25 1,332 1,357 3,053 1,463 4,516 1,310 1,523 2,834 0.4 0.4 0.4 435 517 952 \\n\\n The impact of Covid has resulted in The building management within the working from home being adopted UK sites Newport, Cardiff and Swansea as the norm, with the offices being are controlled by Building Management kept within statutory and regulatory System (BMS) which are actively compliance requirements. This has monitored for performance optimisation naturally resulted in a reduction of utility and time schedule efficiency, and with usage and driven a floor space reduction the requirement to introduce greater which has further increased the energy amounts of fresh air into the buildings or utility savings. \\n\\n 2022-23 Continue to engage with Carbon Trust in verifying the data and expand into Scope 3. 2023 \\n\\n A Sustainability Working Group was established in 2020 to provide additional governance support around matters related to ESG. \\n\\n 2025 \\n\\n Continuous and transparent emissions reporting tracking progress towards the targets. \\n\\n Travel \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf_pagenum-115_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jaime_admiral-group_annual-reportpdf_pagenum-115.png', 'pagenum': 115, 'doc_name': 'userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8185137500421, 'company name': 'Adani Ports and Special Economic Zone Ltd', 'category': 'Grid Energy (in %)', 'variable description': 'Percentage of non-renewable energy consumption in grid energy', 'variable': 'Non-Renewable Energy', 'value': '52', 'date': 'FY 16', 'unit': '%', 'fuzzy_verification_flag': False, 'lm_verification_flag': False, 'context': '"Adani Ports and Special Economic Zone Limited \\n\\n 32 \\n\\n Types of fuel and coolant consumed during FY 19 \\n\\n 1,055 kg 30kg 2.5 kg R22 R410 R134 \\n\\n Energy \\n\\n FY 19 \\n\\n FY 17 \\n\\n FY 16 \\n\\n FY \\n\\n 27.8 ML \\n\\n Diesel \\n\\n 18 \\n\\n 52.8 kL \\n\\n Petrol \\n\\n Mix \\n\\n 4.3KL \\n\\n Furnace Oil \\n\\n There is also a significant impact that we face in terms of cost of acquisition of power 8 fuel. Over the years, the per capita cost has increased further putting pressure on our profitability. So, switching to a cleaner fuel may be of strategic importance for us in the longer term. \\n\\n 82 kg 2,317 kg R407C Acetylene \\n\\n Fuel \\n\\n 28,272 kg LPG \\n\\n Standalone (in %) \\n\\n 87% \\n\\n 39% \\n\\n 57% \\n\\n 58% \\n\\n 42% \\n\\n 59% \\n\\n 43% \\n\\n 5% \\n\\n 14,139 SCM PNG \\n\\n FY 16 \\n\\n Grid \\n\\n Energy \\n\\n FY 19 \\n\\n FY 18 \\n\\n FY \\n\\n Mix \\n\\n 17 \\n\\n Renewable \\n\\n Consolidated \\n\\n 52% \\n\\n 65% \\n\\n 67% \\n\\n 72% \\n\\n (in %) \\n\\n 45% \\n\\n 33% \\n\\n 33% \\n\\n 28% \\n\\n Sustainability Report FY 19 \\n\\n At our ports, energy is primarily used for crane operations, transportation of cargo within campus through trucks 8 conveyor belts, automatic cargo handling system and tug boat. During the FY 19, our total energy consumption was 19,03,253 GJ and total GHG emissions were 2,70,170 tCOe. Through a series of interventions, including technological and process changes we have been able to reduce our energy intensity year-on-year since our base year. \\n\\n Energy Standalone \\n\\n Energy consumption per MMT of cargo handled 47% from previous year KY 67% from the base year \\n\\n Non- Renewable Energy (GJ) \\n\\n 9,906 \\n\\n -7,82,276 1,743 -6.81.000 \\n\\n FY 16 FY 17 \\n\\n 6,246 7,043 \\n\\n FY 18 \\n\\n -9,071 \\n\\n FY 19 \\n\\n -15,806 3,296 3,05,201 \\n\\n Consolidated \\n\\n 9.738 \\n\\n Energy consumption per MMT of cargo handled 32% from previous year 48% from the base year \\n\\n Renewable Energy (GJ) Intensity (GJ/MMT) \\n\\n o 25,65,281 1,748 44,610 -23.42.188 -22,95,625 -18,851 15,503 -14,356 \\n\\n FY 16 FY 17 \\n\\n 52,851 18,50,402 \\n\\n FY 18 FY 19 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17.png', 'pagenum': 17, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8145957411786507, 'company name': 'Adani Ports and Special Economic Zone Ltd', 'category': 'Energy Standalone', 'variable description': 'Non-renewable energy consumption in standalone operations during FY 16', 'variable': 'Non-Renewable Energy', 'value': '9,906', 'date': 'FY 16', 'unit': 'GJ', 'fuzzy_verification_flag': False, 'lm_verification_flag': True, 'context': '"Adani Ports and Special Economic Zone Limited \\n\\n 32 \\n\\n Types of fuel and coolant consumed during FY 19 \\n\\n 1,055 kg 30kg 2.5 kg R22 R410 R134 \\n\\n Energy \\n\\n FY 19 \\n\\n FY 17 \\n\\n FY 16 \\n\\n FY \\n\\n 27.8 ML \\n\\n Diesel \\n\\n 18 \\n\\n 52.8 kL \\n\\n Petrol \\n\\n Mix \\n\\n 4.3KL \\n\\n Furnace Oil \\n\\n There is also a significant impact that we face in terms of cost of acquisition of power 8 fuel. Over the years, the per capita cost has increased further putting pressure on our profitability. So, switching to a cleaner fuel may be of strategic importance for us in the longer term. \\n\\n 82 kg 2,317 kg R407C Acetylene \\n\\n Fuel \\n\\n 28,272 kg LPG \\n\\n Standalone (in %) \\n\\n 87% \\n\\n 39% \\n\\n 57% \\n\\n 58% \\n\\n 42% \\n\\n 59% \\n\\n 43% \\n\\n 5% \\n\\n 14,139 SCM PNG \\n\\n FY 16 \\n\\n Grid \\n\\n Energy \\n\\n FY 19 \\n\\n FY 18 \\n\\n FY \\n\\n Mix \\n\\n 17 \\n\\n Renewable \\n\\n Consolidated \\n\\n 52% \\n\\n 65% \\n\\n 67% \\n\\n 72% \\n\\n (in %) \\n\\n 45% \\n\\n 33% \\n\\n 33% \\n\\n 28% \\n\\n Sustainability Report FY 19 \\n\\n At our ports, energy is primarily used for crane operations, transportation of cargo within campus through trucks 8 conveyor belts, automatic cargo handling system and tug boat. During the FY 19, our total energy consumption was 19,03,253 GJ and total GHG emissions were 2,70,170 tCOe. Through a series of interventions, including technological and process changes we have been able to reduce our energy intensity year-on-year since our base year. \\n\\n Energy Standalone \\n\\n Energy consumption per MMT of cargo handled 47% from previous year KY 67% from the base year \\n\\n Non- Renewable Energy (GJ) \\n\\n 9,906 \\n\\n -7,82,276 1,743 -6.81.000 \\n\\n FY 16 FY 17 \\n\\n 6,246 7,043 \\n\\n FY 18 \\n\\n -9,071 \\n\\n FY 19 \\n\\n -15,806 3,296 3,05,201 \\n\\n Consolidated \\n\\n 9.738 \\n\\n Energy consumption per MMT of cargo handled 32% from previous year 48% from the base year \\n\\n Renewable Energy (GJ) Intensity (GJ/MMT) \\n\\n o 25,65,281 1,748 44,610 -23.42.188 -22,95,625 -18,851 15,503 -14,356 \\n\\n FY 16 FY 17 \\n\\n 52,851 18,50,402 \\n\\n FY 18 FY 19 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17.png', 'pagenum': 17, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8128389164472071, 'company name': 'Adani Ports and Special Economic Zone Ltd', 'category': 'Standalone (in %)', 'variable description': 'Percentage of non-renewable energy consumption in standalone operations', 'variable': 'Non-Renewable Energy', 'value': '87', 'date': '', 'unit': '%', 'fuzzy_verification_flag': False, 'lm_verification_flag': False, 'context': '"Adani Ports and Special Economic Zone Limited \\n\\n 32 \\n\\n Types of fuel and coolant consumed during FY 19 \\n\\n 1,055 kg 30kg 2.5 kg R22 R410 R134 \\n\\n Energy \\n\\n FY 19 \\n\\n FY 17 \\n\\n FY 16 \\n\\n FY \\n\\n 27.8 ML \\n\\n Diesel \\n\\n 18 \\n\\n 52.8 kL \\n\\n Petrol \\n\\n Mix \\n\\n 4.3KL \\n\\n Furnace Oil \\n\\n There is also a significant impact that we face in terms of cost of acquisition of power 8 fuel. Over the years, the per capita cost has increased further putting pressure on our profitability. So, switching to a cleaner fuel may be of strategic importance for us in the longer term. \\n\\n 82 kg 2,317 kg R407C Acetylene \\n\\n Fuel \\n\\n 28,272 kg LPG \\n\\n Standalone (in %) \\n\\n 87% \\n\\n 39% \\n\\n 57% \\n\\n 58% \\n\\n 42% \\n\\n 59% \\n\\n 43% \\n\\n 5% \\n\\n 14,139 SCM PNG \\n\\n FY 16 \\n\\n Grid \\n\\n Energy \\n\\n FY 19 \\n\\n FY 18 \\n\\n FY \\n\\n Mix \\n\\n 17 \\n\\n Renewable \\n\\n Consolidated \\n\\n 52% \\n\\n 65% \\n\\n 67% \\n\\n 72% \\n\\n (in %) \\n\\n 45% \\n\\n 33% \\n\\n 33% \\n\\n 28% \\n\\n Sustainability Report FY 19 \\n\\n At our ports, energy is primarily used for crane operations, transportation of cargo within campus through trucks 8 conveyor belts, automatic cargo handling system and tug boat. During the FY 19, our total energy consumption was 19,03,253 GJ and total GHG emissions were 2,70,170 tCOe. Through a series of interventions, including technological and process changes we have been able to reduce our energy intensity year-on-year since our base year. \\n\\n Energy Standalone \\n\\n Energy consumption per MMT of cargo handled 47% from previous year KY 67% from the base year \\n\\n Non- Renewable Energy (GJ) \\n\\n 9,906 \\n\\n -7,82,276 1,743 -6.81.000 \\n\\n FY 16 FY 17 \\n\\n 6,246 7,043 \\n\\n FY 18 \\n\\n -9,071 \\n\\n FY 19 \\n\\n -15,806 3,296 3,05,201 \\n\\n Consolidated \\n\\n 9.738 \\n\\n Energy consumption per MMT of cargo handled 32% from previous year 48% from the base year \\n\\n Renewable Energy (GJ) Intensity (GJ/MMT) \\n\\n o 25,65,281 1,748 44,610 -23.42.188 -22,95,625 -18,851 15,503 -14,356 \\n\\n FY 16 FY 17 \\n\\n 52,851 18,50,402 \\n\\n FY 18 FY 19 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17.png', 'pagenum': 17, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf'}
]
"""
# ### save df_quant_verified locally
df_quant_verified.to_csv(f'/tmp/df_quant_verified.csv', index=False)

# ## Verify extracted company names
"""
Similar to verifying extracted quant values, we can also run one layer of verification for company names, to minimize any errors for values getting attributed to incorrect companies. 
"""

# ### Verify company names in verified quant files
tasks = [
    bg_async.async_verify_data(
        doc_name=doc_name,
        file_pattern='data_type=verification/**/variable_desc=verified-variable-values/**/source=var-variable_val-value/**.csv',
        val_col='company name',
        context_col='context',
        verification_type='values',
    )
    for doc_name in doc_names
]
verify_company_responses = utils.async_utils.run_async_tasks(tasks)

# ### read quant files with verified values and company names
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=verification/**/variable_desc=verified-values/**/source=company-name/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quant_verified = utils.async_utils.run_async_tasks(tasks)
df_quant_verified = [resp.get_output() for resp in df_quant_verified]
df_quant_verified = [pd.DataFrame(df) for df in df_quant_verified]
df_quant_verified = pd.concat(df_quant_verified)
"""
Number of documents with verified values and company name files, `len(df_quant_verified['doc_name'].unique())`: 48
df_quant_verified columns, `list(df_quant_verified.columns)`
['category', 'company name', 'company name_verification_flag', 'context', 'context_file', 'doc_name', 'fuzzy_verification_flag', 'img_file', 'lm_verification_flag', 'pagenum', 'query', 'row_id', 'score', 'value', 'variable', 'variable description', 'date', 'unit']
As we can see, this data contains an additional column, `company name_verification_flag`, which comes from the call to `/verify_data` with `company name` as the value column. 
"""

# ## Extract document info
"""
In order to better contextualise the information extracted from within the documents, we will now use `/extract_doc_info` 
to extract some key document-level information, e.g. document organisation, document year, document type, etc. 
By default `/extract_doc_info' uses a pre-defined list of document types, which includes documents related to corporate disclosures, 
e.g. annual reports, sustainability reports, press releases, etc. However, you can also pass your own list of choices for document types 
you want to classify documents into. For this example, we will stick to the default choices. 
"""

# ### start time
doc_info_extraction_start_time = time.time()
"""
doc_info_extraction_start_time
1695727051.7271008
"""

# ### trigger doc info extraction
tasks = [
    bg_async.async_extract_doc_info(
        doc_name=doc_name,
    )
    for doc_name in doc_names
]
df_doc_info = utils.async_utils.run_async_tasks(tasks)

# ### read extracted doc info
df_doc_info = [resp.get_output() for resp in df_doc_info]
# convert to dataframe
df_doc_info = [pd.DataFrame(df) for df in df_doc_info]
df_doc_info = pd.concat(df_doc_info)
logger.info(f"length of df_doc_info: {len(df_doc_info)}")
"""
Number of documents in df_doc_info: `len(df_doc_info['doc_name'].unique())`: 53
df_doc_info.head().to_dict('records')
[
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express', 'doc_type': "['annual report']", 'doc_year': 2021, 'num_pages': 8}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'doc_org': 'BillerudKorsnäs', 'doc_type': "['annual report']", 'doc_year': 2021, 'num_pages': 132}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'doc_org': 'Air New Zealand', 'doc_type': "['sustainability report']", 'doc_year': 2021, 'num_pages': 1}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf', 'doc_org': 'UPM', 'doc_type': "['annual report']", 'doc_year': 2021, 'num_pages': 119}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'doc_org': 'Air New Zealand', 'doc_type': "['anti-corruption policy']", 'doc_year': 2019, 'num_pages': 4}
]
"""

# ### merge doc_info with verified quant data
df_quant_verified = pd.merge(
    left=df_quant_verified,
    right=df_doc_info,
    on=['doc_name'],
    how='left'
)

# ## Organise KPIs by company name
"""
Once we have the document meta-data merged with extracted values, we can group extracted values 
by company names, so synthesize values across documents for the same company
"""

# ### clean company name
"""
By default, we will have company name extracted both from document meta-data and specific content within the document. 
The company names extracted from within the content are useful if a company is reporting some value for a subsidiary, or a specific business unit, or it's parent entity, etc. 
However, for many of the values disclosed in the contents of the document, no specific company name may be mentioned, as it is understood that these values belong to the same 
company, who published the document. In such cases, where no company name is explicitly provided in the contents of the document, we can use the company name from the meta-data of the document.
"""
## fill missing values with na
df_quant_verified = df_quant_verified.fillna('')
## convert company name and doc_org columns to str
for col in ['company name', 'doc_org']:
    df_quant_verified[col] = df_quant_verified[col].map(str)
## mask for rows where company name is none, but doc_org (extracted from document meta-data) is not None
empty_vals = ['', 'nan']
mask = (df_quant_verified['company name'].isin(empty_vals)) & \
       (~df_quant_verified['doc_org'].isin(empty_vals))
## set doc_org as company name for masked rows
df_quant_verified.loc[mask, 'company name'] = df_quant_verified.loc[mask, 'doc_org']
logger.info(f"unique company names after cleaning: {list(df_quant_verified['company name'].unique())}")
"""
Unique company names, `list(df_quant_verified['company name'].unique())`
[
    'eROI', 
    'Adani Ports and Special Economic Zone Limited', 
    '3M', 
    'Compass Group', 
    'ABB', 
    'Ecolab', 
    'American Express', 
    'RAVEN PROPERTY GROUP LIMITED', 
    'WEBUILD', 
    'Webuild Group', 
    'Arch Capital Group Ltd.', 
    'Aggreko plc', 
    'Webuild', 
    'Adani Ports and Special Economic Zone Ltd', 
    'VINCI', 
    'SCGG', 
    'Ashtead Group', 
    'AIG', 
    'Raven Property Group Limited', 
    'Samsung SDS', 
    'Aker Carbon', 
    'Admiral Group PLC', 
    'KIN + CARTA', 
    'Air New Zealand', 
    'AIR NEW ZEALAND', 
    'Ashtead Group plc', 
    'Accor', 
    'Raven Property Group', 
    'Webuild S.p.A.', 
    'Deutsche EuroShop', 
    'Sunbelt Rentals', 
    'Weapons Down Gloves Up', 
    'Capita', 
    'UPM', 
    'Admiral Group', 
    'Boliden', 
    'adesso Group', 
    'THE a2 MILK COMPANY LIMITED', 
    'APSEZ', 
    'Massey University / Spark', 
    'Embassy Village', 
    'Bank of America', 
    'BillerudKorsnäs', 
    'China Education Group Holdings Limited', 
    'Lenzing', 
    'DEG Deutsche EuroShop', 
    'Admiral Group plc', 
    'Compass Group PLC', 
    'Fresh by Eurest initiatives', 
    'CGG', 
    'webuild', 
    'a2 Milk Company', 
    'Arch', 
    'Company Name', 
    'CHINA EDUCATION GROUP HOLDINGS LIMITED', 
    'Savills', 
    'Savills plc', 
    'Aviva', 
    'Aviva Investors', 
    'China Education Group Holdings', 
    'Jiangxi Provincial', 
    'Albioma', 
    'Lenzing Group', 
    'Sunbelt', 
    'adesso SE', 
    'CGG SA', 
    'WEBUILD S.p.A.', 
    'RANUTS', 
    'Aviva plc', 
    'Arch Insurance', 
    'Fonds Stratégique de', 
    'Bayer', 
    'Mondi', 
    'ARKEMA', 
    'Participations (FSP)', 
    'Isabelle Boccon-Gibod', 
    'Allianz MD', 
    'Kier'
]
"""
## set company names that failed company name verification to doc_org
mask = (df_quant_verified['company name_verification_flag'] != True)
df_quant_verified.loc[mask, 'company name'] = df_quant_verified.loc[mask, 'doc_org']
"""
Unique company names, `list(df_quant_verified['company name'].unique())`
[
    'American Express', 
    'BillerudKorsnäs', 
    'Air New Zealand', 
    'UPM', 
    'American International Group, Inc.', 
    'AIG', 
    'Aviva plc', 
    'China Education Group Holdings Limited', 
    'CHINA EDUCATION GROUP HOLDINGS LIMITED', 
    'Jiangxi Provincial', 
    'China Education Group Holdings', 
    'RAVEN PROPERTY GROUP LIMITED', 
    'Accor', 
    'Admiral Group plc', 
    'Admiral Group', 
    'DEG Deutsche EuroShop', 
    'Deutsche EuroShop', 
    'Raven Property Group Limited', 
    'Raven Property Group', 
    'Boliden', 
    'Albioma', 
    'Aker Carbon', 
    'ABB', 
    '3M', 
    'Webuild S.p.A.', 
    'Webuild Group', 
    'Webuild', 
    'VINCI', 
    'Allianz MD', 
    'Arch', 
    'Arch Capital Group Ltd.', 
    'Air New Zealand Limited', 
    'Ecolab', 
    'ECOLAB', 
    'Samsung SDS', 
    'Bayer', 
    'WEBUILD', 
    'webuild', 
    'Aggreko plc', 
    'Ashtead Group plc', 
    'Sunbelt Rentals', 
    'Weapons Down Gloves Up', 
    'Embassy Village', 
    'Ashtead Group', 
    'Sunbelt', 
    'Kier Group plc', 
    'Adani Ports and Special Economic Zone Limited', 
    'Adani Ports and Special Economic Zone Ltd', 
    'KIN +CARTA', 
    'KIN + CARTA', 
    'SCGG',
    'CGG SA', 
    'CGG', 
    'Lenzing Group', 
    'Lenzing', 
    'adesso SE', 
    'adesso Group', 
    'Mondi', 
    'ARKEMA', 
    'Isabelle Boccon-Gibod', 
    'Participations (FSP)', 
    'Responsible Business Report', 
    'Capita', 
    'APSEZ', 
    'Compass Group', 
    'Bank of America', 
    'COMPASS GROUP', 
    'Fresh by Eurest initiatives',
    'Compass Group PLC', 
    'Aviva Investors', 
    'Aviva', 
    'WEBUILD S.p.A.', 
    'THE a2 MILK COMPANY LIMITED', 
    'a2 Milk Company', 
    'Arch Insurance', 
    'Savills', 
    'Savills plc', 
    'AIR NEW ZEALAND', 
    'Massey University / Spark'
]
As we can see, replacing company name with doc_org for rows where company name verification failed, fixes a couple of incorrect company names we had previously, ['Company Name', 'Fonds Stratégique de', 'eROI']. 
It is worth noting here that we could also flag specific rows in the data where `company name` does not match `doc_org`, and 
then run a more verification only for these rows, as these are the rows where company name needs more attention, as it is being extracted as something different from the document-level company name, 
and may in some cases just be the name of a product or business unit, rather a company per se. However, we will skip such deeper verification in this example, and use the `company name` as extracted, whenever 
it passes our previous verification check, and use `doc_org` otherwise.
"""
## save df_quant_verified locally
df_quant_verified.to_csv(f'/tmp/df_quant_verified.csv', index=False)

# ## Standardise company names
"""
Since the company names are extracted from different pages and sections of a document, these names may take slightly different form, 
e.g. we have pairs ('American International Group, Inc.', 'AIG') and ('China Education Group Holdings Limited', 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'China Education Group Holdings'), 
which correspond to the same company name written with small variations. We can standardise these names to make our downstream analysis easier, and to be able to use `company name` as a useful grouping column.
"""
# ### trigger company name standardisation
name_std_resp = bg_async.standardise_names(
    data=df_quant_verified[['company name']].drop_duplicates().to_dict('records'),
    text_col='company name',
    name_keyword='company name',
)
## get output
df_std_company_names = name_std_resp.get_output()
df_std_company_names = pd.DataFrame(df_std_company_names)
"""
Standardised company names, `df_std_company_names[['orig_name', 'std_name']].to_dict('records')`
[
    {'orig_name': 'American Express', 'std_name': 'American Express'}, 
    {'orig_name': 'BillerudKorsnäs', 'std_name': 'BillerudKorsnäs'}, 
    {'orig_name': 'Air New Zealand', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'UPM', 'std_name': 'UPM'}, 
    {'orig_name': 'American International Group, Inc.', 'std_name': 'American International Group, Inc.'}, 
    {'orig_name': 'AIG', 'std_name': 'American International Group, Inc.'}, 
    {'orig_name': 'Aviva plc', 'std_name': 'Aviva plc'}, 
    {'orig_name': 'China Education Group Holdings Limited', 'std_name': 'China Education Group Holdings Limited'}, 
    {'orig_name': 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'std_name': 'China Education Group Holdings Limited'}, 
    {'orig_name': 'Jiangxi Provincial', 'std_name': 'Jiangxi Provincial'}, 
    {'orig_name': 'China Education Group Holdings', 'std_name': 'China Education Group Holdings Limited'}, 
    {'orig_name': 'RAVEN PROPERTY GROUP LIMITED', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'Accor', 'std_name': 'Accor'}, 
    {'orig_name': 'Admiral Group plc', 'std_name': 'Admiral Group plc'}, 
    {'orig_name': 'Admiral Group', 'std_name': 'Admiral Group plc'}, 
    {'orig_name': 'DEG Deutsche EuroShop', 'std_name': 'DEG Deutsche EuroShop'}, 
    {'orig_name': 'Deutsche EuroShop', 'std_name': 'DEG Deutsche EuroShop'}, 
    {'orig_name': 'Raven Property Group Limited', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'Raven Property Group', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'Boliden', 'std_name': 'Boliden'}, 
    {'orig_name': 'Albioma', 'std_name': 'Albioma'}, 
    {'orig_name': 'Aker Carbon', 'std_name': 'Aker Carbon'}, 
    {'orig_name': 'ABB', 'std_name': 'ABB'}, 
    {'orig_name': '3M', 'std_name': '3M'}, 
    {'orig_name': 'Webuild S.p.A.', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'Webuild Group', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'Webuild', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'VINCI', 'std_name': 'VINCI'}, 
    {'orig_name': 'Allianz MD', 'std_name': 'Allianz MD'}, 
    {'orig_name': 'Arch', 'std_name': 'Arch'}, 
    {'orig_name': 'Arch Capital Group Ltd.', 'std_name': 'Arch'}, 
    {'orig_name': 'Air New Zealand Limited', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'Ecolab', 'std_name': 'Ecolab'}, 
    {'orig_name': 'ECOLAB', 'std_name': 'Ecolab'}, 
    {'orig_name': 'Samsung SDS', 'std_name': 'Samsung SDS'}, 
    {'orig_name': 'Bayer', 'std_name': 'Bayer'}, 
    {'orig_name': 'WEBUILD', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'webuild', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'Aggreko plc', 'std_name': 'Aggreko plc'}, 
    {'orig_name': 'Ashtead Group plc', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Sunbelt Rentals', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Weapons Down Gloves Up', 'std_name': 'Weapons Down Gloves Up'}, 
    {'orig_name': 'Embassy Village', 'std_name': 'Embassy Village'}, 
    {'orig_name': 'Ashtead Group', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Sunbelt', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Kier Group plc', 'std_name': 'Kier Group plc'}, 
    {'orig_name': 'Adani Ports and Special Economic Zone Limited', 'std_name': 'Adani Ports and Special Economic Zone Limited'}, 
    {'orig_name': 'Adani Ports and Special Economic Zone Ltd', 'std_name': 'Adani Ports and Special Economic Zone Limited'}, 
    {'orig_name': 'KIN +CARTA', 'std_name': 'KIN + CARTA'}, 
    {'orig_name': 'KIN + CARTA', 'std_name': 'KIN + CARTA'}, 
    {'orig_name': 'SCGG', 'std_name': 'SCGG'}, 
    {'orig_name': 'CGG SA', 'std_name': 'SCGG'}, 
    {'orig_name': 'CGG', 'std_name': 'SCGG'}, 
    {'orig_name': 'Lenzing Group', 'std_name': 'Lenzing Group'}, 
    {'orig_name': 'Lenzing', 'std_name': 'Lenzing Group'}, 
    {'orig_name': 'adesso SE', 'std_name': 'adesso SE'}, 
    {'orig_name': 'adesso Group', 'std_name': 'adesso SE'}, 
    {'orig_name': 'Mondi', 'std_name': 'Mondi'}, 
    {'orig_name': 'ARKEMA', 'std_name': 'ARKEMA'}, 
    {'orig_name': 'Isabelle Boccon-Gibod', 'std_name': 'Isabelle Boccon-Gibod'}, 
    {'orig_name': 'Participations (FSP)', 'std_name': 'Isabelle Boccon-Gibod'},
    {'orig_name': 'Responsible Business Report', 'std_name': 'Isabelle Boccon-Gibod'}, 
    {'orig_name': 'Capita', 'std_name': 'Capita'}, 
    {'orig_name': 'APSEZ', 'std_name': 'APSEZ'}, 
    {'orig_name': 'Compass Group', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Bank of America', 'std_name': 'Bank of America'}, 
    {'orig_name': 'COMPASS GROUP', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Fresh by Eurest initiatives', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Compass Group PLC', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Aviva Investors', 'std_name': 'Aviva Investors'}, 
    {'orig_name': 'Aviva', 'std_name': 'Aviva Investors'}, 
    {'orig_name': 'THE a2 MILK COMPANY LIMITED', 'std_name': 'THE a2 MILK COMPANY LIMITED'}, 
    {'orig_name': 'a2 Milk Company', 'std_name': 'THE a2 MILK COMPANY LIMITED'}, 
    {'orig_name': 'Arch Insurance', 'std_name': 'Arch Insurance'}, 
    {'orig_name': 'Savills', 'std_name': 'Savills'}, 
    {'orig_name': 'Savills plc', 'std_name': 'Savills'}, 
    {'orig_name': 'AIR NEW ZEALAND', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'Massey University / Spark', 'std_name': 'Massey University / Spark'}
]
Number of unique standardised company names: `len(df_std_company_names['orig_name'].unique())`: 78
Number of unique standardised company names: `len(df_std_company_names['std_name'].unique())`: 46
As we can see, company names have now been standardised quite well, 
and number of company names have gone down from 78 in the original names to 46 in the standardised names. 
"""

# ### merge standardised names onto df_quant_verified
df_quant_verified = pd.merge(
    left=df_quant_verified,
    right=df_std_company_names[['orig_name', 'std_name']].rename(
        columns={'orig_name': 'company name',
                 'std_name': 'company name_std'},
    ),
    on=['company name'],
    how='left',
)
"""
Check standardised names in df_quant_verified, `df_quant_verified[['company name', 'company name_std']].drop_duplicates().tail().to_dict('records')`
[
    {'company name': 'Arch Insurance', 'company name_std': 'Arch Insurance'}, 
    {'company name': 'Savills', 'company name_std': 'Savills'}, 
    {'company name': 'Savills plc', 'company name_std': 'Savills'}, 
    {'company name': 'AIR NEW ZEALAND', 'company name_std': 'Air New Zealand'}, 
    {'company name': 'Massey University / Spark', 'company name_std': 'Massey University / Spark'}
]
"""
## save df_quant_verified locally
df_quant_verified.to_csv(f'/tmp/df_quant_verified.csv', index=False)

# ## Filter most relevant data
"""
With company names standardised, we can filter data by company name, and keep most relevant rows for each KPI for each company.
"""

# ### sort data by company name, KPI
df_quant_verified = df_quant_verified.sort_values(
    by=['query', 'score', 'company name_std', ],
    ascending=False
).reset_index(drop=True)
"""
A sample of sorted data
df_quant_verified[[ 
    'query', 'score', 
    'company name_std', 'variable description', 'variable', 'value', 'unit', 'date',
    'doc_year', 'pagenum', 'doc_name'
]].head().to_dict('records')
[
    {'query': 'hazardous waste', 'score': 0.774275504945014, 'company name_std': 'Ecolab', 'variable description': 'Avoid more than 84 MILLION pounds of waste', 'variable': 'Pounds of waste', 'value': '84 million', 'unit': nan, 'date': '2021', 'doc_year': '2021', 'pagenum': 13, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_27_ecolab_corporate-responsibility-report_2021pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7736468217838872, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Achievement of 75% reduction in waste intensity', 'variable': 'Waste intensity reduction', 'value': '75%', 'unit': nan, 'date': 'FY 2021-22', 'doc_year': '2021', 'pagenum': 108, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7712695153236897, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Quantity of wastewater recycled and reused', 'variable': '-', 'value': '650', 'unit': 'ML', 'date': nan, 'doc_year': '2021', 'pagenum': 100, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7703765424786837, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Target for achieving 20% reduction in waste intensity', 'variable': 'Waste intensity reduction', 'value': '20%', 'unit': nan, 'date': 'FY 2024-25', 'doc_year': '2021', 'pagenum': 108, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7693046430369277, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Quantity of waste managed', 'variable': '-', 'value': '29,359', 'unit': 'MT', 'date': nan, 'doc_year': '2021', 'pagenum': 100, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}
]
As we can see, the top 5 rows of the sorted data are highly relevant to the query ('hazardous waste' in this case). 
"""

# # ### Keep only verified values
# df_quant_verified_filtered = \
#     df_quant_verified[df_quant_verified['lm_verification_flag'] == True].reset_index(drop=True)

# ### keep top 5 values for each company and KPI
df_quant_verified_filtered = df_quant_verified.groupby(
    by=['company name_std', 'query'],
    group_keys=False,
).apply(
    lambda x: x.sort_values(
        by=['company name_std', 'query', 'score'],
        ascending=False
    ).head(10)
).reset_index()
## relevant columns for printing data
data_cols = ['company name_std', 'query', 'score', 'variable description', 'variable', 'value', 'unit', 'date']
## check values for one company and kpi
mask = (df_quant_verified_filtered['company name_std'] == '3M') & \
       (df_quant_verified_filtered['query'] == 'gender pay gap')
logger.info(f"df_quant_verified_filtered[mask][data_cols].to_dict('records'): "
            f"{df_quant_verified_filtered[mask][data_cols].to_dict('records')}")
"""
df_quant_verified_filtered[mask][data_cols].to_dict('records')
[
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8206044541364184, 'variable description': 'Total Scope 1 and 2 location-based emissions', 'variable': 'Emissions', 'value': '73.3', 'unit': 'metric tons CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8203032984225203, 'variable description': 'Scope 1 emissions', 'variable': 'Emissions', 'value': '20,600', 'unit': 'metric tons COe', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.817225411941106, 'variable description': 'Scope 2 location-based emissions', 'variable': 'Emissions', 'value': '14,600', 'unit': 'metric tons CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8102522382382019, 'variable description': 'Scope 2 market-based emissions', 'variable': 'Emissions', 'value': '1.16', 'unit': 'metric tons COe', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8093313918397605, 'variable description': 'Total Scope 1 and 2 market-based emissions', 'variable': 'Emissions', 'value': '87.9', 'unit': 'metric tons COe', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8057149633162554, 'variable description': '% reduction Scope 1 and Scope 2 location-based emissions', 'variable': 'Emissions reduction', 'value': '73.5', 'unit': '% CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8022968369319777, 'variable description': '% reduction Scope 1 and Scope 2 market-based emissions', 'variable': 'Emissions reduction', 'value': '66.3', 'unit': '% CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.7822868484425515, 'variable description': 'Total customer avoided metric tons CO2e emissions, cumulative since 2015', 'variable': 'Total customer avoided emissions', 'value': '25', 'unit': 'metric tons CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.7601040228162862, 'variable description': '% improved energy efficiency, indexed to net sales', 'variable': 'Energy efficiency', 'value': '34.7', 'unit': '%', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.7555038645494616, 'variable description': '% renewable energy to total electricity use', 'variable': 'Renewable energy', 'value': '29.1', 'unit': '%', 'date': '2022.0'}
]
"""

# ## Process filtered data

# ### estimate values for each KPI for each document
