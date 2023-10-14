# # Extract specific KPIs from documents

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
            'amount of energy consumption',
            'amount of energy production',
        ],
    )
    for doc_name in doc_names
]
## run tasks
verify_dataset_responses = utils.async_utils.run_async_tasks(tasks)

# ### list dataset files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=verification/**/variable_desc=verified-values/**dataset*.csv"
    )
    for doc_name in doc_names
]
tabular_dataset_files = utils.async_utils.run_async_tasks(tasks)
tabular_dataset_files = [resp.get_output() for resp in tabular_dataset_files if resp.get_output() is not None]
"""
Number of documents for which dataset files are available, `len(tabular_dataset_files)`: 48
Dataset files for the first document, `tabular_dataset_files[0]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=verification/format=csv/variable_desc=verified-values/source=var-variable_val-value/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0_dataset-32258c3b794fddc61ccb56c860059c95_verified-values.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=verification/format=csv/variable_desc=verified-values/source=var-variable_val-value/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0_dataset-82da73aa63e4f12d262625048723236e_verified-values.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=verification/format=csv/variable_desc=verified-values/source=var-variable_val-value/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-0_dataset-2bbdd7d5532b826b7542438b805b1b7a_verified-values.csv'
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
['context', 'file', 'row_id', 'row_num', 'value', 'variable', 'doc_name', 'fuzzy_verification_flag', 'lm_verification_flag']
"""

# ### identify rows with verified values
"""
('fuzzy_verification_flag', 'lm_verification_flag') column contain a verification flag for the extracted ('variable', 'value') pairs, 
based on fuzzy matching and language inference, respectively. Fuzzy matching just checks whether the extracted variable and value strings 
match the context from which they are derived. Language inference uses an inference model to determine if the context implies (variable, value) pair. 
We will rely on 'lm_verification_flag' to decide whether values are accurately extracted or not.
"""
## set empty verification flag to np.nan
df_tabular_datasets.loc[df_tabular_datasets['lm_verification_flag'] == '', 'lm_verification_flag'] = np.nan
## flag rows with verified values
df_tabular_datasets['row_verification_flag'] = df_tabular_datasets.groupby(
    by=['file', 'row_num']
)['lm_verification_flag'].transform(lambda x: np.nansum(x))
"""
Total rows, `len(df_tabular_datasets)`: 14,082
Number of verified rows, `len(df_tabular_datasets[df_tabular_datasets['row_verification_flag'] > 0])`: 1158
As we can see, verification has filtered out a small number of rows that are correct, which will likely have the most reliable information.
"""
df_tabular_datasets[df_tabular_datasets['row_verification_flag'] > 0].head().to_dict('records')

## save df_tabular_datasets locally
df_tabular_datasets.to_csv(f"/tmp/df_tabular_datasets.csv", index=False)
# df_tabular_datasets.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_tabular_datasets.csv", index=False)

# ### Merge file scores and document info with df_tabular_datasets
## add page number
df_tabular_datasets['pagenum'] = [
    file.split('/')[-1].split('pagenum-')[-1].split('_')[0]
    for file in df_tabular_datasets['file']
]
df_filtered_table_sim_files['pagenum'] = [
    file.split('/')[-1].split('pagenum-')[-1].split('_')[0]
    for file in df_filtered_table_sim_files['file']
]
## add tablue num
df_tabular_datasets['tablenum'] = [
    file.split('/')[-1].split('tablenum-')[-1].split('_')[0] for file in df_tabular_datasets['file']
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
            'file', 'orig_table_file', 'context', 'row_num', 'row_verification_flag',
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
