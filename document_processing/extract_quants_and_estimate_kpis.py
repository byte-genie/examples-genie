# # Extract quants from pages, and estimate KPI values
"""
In this example, we will use previously filtered document pages (see `document_processing/filter_relevant_pages.py`)
to estimate values for our KPIs of interest. We will do so in the following steps:
- Extract and structure all quant metrics from filtered pages;
- Estimate values for specific KPIs, using structured quant data as input;
- Merge document meta-data onto estimate KPI values;
- Standardise company names and dates;
- Sort values by standardised company names and dates.
"""


# ## import necessary libraries

import os
import json
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
    secrets_file='secrets.json',
    task_mode='sync',
    overwrite=0,
    verbose=1,
)

# ## Set inputs

# ### Set documents
doc_names = [
    'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf',
    'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf',
    'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf',
    'userid_stuartcullinan_uploadfilename_jason_09_srpdf',
    'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf',
    'userid_stuartcullinan_uploadfilename_anastasia_19_china_east_education_ltd_20211228164502_62371643_enpdf',
    'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf',
    'userid_stuartcullinan_uploadfilename_28_kim_cartapdf',
    'userid_stuartcullinan_uploadfilename_karishma-03-lse_rav_2020pdf',
    'userid_stuartcullinan_uploadfilename_1_accor_mrpdf',
    'userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf',
    'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf',
    'userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf',
    'userid_stuartcullinan_uploadfilename_jeon_22_boliden_annual-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf',
    'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf',
    'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf',
    'userid_stuartcullinan_uploadfilename_jaime_allianz-group_sustainability-reportpdf',
    'userid_stuartcullinan_uploadfilename_jason_14_srpdf',
    'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-annual-financial-resultspdf',
    'userid_stuartcullinan_uploadfilename_jeon_27_ecolab_corporate-responsibility-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_16_samsung_sdspdf',
    'userid_stuartcullinan_uploadfilename_jeon_26_bayer_sustainability-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_al_9_webuild_ethics_code_1pdf',
    'userid_stuartcullinan_uploadfilename_anastasia_4_-2020-aggreko-annual-reportpdf',
    'userid_stuartcullinan_uploadfilename_12_ashteadgroup_mrpdf',
    'userid_stuartcullinan_uploadfilename_al_6_kier-2021-ara-finalpdf',
    'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf',
    'userid_stuartcullinan_uploadfilename_4_kim_cartapdfpdf', 'userid_stuartcullinan_uploadfilename_3_cgcpdf',
    'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_1_adesso_sepdfpdf',
    'userid_stuartcullinan_uploadfilename_jason_08_srpdf',
    'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf',
    'userid_stuartcullinan_uploadfilename_12_argo_blockchainpdfpdf',
    'userid_stuartcullinan_uploadfilename_13_capita_mrpdf',
    'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf',
    'userid_stuartcullinan_uploadfilename_5_compass-group_mrpdf',
    'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_uk-pay-gap-reportpdf',
    'userid_stuartcullinan_uploadfilename_karishma-04-sustainability-highlights-report-2021-19-finalpdf',
    'userid_stuartcullinan_uploadfilename_karishma-01-des-annualreport-2021-e-spdf',
    'userid_stuartcullinan_uploadfilename_al_9_relazione-governance-2021-final_eng-con-tabellepdf',
    'userid_stuartcullinan_uploadfilename_jeon_07_a2-milk-company_annual-report_2021pdf',
    'userid_stuartcullinan_uploadfilename_jason_14_gpgpdf',
    'userid_stuartcullinan_uploadfilename_karishma-04-savills-plc-ar21pdf',
    'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-greenhouse-gas-inventory-report_finalpdf',
    'userid_stuartcullinan_uploadfilename_karishma-13-air-new-zealand-sustainability-report-2020pdf'
]

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

## quant KPIs
quant_kpis = [
    kpi for kpi in kpis
    if kpi not in ['anti-corruption policies', 'anti-bribery policies']
]
"""
Quant KPIs, `quant_kpis`
['% of female representation on the board', 'hazardous waste', 'gender pay gap', 'GHG Scope 1 emissions',
 'GHG Scope 2 emissions', 'GHG Scope 3 emissions', 'Non-renewable energy consumption', 'Emissions to water',
 'Percentage of non-renewable energy production'
]
"""

# ### Set page rank threshold to ignore pages below that rank
page_rank_threshold = 2

# ### Set filtered page file
"""
See `document_processing/filter_relevant_pages.py` to see how these pages were filtered, and saved
"""
filtered_pages_file = 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_df_filtered_sim_filescsv/data_type=unstructured/format=csv/variable_desc=uploaded-document/source=stuartcullinan/df_filtered_sim_filescsv.csv'

# ### Read filtered pages
df_filtered_pages = bg_sync.read_file(filtered_pages_file)
df_filtered_pages = df_filtered_pages.get_output()
df_filtered_pages = pd.DataFrame(df_filtered_pages)

# ## Structure quants from page data files
"""
Now that we have the page data files for all the pages of interest, we can extract and structure all the quants 
from these pages, and finally filter the ones most relevant to the KPIs of interest.
"""
structured_quants_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"structuring quants for {doc_num}/{len(doc_names)}: {doc_name}")
    tasks = [
        bg_async.async_structure_page_quants(
            doc_name=doc_name,
            page_numbers=df_filtered_pages[
                (df_filtered_pages['doc_name'] == doc_name) &
                (df_filtered_pages['page_rank'] <= page_rank_threshold)
                ]['pagenum'].unique().tolist()
        )
    ]
    structured_quants_responses_ = utils.async_utils.run_async_tasks(tasks)
    structured_quants_responses = structured_quants_responses + structured_quants_responses_
    # ## wait a little to avoid rate limit errors
    # time.sleep(15)
structured_quants_files = [resp.get_output() for resp in structured_quants_responses]
# missing_structured_quants_docnames = [doc_names[file_num] for file_num, file in enumerate(structured_quants_files) if file is None]
structured_quants_files = [file for file in structured_quants_files if file is not None]
"""
Number of documents for which structured page quants files are available, `len(structured_quants_files)`: 48
Structured quants files for first document, structured_quants_files[0]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_page-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary.csv'
]
"""

# ### Flatten structured_quants_files
structured_quants_files = [file for files in structured_quants_files for file in files]
logger.info(f"Number of structured page quants files across all documents: {len(structured_quants_files)}")
"""
Number of structured page quants files across all documents, `len(structured_quants_files)`: 526
"""

# ### Combine document name, page number, and structured quant files in one dataframe

## initalise a data frame
df_structured_quants_files = pd.DataFrame()
## store quant files in the df
df_structured_quants_files['file'] = structured_quants_files
## add doc_name
df_structured_quants_files['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_structured_quants_files['file']
]
## add page number
df_structured_quants_files['pagenum'] = [
    os.path.splitext(file)[0].split('_pagenum-')[-1].split('_')[0]
    for file in df_structured_quants_files['file']
]
## convert pagenum to int
df_structured_quants_files['pagenum'] = [int(p) for p in df_structured_quants_files['pagenum']]
## merge df_structured_quants_files with df_filtered_pages
df_filtered_pages = pd.merge(
    left=df_filtered_pages,
    right=df_structured_quants_files.drop_duplicates(),
    on=['doc_name', 'pagenum'],
    how='left',
    suffixes=('_page', '_quants')
)
"""
Now, we have the relevant structured quant files for each document and page number in the same dataframe,
`df_filtered_pages[['query', 'score', 'pagenum', 'doc_name', 'file_page', 'file_quants']].head().to_dict('records')`
[
    {'query': '% of female representation on the board', 'score': 0.885834557694716, 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf', 'file_page': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jeon_01_3m-company_sustainability-report_2021pdf_pagenum-0_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv', 'file_quants': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf_pagenum-0_page-quants_structured-quant-summary.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.8688483983019661, 'pagenum': 94, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf', 'file_page': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jeon_08_abb_sustainability-report_2021pdf_pagenum-94_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv', 'file_quants': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf_pagenum-94_page-quants_structured-quant-summary.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.8520371452852017, 'pagenum': 73, 'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf', 'file_page': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/1_accor_mrpdf_pagenum-73_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv', 'file_quants': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_1_accor_mrpdf_pagenum-73_page-quants_structured-quant-summary.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.831945273415326, 'pagenum': 2, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf', 'file_page': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_09_gpgpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv', 'file_quants': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jason_09_gpgpdf_pagenum-2_page-quants_structured-quant-summary.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.8651149368646915, 'pagenum': 107, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'file_page': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jeon_21_aker-carbon-capture_annual-report_2021pdf_pagenum-107_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv', 'file_quants': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=page-quants/userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf_pagenum-107_page-quants_structured-quant-summary.csv'}
]
"""

# ### Read structured quants files
"""
Now, we can read a structured page quants files, to see the structure of this data. 
These quant files aim to extract all the quants from the page, 
and will give us all the quants available on a page in a structured from.
"""

tasks = [
    bg_sync.async_read_file(
        file=file,
        add_file=1,
    )
    for file in structured_quants_files[:10]
]
df_structured_quants_sample = utils.async_utils.run_async_tasks(tasks)
df_structured_quants_sample = [resp.get_output() for resp in df_structured_quants_sample]
df_structured_quants_sample = [pd.DataFrame(df) for df in df_structured_quants_sample]
df_structured_quants_sample = pd.concat(df_structured_quants_sample)
## filter over rows with non-empty values
df_structured_quants_sample = df_structured_quants_sample[df_structured_quants_sample['value'] != '']
df_structured_quants_sample = df_structured_quants_sample.reset_index(drop=True)
"""
Length of df_structured_quants_sample, `len(df_structured_quants_sample)`: 4293
Columns of structured quants data, `list(df_structured_quants_sample.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'pagenum', 'relevant quote from text', 'unit', 'value', 'variable', 'variable description']
Sample of structured quants data
`df_structured_quants_sample[['company name', 'category', 'variable description', 'variable', 'value', 'unit', 'date', 'relevant quote from text', 'context', 'pagenum', 'doc_name']].head().to_dict('records')`
[
    {'company name': '', 'category': 'GENDER PAY GAP', 'variable description': 'The mean gender pay gap is 14.7% for the year 2021.', 'variable': 'MEAN', 'value': '14.7%', 'unit': '', 'date': '', 'relevant quote from text': "['GENDER; MEAN', 'GENDER; MEDIAN', '% W/M; WOMEN', '% W/M; MEN', ['14.7%', '16.7%', '55%', '45%']]", 'context': '## tablenum-0\n\n[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]\n---\n## tablenum-1\n\n[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]\n---\n## tablenum-2\n\n[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]\n---\n## text-segments\n\n[\'Our 2021 Gender Pay Gap Results\', \'Our gender pay gap has seen a 1.7 percentage point improvement year-on-year, moving to 14.7% from a mean hourly pay gap of 16.4% in 2020. Our median hourly is 16.7% compared to 18.1% in 2020. pay gap The composition of our workforce remains the primary reason for our 2021 gender pay gap, as we continue to have more women in our more junior roles (lower quartiles) and more men in our senior leadership roles (upper quartiles).\', "We see this impact follow through to our bonus gap figures where, under the company\'s annual incentive programme, senior positions have a bigger proportion of their total compensation made up of performance-driven pay. The bonus pay gap reflects the higher proportion of men in senior positions than women, meaning they have higher potential bonus pay. Similarly, there is a greater proportion of men than women in roles eligible for sales incentive programmes.", \'% OF EMPLOYEES IN EACH PAY QUARTILE\', \'4\', \'HOURLY GENDER PAY GAP\', \'% RECEIVING A BONUS\', \'MEAN\', \'WOMEN\', \'WOMEN\', \'MEDIAN\', \'MEN\', \'UPPER MIDDLE\', \'LOWER MIDDLE\', \'% W/M IN THE WORKFORCE\', \'BONUS PAY GAP\', \'MEN\', \'WOMEN\', \'MEAN\', \'MEN\', \'MEDIAN\', \'DEFINITIONS AND METHODOLOGY\', \'The Gender Pay Gap and Equal Pay The gender pay gap is the difference between the average hourly pay for men and hourly pay for women across the company without comparing role, band, or seniority. Equal pay deals with the pay received by men and women who carry out the same or similar jobs - American Express has 100% equal pay globally and the gender pay gap cannot be interpreted to mean that any individual is paid more or less than colleague in the similar role. a same or\', "The Bonus Pay Gap The bonus pay gap is the difference in the average bonuses given to men and women over a 12-month period. It is influenced by the composition of a company\'s workforce in that more senior positions attract the possibility of higher bonus payments which represent a bigger proportion of the total pay an individual receives.", \'Calculating the Mean and Median The mean is determined by adding together the hourly pay rate or annual bonus amounts of all colleagues and then dividing by the number of colleagues. The median is the mid-point, or the amount paid to the individual in the middle of the list if colleagues are listed in ascending order of hourly pay or bonus.\']\n---\n', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': 'GENDER PAY GAP', 'variable description': 'The median gender pay gap is 16.7% for the year 2021.', 'variable': 'MEDIAN', 'value': '16.7%', 'unit': '', 'date': '', 'relevant quote from text': "['GENDER; MEAN', 'GENDER; MEDIAN', '% W/M; WOMEN', '% W/M; MEN', ['14.7%', '16.7%', '55%', '45%']]", 'context': '## tablenum-0\n\n[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]\n---\n## tablenum-1\n\n[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]\n---\n## tablenum-2\n\n[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]\n---\n## text-segments\n\n[\'Our 2021 Gender Pay Gap Results\', \'Our gender pay gap has seen a 1.7 percentage point improvement year-on-year, moving to 14.7% from a mean hourly pay gap of 16.4% in 2020. Our median hourly is 16.7% compared to 18.1% in 2020. pay gap The composition of our workforce remains the primary reason for our 2021 gender pay gap, as we continue to have more women in our more junior roles (lower quartiles) and more men in our senior leadership roles (upper quartiles).\', "We see this impact follow through to our bonus gap figures where, under the company\'s annual incentive programme, senior positions have a bigger proportion of their total compensation made up of performance-driven pay. The bonus pay gap reflects the higher proportion of men in senior positions than women, meaning they have higher potential bonus pay. Similarly, there is a greater proportion of men than women in roles eligible for sales incentive programmes.", \'% OF EMPLOYEES IN EACH PAY QUARTILE\', \'4\', \'HOURLY GENDER PAY GAP\', \'% RECEIVING A BONUS\', \'MEAN\', \'WOMEN\', \'WOMEN\', \'MEDIAN\', \'MEN\', \'UPPER MIDDLE\', \'LOWER MIDDLE\', \'% W/M IN THE WORKFORCE\', \'BONUS PAY GAP\', \'MEN\', \'WOMEN\', \'MEAN\', \'MEN\', \'MEDIAN\', \'DEFINITIONS AND METHODOLOGY\', \'The Gender Pay Gap and Equal Pay The gender pay gap is the difference between the average hourly pay for men and hourly pay for women across the company without comparing role, band, or seniority. Equal pay deals with the pay received by men and women who carry out the same or similar jobs - American Express has 100% equal pay globally and the gender pay gap cannot be interpreted to mean that any individual is paid more or less than colleague in the similar role. a same or\', "The Bonus Pay Gap The bonus pay gap is the difference in the average bonuses given to men and women over a 12-month period. It is influenced by the composition of a company\'s workforce in that more senior positions attract the possibility of higher bonus payments which represent a bigger proportion of the total pay an individual receives.", \'Calculating the Mean and Median The mean is determined by adding together the hourly pay rate or annual bonus amounts of all colleagues and then dividing by the number of colleagues. The median is the mid-point, or the amount paid to the individual in the middle of the list if colleagues are listed in ascending order of hourly pay or bonus.\']\n---\n', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': 'RECEIVING A BONUS', 'variable description': '97.5% of women are receiving a bonus.', 'variable': 'WOMEN', 'value': '97.5%', 'unit': '', 'date': '', 'relevant quote from text': "['% RECEIVING; WOMEN', '% RECEIVING; MEN', 'nan', 'BONUS; MEAN', 'BONUS; MEDIAN', ['97.5%', '98.6%', nan, '43.7%', '45.2%']]", 'context': '## tablenum-0\n\n[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]\n---\n## tablenum-1\n\n[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]\n---\n## tablenum-2\n\n[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]\n---\n## text-segments\n\n[\'Our 2021 Gender Pay Gap Results\', \'Our gender pay gap has seen a 1.7 percentage point improvement year-on-year, moving to 14.7% from a mean hourly pay gap of 16.4% in 2020. Our median hourly is 16.7% compared to 18.1% in 2020. pay gap The composition of our workforce remains the primary reason for our 2021 gender pay gap, as we continue to have more women in our more junior roles (lower quartiles) and more men in our senior leadership roles (upper quartiles).\', "We see this impact follow through to our bonus gap figures where, under the company\'s annual incentive programme, senior positions have a bigger proportion of their total compensation made up of performance-driven pay. The bonus pay gap reflects the higher proportion of men in senior positions than women, meaning they have higher potential bonus pay. Similarly, there is a greater proportion of men than women in roles eligible for sales incentive programmes.", \'% OF EMPLOYEES IN EACH PAY QUARTILE\', \'4\', \'HOURLY GENDER PAY GAP\', \'% RECEIVING A BONUS\', \'MEAN\', \'WOMEN\', \'WOMEN\', \'MEDIAN\', \'MEN\', \'UPPER MIDDLE\', \'LOWER MIDDLE\', \'% W/M IN THE WORKFORCE\', \'BONUS PAY GAP\', \'MEN\', \'WOMEN\', \'MEAN\', \'MEN\', \'MEDIAN\', \'DEFINITIONS AND METHODOLOGY\', \'The Gender Pay Gap and Equal Pay The gender pay gap is the difference between the average hourly pay for men and hourly pay for women across the company without comparing role, band, or seniority. Equal pay deals with the pay received by men and women who carry out the same or similar jobs - American Express has 100% equal pay globally and the gender pay gap cannot be interpreted to mean that any individual is paid more or less than colleague in the similar role. a same or\', "The Bonus Pay Gap The bonus pay gap is the difference in the average bonuses given to men and women over a 12-month period. It is influenced by the composition of a company\'s workforce in that more senior positions attract the possibility of higher bonus payments which represent a bigger proportion of the total pay an individual receives.", \'Calculating the Mean and Median The mean is determined by adding together the hourly pay rate or annual bonus amounts of all colleagues and then dividing by the number of colleagues. The median is the mid-point, or the amount paid to the individual in the middle of the list if colleagues are listed in ascending order of hourly pay or bonus.\']\n---\n', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': 'RECEIVING A BONUS', 'variable description': '98.6% of men are receiving a bonus.', 'variable': 'MEN', 'value': '98.6%', 'unit': '', 'date': '', 'relevant quote from text': "['% RECEIVING; WOMEN', '% RECEIVING; MEN', 'nan', 'BONUS; MEAN', 'BONUS; MEDIAN', ['97.5%', '98.6%', nan, '43.7%', '45.2%']]", 'context': '## tablenum-0\n\n[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]\n---\n## tablenum-1\n\n[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]\n---\n## tablenum-2\n\n[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]\n---\n## text-segments\n\n[\'Our 2021 Gender Pay Gap Results\', \'Our gender pay gap has seen a 1.7 percentage point improvement year-on-year, moving to 14.7% from a mean hourly pay gap of 16.4% in 2020. Our median hourly is 16.7% compared to 18.1% in 2020. pay gap The composition of our workforce remains the primary reason for our 2021 gender pay gap, as we continue to have more women in our more junior roles (lower quartiles) and more men in our senior leadership roles (upper quartiles).\', "We see this impact follow through to our bonus gap figures where, under the company\'s annual incentive programme, senior positions have a bigger proportion of their total compensation made up of performance-driven pay. The bonus pay gap reflects the higher proportion of men in senior positions than women, meaning they have higher potential bonus pay. Similarly, there is a greater proportion of men than women in roles eligible for sales incentive programmes.", \'% OF EMPLOYEES IN EACH PAY QUARTILE\', \'4\', \'HOURLY GENDER PAY GAP\', \'% RECEIVING A BONUS\', \'MEAN\', \'WOMEN\', \'WOMEN\', \'MEDIAN\', \'MEN\', \'UPPER MIDDLE\', \'LOWER MIDDLE\', \'% W/M IN THE WORKFORCE\', \'BONUS PAY GAP\', \'MEN\', \'WOMEN\', \'MEAN\', \'MEN\', \'MEDIAN\', \'DEFINITIONS AND METHODOLOGY\', \'The Gender Pay Gap and Equal Pay The gender pay gap is the difference between the average hourly pay for men and hourly pay for women across the company without comparing role, band, or seniority. Equal pay deals with the pay received by men and women who carry out the same or similar jobs - American Express has 100% equal pay globally and the gender pay gap cannot be interpreted to mean that any individual is paid more or less than colleague in the similar role. a same or\', "The Bonus Pay Gap The bonus pay gap is the difference in the average bonuses given to men and women over a 12-month period. It is influenced by the composition of a company\'s workforce in that more senior positions attract the possibility of higher bonus payments which represent a bigger proportion of the total pay an individual receives.", \'Calculating the Mean and Median The mean is determined by adding together the hourly pay rate or annual bonus amounts of all colleagues and then dividing by the number of colleagues. The median is the mid-point, or the amount paid to the individual in the middle of the list if colleagues are listed in ascending order of hourly pay or bonus.\']\n---\n', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': 'BONUS PAY GAP', 'variable description': 'The mean bonus pay gap is 43.7% for the year 2021.', 'variable': 'MEAN', 'value': '43.7%', 'unit': '', 'date': '', 'relevant quote from text': "['% RECEIVING; WOMEN', '% RECEIVING; MEN', 'nan', 'BONUS; MEAN', 'BONUS; MEDIAN', ['97.5%', '98.6%', nan, '43.7%', '45.2%']]", 'context': '## tablenum-0\n\n[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]\n---\n## tablenum-1\n\n[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]\n---\n## tablenum-2\n\n[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]\n---\n## text-segments\n\n[\'Our 2021 Gender Pay Gap Results\', \'Our gender pay gap has seen a 1.7 percentage point improvement year-on-year, moving to 14.7% from a mean hourly pay gap of 16.4% in 2020. Our median hourly is 16.7% compared to 18.1% in 2020. pay gap The composition of our workforce remains the primary reason for our 2021 gender pay gap, as we continue to have more women in our more junior roles (lower quartiles) and more men in our senior leadership roles (upper quartiles).\', "We see this impact follow through to our bonus gap figures where, under the company\'s annual incentive programme, senior positions have a bigger proportion of their total compensation made up of performance-driven pay. The bonus pay gap reflects the higher proportion of men in senior positions than women, meaning they have higher potential bonus pay. Similarly, there is a greater proportion of men than women in roles eligible for sales incentive programmes.", \'% OF EMPLOYEES IN EACH PAY QUARTILE\', \'4\', \'HOURLY GENDER PAY GAP\', \'% RECEIVING A BONUS\', \'MEAN\', \'WOMEN\', \'WOMEN\', \'MEDIAN\', \'MEN\', \'UPPER MIDDLE\', \'LOWER MIDDLE\', \'% W/M IN THE WORKFORCE\', \'BONUS PAY GAP\', \'MEN\', \'WOMEN\', \'MEAN\', \'MEN\', \'MEDIAN\', \'DEFINITIONS AND METHODOLOGY\', \'The Gender Pay Gap and Equal Pay The gender pay gap is the difference between the average hourly pay for men and hourly pay for women across the company without comparing role, band, or seniority. Equal pay deals with the pay received by men and women who carry out the same or similar jobs - American Express has 100% equal pay globally and the gender pay gap cannot be interpreted to mean that any individual is paid more or less than colleague in the similar role. a same or\', "The Bonus Pay Gap The bonus pay gap is the difference in the average bonuses given to men and women over a 12-month period. It is influenced by the composition of a company\'s workforce in that more senior positions attract the possibility of higher bonus payments which represent a bigger proportion of the total pay an individual receives.", \'Calculating the Mean and Median The mean is determined by adding together the hourly pay rate or annual bonus amounts of all colleagues and then dividing by the number of colleagues. The median is the mid-point, or the amount paid to the individual in the middle of the list if colleagues are listed in ascending order of hourly pay or bonus.\']\n---\n', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
As we can see, the structured quants data contains a structured dataset of extracted quants, as well as a `relevant quote from text` column, and a `context` column.  
`context` column contains the details of the page, from which the data was extracted, and 
`relevant quote from text` column contains the most relevant part of the context relevant to the extract value in that row. 
If we look into one of the context in more details, `df_structured_quants_sample['context'].tolist()[0]`
'## tablenum-0\n\n[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]\n---\n## tablenum-1\n\n[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]\n---\n## tablenum-2\n\n[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]\n---\n## text-segments\n\n[\'Our 2021 Gender Pay Gap Results\', \'Our gender pay gap has seen a 1.7 percentage point improvement year-on-year, moving to 14.7% from a mean hourly pay gap of 16.4% in 2020. Our median hourly is 16.7% compared to 18.1% in 2020. pay gap The composition of our workforce remains the primary reason for our 2021 gender pay gap, as we continue to have more women in our more junior roles (lower quartiles) and more men in our senior leadership roles (upper quartiles).\', "We see this impact follow through to our bonus gap figures where, under the company\'s annual incentive programme, senior positions have a bigger proportion of their total compensation made up of performance-driven pay. The bonus pay gap reflects the higher proportion of men in senior positions than women, meaning they have higher potential bonus pay. Similarly, there is a greater proportion of men than women in roles eligible for sales incentive programmes.", \'% OF EMPLOYEES IN EACH PAY QUARTILE\', \'4\', \'HOURLY GENDER PAY GAP\', \'% RECEIVING A BONUS\', \'MEAN\', \'WOMEN\', \'WOMEN\', \'MEDIAN\', \'MEN\', \'UPPER MIDDLE\', \'LOWER MIDDLE\', \'% W/M IN THE WORKFORCE\', \'BONUS PAY GAP\', \'MEN\', \'WOMEN\', \'MEAN\', \'MEN\', \'MEDIAN\', \'DEFINITIONS AND METHODOLOGY\', \'The Gender Pay Gap and Equal Pay The gender pay gap is the difference between the average hourly pay for men and hourly pay for women across the company without comparing role, band, or seniority. Equal pay deals with the pay received by men and women who carry out the same or similar jobs - American Express has 100% equal pay globally and the gender pay gap cannot be interpreted to mean that any individual is paid more or less than colleague in the similar role. a same or\', "The Bonus Pay Gap The bonus pay gap is the difference in the average bonuses given to men and women over a 12-month period. It is influenced by the composition of a company\'s workforce in that more senior positions attract the possibility of higher bonus payments which represent a bigger proportion of the total pay an individual receives.", \'Calculating the Mean and Median The mean is determined by adding together the hourly pay rate or annual bonus amounts of all colleagues and then dividing by the number of colleagues. The median is the mid-point, or the amount paid to the individual in the middle of the list if colleagues are listed in ascending order of hourly pay or bonus.\']\n---\n'
We can see that a context has different sections, like `## tablenum-0`, `## tablenum-1`, `## text-segments`, etc. 
These sections correspond to different parts of the page, from which the data was extracted. 
For example, `## tablenum-0` corresponds to the first table on the page, 
`## tablenum-1` corresponds to the second table on the page, 
and `## text-segments` corresponds to the remaining text segments (passages) on the page.
"""

# ## Verify quant files
"""
At this point, we could run one layer of automated verification on extracted quants, to filter out any incorrectly extracted quants. 
We can use `/verify_data` endpoint for this purpose. `/verify_data` takes a pair of (variable, value) column names, 
and a context column name, from which (variable, value) pair was extracted, 
and verifies whether the (variable, value) is correctly extracted based on the context.
Here is a sample call to `/verify_data`
verify_data_responses = bg_async.verify_data(
    files=input_files,
    var_col='variable',
    val_col='value',   
    context_col='context',
) 
To keep this code relatively simple, we will skip the verification at this stage, as it can also be run at a later stage.
"""



# ## Estimate values for KPIs
"""
Now that we have extracted quants from relevant pages, we can use these quant data files to estimate the values for our specific KPIs. 
This estimation is helpful, as it can re-write the extracted values to make it more relevant to our desired KPIs, 
and remove the values from our data that are not needed for estimating our KPIs.
"""

# ### Trigger value estimation for KPIs
"""
To avoid rate limit errors, we will trigger value estimation for one document at a time
"""
value_estimation_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"Running value estimation for ({doc_num}/{len(doc_names)}): {doc_name}")
    ## structured quant files for the document
    quant_files = df_filtered_pages[
        (df_filtered_pages['doc_name'] == doc_name) &
        (df_filtered_pages['page_rank'] <= page_rank_threshold)
        ]['file_quants'].unique().tolist()
    if len(quant_files) <= 0:
        logger.info(f"No files ranked in top {page_rank_threshold} in this document")
    ## define tasks
    tasks = [
        bg_async.async_estimate_values(
            files=[quant_file],
            metrics_to_estimate=df_filtered_pages[
                (df_filtered_pages['file_quants'] == quant_file) &
                (df_filtered_pages['page_rank'] <= page_rank_threshold)
            ]['query'].unique().tolist(),
            attrs_to_estimate=[
                'company name', 'quantity name',
                'quantity description', 'quantitative value',
                'unit or currency of value', 'date',
            ],
            cols_to_use=[
                'company name',
                'variable description', 'category', 'variable',
                'value', 'date', 'unit', 'relevant quote from text'
            ],
            non_null_cols=['value'],
        )
        for quant_file in quant_files
    ]
    ## run tasks
    value_estimation_responses_ = utils.async_utils.run_async_tasks(tasks)
    value_estimation_responses = value_estimation_responses + value_estimation_responses_
    # ## wait for 30 sec for each file to avoid rate limits
    # time.sleep(len(quant_files) * 15)

# ### Get estimated value files
value_estimation_files = [resp.get_output() for resp in value_estimation_responses]
value_estimation_files = [file for file in value_estimation_files if file is not None]
## flatten files
value_estimation_files = [file for files in value_estimation_files for file in files]
"""
Total number of estimated value files, `len(value_estimation_files)`: 463
First 5 estimated value files, `value_estimation_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_page-quants_structured-quant-summary_estimated-values_metrics-of-female-representation-on-the-board.csv',
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-51_page-quants_structured-quant-summary_estimated-values_metrics-of-female-representation-on-the-board.csv',
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-36_page-quants_structured-quant-summary_estimated-values_metrics-of-female-representation-on-the-board_gender-pay-gap.csv'
]
"""

# ### Read estimated value files
tasks = [
    bg_sync.async_read_files(
        files=value_estimation_files,
        add_file=1
    )
]
df_estimated_kpi = utils.async_utils.run_async_tasks(tasks)
df_estimated_kpi = [resp.get_output() for resp in df_estimated_kpi]
df_estimated_kpi = [pd.DataFrame(df) for df in df_estimated_kpi]
df_estimated_kpi = pd.concat(df_estimated_kpi)
## keep only relevant columns
cols_to_keep = [
    'company name', 'quantity description', 'quantity name', 'quantitative value',
    'unit or currency of value', 'date', 'context', 'file',
]
df_estimated_kpi = df_estimated_kpi[cols_to_keep]
"""
Number of rows in df_estimated_kpi, `len(df_estimated_kpi)`: 1584
Columns of df_estimated_kpi, `list(df_estimated_kpi.columns)`
['company name', 'quantity description', 'quantity name', 'quantitative value', 'unit or currency of value', 'date', 'context', 'file']
First few rows of df_estimated_kpi, `df_estimated_kpi.head().to_dict('records')`
[{'company name': 'American Express',
  'quantity description': '50:50 gender balance at senior level by 2024, nearly equal gender representation at first level senior leadership roles',
  'quantity name': '% of female representation on the board', 'quantitative value': '', 'unit or currency of value': '',
  'date': '',
  'context': '[["company name", "relevant quote from text"], ["", "3"], ["", "We\'re moving in the right direction to meet our HM Treasury\'s Women in Finance Charter pledge to achieve 50:50 gender balance at this level in the UK by 2024. Encouragingly, the number of women in first level senior leadership roles has almost trebled, meaning that there are now almost as many women as men at this level."], ["", "One of my priorities as a senior leader is ensuring that our culture is not only equal, but also inclusive. Every one of our 5,000 plus colleagues in the UK should be able to bring their whole self to work. To this end, we have inclusive recruitment practices and enterprise-wide training, ensuring we are hiring and developing the best talent from diverse backgrounds. We\'ve also recently introduced an exciting new enterprise working model. Amex Flex offers colleagues flexible ways of working, taking the best of what we\'ve learnt through the pandemic. It\'s been designed to suit colleagues\' lifestyles and commitments, while preserving the important benefits of our unique in-person culture."], ["", "Many colleagues have been juggling workloads with additional pressures and increased responsibilities at home, such as caregiving responsibilities, which often disproportionately fall on women. Our new model of working is intended to help support all colleagues in their work and home life. As a woman in a senior leadership position in a business in the UK financial sector, I passionately believe it is my responsibility to pay it forward, making it easier for those who follow. At American Express, we are committed to continue to support all colleagues fulfilling their ambitions and to ensure our organization reflects the diversity of the communities we serve. I confirm that the data in this report is accurate."], ["American Express", "Charlotte"], ["", "LinkedIn Top Companies List 2021 Top 25 workplaces in the UK for career growth"], ["", "Working Families Top 10 Family Friendly Employer"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_page-quants_structured-quant-summary_estimated-values_metrics-of-female-representation-on-the-board.csv'},
 {'company name': 'Company', 'quantity description': 'Mean', 'quantity name': 'Gender Pay Gap',
  'quantitative value': '14.7%', 'unit or currency of value': 'Percentage', 'date': 2021,
  'context': '[["variable description", "category", "variable", "value", "relevant quote from text"], ["The mean gender pay gap is 14.7% for the year 2021.", "GENDER PAY GAP", "MEAN", "14.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["The median gender pay gap is 16.7% for the year 2021.", "GENDER PAY GAP", "MEDIAN", "16.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["97.5% of women are receiving a bonus.", "RECEIVING A BONUS", "WOMEN", "97.5%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["98.6% of men are receiving a bonus.", "RECEIVING A BONUS", "MEN", "98.6%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The mean bonus pay gap is 43.7% for the year 2021.", "BONUS PAY GAP", "MEAN", "43.7%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The median bonus pay gap is 45.2% for the year 2021.", "BONUS PAY GAP", "MEDIAN", "45.2%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["51% of employees in the company are women.", "% OF EMPLOYEES", "WOMEN", "51%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["49% of employees in the company are men.", "% OF EMPLOYEES", "MEN", "49%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["56% of women have a bonus pay gap.", "BONUS PAY GAP", "WOMEN", "56%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["44% of men have a bonus pay gap.", "BONUS PAY GAP", "MEN", "44%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv'},
 {'company name': 'Company', 'quantity description': 'Median', 'quantity name': 'Gender Pay Gap',
  'quantitative value': '16.7%', 'unit or currency of value': 'Percentage', 'date': 2021,
  'context': '[["variable description", "category", "variable", "value", "relevant quote from text"], ["The mean gender pay gap is 14.7% for the year 2021.", "GENDER PAY GAP", "MEAN", "14.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["The median gender pay gap is 16.7% for the year 2021.", "GENDER PAY GAP", "MEDIAN", "16.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["97.5% of women are receiving a bonus.", "RECEIVING A BONUS", "WOMEN", "97.5%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["98.6% of men are receiving a bonus.", "RECEIVING A BONUS", "MEN", "98.6%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The mean bonus pay gap is 43.7% for the year 2021.", "BONUS PAY GAP", "MEAN", "43.7%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The median bonus pay gap is 45.2% for the year 2021.", "BONUS PAY GAP", "MEDIAN", "45.2%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["51% of employees in the company are women.", "% OF EMPLOYEES", "WOMEN", "51%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["49% of employees in the company are men.", "% OF EMPLOYEES", "MEN", "49%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["56% of women have a bonus pay gap.", "BONUS PAY GAP", "WOMEN", "56%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["44% of men have a bonus pay gap.", "BONUS PAY GAP", "MEN", "44%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv'},
 {'company name': 'AMEX SERVICES EUROPE LTD AESEL', 'quantity description': 'Mean gender pay gap',
  'quantity name': 'Mean Gender Pay Gap', 'quantitative value': '14%', 'unit or currency of value': '',
  'date': '5 April 2021',
  'context': '[["company name", "variable description", "category", "variable", "value", "date", "relevant quote from text"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEAN GENDER PAY", "14%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN GENDER PAY\', \'14%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "15.8%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN GENDER PAY\', \'15.8%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "45%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN BONUS PAY GAP\', \'45%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.7%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN BONUS PAY\', \'45.7%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of women employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.3%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'97.3%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of men employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Mean gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEAN GENDER PAY", "13.3%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN GENDER PAY\', \'13.3%\']"], ["AMEX EUROPE LTD AEEL", "Median gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "7.7%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN GENDER PAY\', \'7.7%\']"], ["AMEX EUROPE LTD AEEL", "Mean bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "35.9%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN BONUS PAY GAP\', \'35.9%\']"], ["AMEX EUROPE LTD AEEL", "Median bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "15.0%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN BONUS PAY\', \'15.0%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of women employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of men employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "99.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'99.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN GENDER PAY\', \'8.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "10.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN GENDER PAY\', \'10.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "29.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN BONUS PAY GAP\', \'29.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "47.5%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN BONUS PAY\', \'47.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of women employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "96.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'96.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of men employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.8%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'98.8%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.7%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN GENDER PAY\', \'8.7%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN GENDER PAY\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN BONUS PAY GAP\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "7.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN BONUS PAY\', \'7.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of women employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "99.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'99.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of men employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'98.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEAN GENDER PAY", "14.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN GENDER PAY\', \'14.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEDIAN GENDER PAY", "16.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN GENDER PAY\', \'16.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "43.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN BONUS PAY GAP\', \'43.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.2%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN BONUS PAY\', \'45.2%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of women employees receiving bonus for American Express Overall UK", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.5%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'97.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of men employees receiving bonus for American Express Overall UK", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.6%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'98.6%\']"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv'},
 {'company name': 'AMEX SERVICES EUROPE LTD AESEL', 'quantity description': 'Median gender pay gap',
  'quantity name': 'Median Gender Pay Gap', 'quantitative value': '15.8%', 'unit or currency of value': '',
  'date': '5 April 2021',
  'context': '[["company name", "variable description", "category", "variable", "value", "date", "relevant quote from text"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEAN GENDER PAY", "14%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN GENDER PAY\', \'14%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "15.8%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN GENDER PAY\', \'15.8%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "45%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN BONUS PAY GAP\', \'45%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.7%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN BONUS PAY\', \'45.7%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of women employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.3%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'97.3%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of men employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Mean gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEAN GENDER PAY", "13.3%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN GENDER PAY\', \'13.3%\']"], ["AMEX EUROPE LTD AEEL", "Median gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "7.7%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN GENDER PAY\', \'7.7%\']"], ["AMEX EUROPE LTD AEEL", "Mean bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "35.9%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN BONUS PAY GAP\', \'35.9%\']"], ["AMEX EUROPE LTD AEEL", "Median bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "15.0%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN BONUS PAY\', \'15.0%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of women employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of men employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "99.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'99.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN GENDER PAY\', \'8.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "10.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN GENDER PAY\', \'10.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "29.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN BONUS PAY GAP\', \'29.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "47.5%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN BONUS PAY\', \'47.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of women employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "96.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'96.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of men employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.8%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'98.8%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.7%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN GENDER PAY\', \'8.7%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN GENDER PAY\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN BONUS PAY GAP\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "7.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN BONUS PAY\', \'7.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of women employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "99.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'99.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of men employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'98.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEAN GENDER PAY", "14.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN GENDER PAY\', \'14.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEDIAN GENDER PAY", "16.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN GENDER PAY\', \'16.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "43.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN BONUS PAY GAP\', \'43.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.2%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN BONUS PAY\', \'45.2%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of women employees receiving bonus for American Express Overall UK", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.5%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'97.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of men employees receiving bonus for American Express Overall UK", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.6%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'98.6%\']"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv'}
]
"""

# ### Filter rows with non-empty values
df_estimated_kpi = df_estimated_kpi[df_estimated_kpi['quantitative value'] != ''].reset_index(drop=True)
## add pagenum
df_estimated_kpi['pagenum'] = [
    int(os.path.splitext(file)[0].split('_pagenum-')[-1].split('_')[0])
    for file in df_estimated_kpi['file']
]
## add doc_name
df_estimated_kpi['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_estimated_kpi['file']
]

# ### Merge estimated values with document info
cols_to_merge = ['doc_name', 'pagenum', 'doc_org', 'doc_org_std', 'doc_type', 'doc_year', 'num_pages']
df_estimated_kpi = pd.merge(
    left=df_estimated_kpi,
    right=df_filtered_pages[cols_to_merge].drop_duplicates(),
    on=['doc_name', 'pagenum'],
    how='left',
    suffixes=('_kpi', '_page')
)
"""
First few rows of df_estimated_kpi, `df_estimated_kpi.head().to_dict('records')`
[{'company name': 'Company', 'quantity description': 'Mean', 'quantity name': 'Gender Pay Gap',
  'quantitative value': '14.7%', 'unit or currency of value': 'Percentage', 'date': 2021,
  'context': '[["variable description", "category", "variable", "value", "relevant quote from text"], ["The mean gender pay gap is 14.7% for the year 2021.", "GENDER PAY GAP", "MEAN", "14.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["The median gender pay gap is 16.7% for the year 2021.", "GENDER PAY GAP", "MEDIAN", "16.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["97.5% of women are receiving a bonus.", "RECEIVING A BONUS", "WOMEN", "97.5%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["98.6% of men are receiving a bonus.", "RECEIVING A BONUS", "MEN", "98.6%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The mean bonus pay gap is 43.7% for the year 2021.", "BONUS PAY GAP", "MEAN", "43.7%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The median bonus pay gap is 45.2% for the year 2021.", "BONUS PAY GAP", "MEDIAN", "45.2%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["51% of employees in the company are women.", "% OF EMPLOYEES", "WOMEN", "51%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["49% of employees in the company are men.", "% OF EMPLOYEES", "MEN", "49%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["56% of women have a bonus pay gap.", "BONUS PAY GAP", "WOMEN", "56%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["44% of men have a bonus pay gap.", "BONUS PAY GAP", "MEN", "44%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
  'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express',
  'doc_org_std': 'American Express', 'doc_type': "['sustainability report']", 'doc_year': '2021', 'num_pages': 192},
 {'company name': 'Company', 'quantity description': 'Median', 'quantity name': 'Gender Pay Gap',
  'quantitative value': '16.7%', 'unit or currency of value': 'Percentage', 'date': 2021,
  'context': '[["variable description", "category", "variable", "value", "relevant quote from text"], ["The mean gender pay gap is 14.7% for the year 2021.", "GENDER PAY GAP", "MEAN", "14.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["The median gender pay gap is 16.7% for the year 2021.", "GENDER PAY GAP", "MEDIAN", "16.7%", "[\'GENDER; MEAN\', \'GENDER; MEDIAN\', \'% W/M; WOMEN\', \'% W/M; MEN\', [\'14.7%\', \'16.7%\', \'55%\', \'45%\']]"], ["97.5% of women are receiving a bonus.", "RECEIVING A BONUS", "WOMEN", "97.5%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["98.6% of men are receiving a bonus.", "RECEIVING A BONUS", "MEN", "98.6%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The mean bonus pay gap is 43.7% for the year 2021.", "BONUS PAY GAP", "MEAN", "43.7%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["The median bonus pay gap is 45.2% for the year 2021.", "BONUS PAY GAP", "MEDIAN", "45.2%", "[\'% RECEIVING; WOMEN\', \'% RECEIVING; MEN\', \'nan\', \'BONUS; MEAN\', \'BONUS; MEDIAN\', [\'97.5%\', \'98.6%\', nan, \'43.7%\', \'45.2%\']]"], ["51% of employees in the company are women.", "% OF EMPLOYEES", "WOMEN", "51%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["49% of employees in the company are men.", "% OF EMPLOYEES", "MEN", "49%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["56% of women have a bonus pay gap.", "BONUS PAY GAP", "WOMEN", "56%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"], ["44% of men have a bonus pay gap.", "BONUS PAY GAP", "MEN", "44%", "[\'WOMEN\', \'UPPER\', \'UPPER_2\', \'UPPER_3\', \'MEN\', [\'45%\', \'45%\', nan, nan, \'55%\'], [\'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\', \'UPPER\'], [\'51%\', nan, nan, nan, \'49%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'56%\', nan, nan, nan, \'44%\'], [\'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\', \'LOWER\'], [\'63%\', nan, nan, nan, \'37%\']]"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
  'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express',
  'doc_org_std': 'American Express', 'doc_type': "['sustainability report']", 'doc_year': '2021', 'num_pages': 192},
 {'company name': 'AMEX SERVICES EUROPE LTD AESEL', 'quantity description': 'Mean gender pay gap',
  'quantity name': 'Mean Gender Pay Gap', 'quantitative value': '14%', 'unit or currency of value': '',
  'date': '5 April 2021',
  'context': '[["company name", "variable description", "category", "variable", "value", "date", "relevant quote from text"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEAN GENDER PAY", "14%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN GENDER PAY\', \'14%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "15.8%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN GENDER PAY\', \'15.8%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "45%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN BONUS PAY GAP\', \'45%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.7%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN BONUS PAY\', \'45.7%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of women employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.3%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'97.3%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of men employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Mean gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEAN GENDER PAY", "13.3%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN GENDER PAY\', \'13.3%\']"], ["AMEX EUROPE LTD AEEL", "Median gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "7.7%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN GENDER PAY\', \'7.7%\']"], ["AMEX EUROPE LTD AEEL", "Mean bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "35.9%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN BONUS PAY GAP\', \'35.9%\']"], ["AMEX EUROPE LTD AEEL", "Median bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "15.0%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN BONUS PAY\', \'15.0%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of women employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of men employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "99.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'99.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN GENDER PAY\', \'8.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "10.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN GENDER PAY\', \'10.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "29.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN BONUS PAY GAP\', \'29.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "47.5%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN BONUS PAY\', \'47.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of women employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "96.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'96.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of men employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.8%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'98.8%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.7%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN GENDER PAY\', \'8.7%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN GENDER PAY\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN BONUS PAY GAP\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "7.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN BONUS PAY\', \'7.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of women employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "99.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'99.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of men employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'98.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEAN GENDER PAY", "14.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN GENDER PAY\', \'14.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEDIAN GENDER PAY", "16.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN GENDER PAY\', \'16.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "43.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN BONUS PAY GAP\', \'43.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.2%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN BONUS PAY\', \'45.2%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of women employees receiving bonus for American Express Overall UK", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.5%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'97.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of men employees receiving bonus for American Express Overall UK", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.6%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'98.6%\']"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
  'pagenum': 7, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express',
  'doc_org_std': 'American Express', 'doc_type': "['sustainability report']", 'doc_year': '2021', 'num_pages': 192},
 {'company name': 'AMEX SERVICES EUROPE LTD AESEL', 'quantity description': 'Median gender pay gap',
  'quantity name': 'Median Gender Pay Gap', 'quantitative value': '15.8%', 'unit or currency of value': '',
  'date': '5 April 2021',
  'context': '[["company name", "variable description", "category", "variable", "value", "date", "relevant quote from text"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEAN GENDER PAY", "14%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN GENDER PAY\', \'14%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "15.8%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN GENDER PAY\', \'15.8%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "45%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN BONUS PAY GAP\', \'45%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.7%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN BONUS PAY\', \'45.7%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of women employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.3%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'97.3%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of men employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Mean gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEAN GENDER PAY", "13.3%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN GENDER PAY\', \'13.3%\']"], ["AMEX EUROPE LTD AEEL", "Median gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "7.7%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN GENDER PAY\', \'7.7%\']"], ["AMEX EUROPE LTD AEEL", "Mean bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "35.9%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN BONUS PAY GAP\', \'35.9%\']"], ["AMEX EUROPE LTD AEEL", "Median bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "15.0%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN BONUS PAY\', \'15.0%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of women employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of men employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "99.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'99.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN GENDER PAY\', \'8.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "10.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN GENDER PAY\', \'10.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "29.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN BONUS PAY GAP\', \'29.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "47.5%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN BONUS PAY\', \'47.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of women employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "96.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'96.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of men employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.8%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'98.8%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.7%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN GENDER PAY\', \'8.7%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN GENDER PAY\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN BONUS PAY GAP\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "7.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN BONUS PAY\', \'7.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of women employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "99.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'99.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of men employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'98.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEAN GENDER PAY", "14.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN GENDER PAY\', \'14.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEDIAN GENDER PAY", "16.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN GENDER PAY\', \'16.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "43.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN BONUS PAY GAP\', \'43.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.2%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN BONUS PAY\', \'45.2%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of women employees receiving bonus for American Express Overall UK", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.5%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'97.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of men employees receiving bonus for American Express Overall UK", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.6%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'98.6%\']"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
  'pagenum': 7, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express',
  'doc_org_std': 'American Express', 'doc_type': "['sustainability report']", 'doc_year': '2021', 'num_pages': 192},
 {'company name': 'AMEX EUROPE LTD AEEL', 'quantity description': 'Mean gender pay gap',
  'quantity name': 'Mean Gender Pay Gap', 'quantitative value': '13.3%', 'unit or currency of value': '',
  'date': '5 April 2021',
  'context': '[["company name", "variable description", "category", "variable", "value", "date", "relevant quote from text"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEAN GENDER PAY", "14%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN GENDER PAY\', \'14%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median gender pay gap for AMEX Services Europe Ltd AESEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "15.8%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN GENDER PAY\', \'15.8%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Mean bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "45%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEAN BONUS PAY GAP\', \'45%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Median bonus pay gap for AMEX Services Europe Ltd AESEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.7%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'MEDIAN BONUS PAY\', \'45.7%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of women employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.3%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'97.3%\']"], ["AMEX SERVICES EUROPE LTD AESEL", "Proportion of men employees receiving bonus for AMEX Services Europe Ltd AESEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX SERVICES EUROPE LTD AESEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Mean gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEAN GENDER PAY", "13.3%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN GENDER PAY\', \'13.3%\']"], ["AMEX EUROPE LTD AEEL", "Median gender pay gap for AMEX Europe Ltd AEEL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "7.7%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN GENDER PAY\', \'7.7%\']"], ["AMEX EUROPE LTD AEEL", "Mean bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "35.9%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEAN BONUS PAY GAP\', \'35.9%\']"], ["AMEX EUROPE LTD AEEL", "Median bonus pay gap for AMEX Europe Ltd AEEL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "15.0%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'MEDIAN BONUS PAY\', \'15.0%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of women employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'98.5%\']"], ["AMEX EUROPE LTD AEEL", "Proportion of men employees receiving bonus for AMEX Europe Ltd AEEL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "99.5%", "5 April 2021", "[\'AMEX EUROPE LTD AEEL\', \'PROPORTION\', \'99.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN GENDER PAY\', \'8.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median gender pay gap for AMEX Payment Services Ltd AEPSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "10.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN GENDER PAY\', \'10.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Mean bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "29.2%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEAN BONUS PAY GAP\', \'29.2%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Median bonus pay gap for AMEX Payment Services Ltd AEPSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "47.5%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'MEDIAN BONUS PAY\', \'47.5%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of women employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "96.7%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'96.7%\']"], ["AMEX PAYMENT SERVICES LTD AEPSL", "Proportion of men employees receiving bonus for AMEX Payment Services Ltd AEPSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.8%", "5 April 2021", "[\'AMEX PAYMENT SERVICES LTD AEPSL\', \'PROPORTION\', \'98.8%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEAN GENDER PAY", "8.7%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN GENDER PAY\', \'8.7%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median gender pay gap for AMEX Group Services Ltd AEGSL", "GENDER PAY GAP", "MEDIAN GENDER PAY", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN GENDER PAY\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Mean bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "5.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEAN BONUS PAY GAP\', \'5.5%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Median bonus pay gap for AMEX Group Services Ltd AEGSL", "BONUS PAY GAP", "MEDIAN BONUS PAY", "7.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'MEDIAN BONUS PAY\', \'7.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of women employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "99.3%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'99.3%\']"], ["AMEX GROUP SERVICES LTD AEGSL", "Proportion of men employees receiving bonus for AMEX Group Services Ltd AEGSL", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.5%", "5 April 2021", "[\'AMEX GROUP SERVICES LTD AEGSL\', \'PROPORTION\', \'98.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEAN GENDER PAY", "14.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN GENDER PAY\', \'14.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median gender pay gap for American Express Overall UK", "GENDER PAY GAP", "MEDIAN GENDER PAY", "16.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN GENDER PAY\', \'16.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Mean bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEAN BONUS PAY GAP", "43.7%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEAN BONUS PAY GAP\', \'43.7%\']"], ["AMERICAN EXPRESS OVERALL UK", "Median bonus pay gap for American Express Overall UK", "BONUS PAY GAP", "MEDIAN BONUS PAY", "45.2%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'MEDIAN BONUS PAY\', \'45.2%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of women employees receiving bonus for American Express Overall UK", "PROPORTION OF WOMEN RECEIVING BONUS", "PROPORTION", "97.5%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'97.5%\']"], ["AMERICAN EXPRESS OVERALL UK", "Proportion of men employees receiving bonus for American Express Overall UK", "PROPORTION OF MEN RECEIVING BONUS", "PROPORTION", "98.6%", "5 April 2021", "[\'AMERICAN EXPRESS OVERALL UK\', \'PROPORTION\', \'98.6%\']"]]',
  'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=estimation/format=csv/variable_desc=structured-quant-summary/source=estimate_values/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_page-quants_structured-quant-summary_estimated-values_metrics-gender-pay-gap.csv',
  'pagenum': 7, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express',
  'doc_org_std': 'American Express', 'doc_type': "['sustainability report']", 'doc_year': '2021', 'num_pages': 192}
]
"""

# ## Rank values by recency
"""
Now that we have relevant values extracted and scored, we can further rank these values by recency, 
to be able to pick the most recent value for each KPI. To do so, we will follow the following steps:
- combine doc_year and value date;
- standardise dates;
- sort data by dates;
- add a recency rank, grouping data by company and KPI.
"""

# ### Combine doc_year and date
"""
Some values, especially tabular values, are assigned a date directly to them in the disclosures, 
e.g. when a company reports its revenues for multiple years in a table for different years in different columns. 
But some values are reported without mentioning any specific date, 
in which case the value likely belongs to the document year. 
Any specific date for the value will be getting picked up in `date` column, 
and the document year is present in the `doc_year` column. 
So whenever the `date` is missing, we will set it to `doc_year`
"""
mask = df_estimated_kpi['date'].isin([''])
df_estimated_kpi.loc[mask, 'date'] = df_estimated_kpi.loc[mask, 'doc_year']

# ### Extract standardised 4-digit years from dates
date_std_resp = bg_async.standardise_years(
    data=df_estimated_kpi[['date', 'doc_year']].drop_duplicates().to_dict('records'),
    time_cols=['date', 'doc_year'],
)
df_date_std = date_std_resp.get_output()
df_date_std = pd.DataFrame(df_date_std)
## set empty std_year to doc_year
mask = (df_date_std['std_year'] == '') & (df_date_std['doc_year'] != '')
df_date_std.loc[mask, 'std_year'] = df_date_std.loc[mask, 'doc_year']

## Merge standardised dates onto ranked quants data
df_estimated_kpi = pd.merge(
    left=df_estimated_kpi,
    right=df_date_std[['date', 'doc_year', 'std_year']].drop_duplicates(),
    on=['date', 'doc_year'],
    how='left',
    suffixes=('', '_std')
)

# ### Sort data by standardised year
df_estimated_kpi = df_estimated_kpi.sort_values(
    by=['doc_org_std', 'std_year'],
    ascending=[True, False]
).reset_index(drop=True)
"""
A sample of sorted data
`df_estimated_kpi[['doc_org_std', 'quantity description', 'quantity name', 'quantitative value', 'unit or currency of value', 'std_year', 'pagenum', 'doc_name']].head().to_dict('records')`
[{'doc_org_std': 'ABB', 'quantity description': 'Women in Board (percentage)',
  'quantity name': '% of female representation on the board', 'quantitative value': '20%',
  'unit or currency of value': 'percentage', 'std_year': '2021', 'pagenum': 94,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'quantity description': 'NOx from burning gas', 'quantity name': 'Emissions to water',
  'quantitative value': 93, 'unit or currency of value': '-', 'std_year': '2021', 'pagenum': 89,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'quantity description': 'Hazardous waste sent for disposal¹',
  'quantity name': 'Hazardous waste', 'quantitative value': 7, 'unit or currency of value': '-', 'std_year': '2021',
  'pagenum': 89, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'quantity description': 'Scope 2 GHG emissions from electricity (Market-based)',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': 195.0, 'unit or currency of value': 'Kilotons CO2e',
  'std_year': '2021', 'pagenum': 17,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'quantity description': 'Scope 2 GHG emissions from electricity (Location-based)',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': 351.0, 'unit or currency of value': 'Kilotons CO2e',
  'std_year': '2021', 'pagenum': 17,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'}
]
"""

# ## Standardise entity name
"""
Since the quantity names for the estimated KPI might show some variation, 
we can standardise these names by classifying them into one of our KPIs 
"""

# ### Combine all quantity description text
df_estimated_kpi['full_description'] = [
    f"""{row.to_dict()}"""
    for row_num, row in list(df_estimated_kpi[['quantity name', 'quantity description']].iterrows())
]

## define tasks
tasks = [
    bg_async.async_classify_texts(
        texts=df_estimated_kpi['full_description'].unique().tolist(),
        labels=kpis,
        multi_class=0
    )
]
## run tasks
df_name_std = utils.async_utils.run_async_tasks(tasks)
## get output
df_name_std = [resp.get_output() for resp in df_name_std]
## convert to dataframe
df_name_std = [pd.DataFrame(df) for df in df_name_std]
## concatenate all dataframes
df_name_std = pd.concat(df_name_std)
## filter over labels with max score
df_name_std = df_name_std.groupby(
    by=['sequence']
).apply(
    lambda x: x[x['scores'] == x['scores'].max()]
).reset_index(drop=True)
## rename sequence to full_description
df_name_std = df_name_std.rename(
    columns={'sequence': 'full_description',
             'labels': 'kpi',
             'scores': 'score'}
)
## merge df_name_std on df_estimated_kpi
df_estimated_kpi = pd.merge(
    left=df_estimated_kpi,
    right=df_name_std,
    on=['full_description'],
    how='left',
    suffixes=('', '_name_std')
)

# ## Add similarity and recency rank

# ### Add similarity score
"""
This will rank each estimated data row for each company and KPI, 
so the rows with rank 1 will be the most relevant rows for that company and KPI 
"""
df_estimated_kpi['similarity_score'] = df_estimated_kpi.groupby(
    by=['doc_org_std', 'kpi']
)['score'].rank('dense', ascending=False)

# ### Add recency score
"""
This will rank estimated data by year for each company and KPI, 
so the most recent estimated value for a KPI for a given company will have recency rank of 1. 
"""
df_estimated_kpi['recency_score'] = df_estimated_kpi.groupby(
    by=['doc_org_std', 'kpi']
)['std_year'].rank('dense', ascending=False)
"""
A sample of estimated values mapped to KPIs (labels)
df_estimated_kpi[
    ['doc_org_std', 'kpi', 'company name', 'similarity_score', 'recency_score',
     'quantity description', 'quantity name', 'quantitative value',
     'unit or currency of value', 'std_year',
     'pagenum', 'doc_name']
].head(20).to_dict('records')
[{'doc_org_std': 'ABB', 'kpi': '% of female representation on the board', 'company name': 'Sweden',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Women in Board (percentage)',
  'quantity name': '% of female representation on the board', 'quantitative value': '20%',
  'unit or currency of value': 'percentage', 'std_year': 2021, 'pagenum': 94,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'Emissions to water', 'company name': 'LOW-CARBON SOCIETY', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'NOx from burning gas', 'quantity name': 'Emissions to water',
  'quantitative value': '93', 'unit or currency of value': '-', 'std_year': 2021, 'pagenum': 89,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'hazardous waste', 'company name': 'SELECTED', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Hazardous waste sent for disposal¹',
  'quantity name': 'Hazardous waste', 'quantitative value': '7', 'unit or currency of value': '-', 'std_year': 2021,
  'pagenum': 89, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Scope 2 GHG emissions from electricity (Market-based)',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '195.0', 'unit or currency of value': 'Kilotons CO2e',
  'std_year': 2021, 'pagenum': 17,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 3.0,
  'recency_score': 1.0, 'quantity description': 'Scope 2 GHG emissions from electricity (Location-based)',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '351.0', 'unit or currency of value': 'Kilotons CO2e',
  'std_year': 2021, 'pagenum': 17,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 1 emissions', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total Scope 1 and 2 GHG emissions',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '405', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total Scope 1 and 2 GHG emissions',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '405', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 1 emissions', 'company name': nan, 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Fuel and energy-related activities not in Scope 1/2',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '44', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 4.0,
  'recency_score': 1.0, 'quantity description': 'Fuel and energy-related activities not in Scope 1/2',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '44', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 1 emissions', 'company name': 'ABB', 'similarity_score': 3.0,
  'recency_score': 1.0, 'quantity description': 'Identified areas where we can reduce our Scope 1 and 2...',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '80%', 'unit or currency of value': 'percent',
  'std_year': 2021, 'pagenum': 24,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Business travel', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '71', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 4.0,
  'recency_score': 1.0, 'quantity description': 'Employee commuting', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '175', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 7.0,
  'recency_score': 1.0, 'quantity description': 'Up-and downstream leased assets',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '233',
  'unit or currency of value': 'tonnes CO2 equivalent per million $ sales', 'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 3.0,
  'recency_score': 1.0, 'quantity description': 'Use of sold products', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '118,000', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 8.0,
  'recency_score': 1.0, 'quantity description': 'End-of-life treatment of sold products',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '148',
  'unit or currency of value': 'tonnes CO2 equivalent per million $ sales', 'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 6.0,
  'recency_score': 1.0, 'quantity description': 'Investments', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '54', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 5.0,
  'recency_score': 1.0, 'quantity description': 'Volatile organic compounds (VOC)',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '592',
  'unit or currency of value': 'tonnes CO2 equivalent per million $ sales', 'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': 'ABB', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Upstream Scope 3 emissions', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '6400 kilotons', 'unit or currency of value': 'CO2e', 'std_year': 2021, 'pagenum': 18,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'Non-renewable energy consumption', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total energy used',
  'quantity name': 'Non-renewable energy consumption', 'quantitative value': '1,553',
  'unit or currency of value': 'GWh', 'std_year': 2021, 'pagenum': 84,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'Percentage of non-renewable energy production', 'company name': nan,
  'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': '(Total energy used-Total energy from renewable sources) / Total energy used * 100',
  'quantity name': 'Percentage of non-renewable energy production', 'quantitative value': '67.8',
  'unit or currency of value': '%', 'std_year': 2021, 'pagenum': 84,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'}
]
"""


# ## Save ranked data to the cloud

# ### write the data locally first
os.makedirs("/tmp/estimated-values", exist_ok=True)
df_estimated_kpi.to_csv(f"/tmp/estimated-values/df_estimated_kpi.csv", index=False)

# ### read data in bytes
df_estimated_kpi_bytes = utils.common.read_file_contents("/tmp/estimated-values")

# ### upload data
upload_resp = bg_sync.upload_data(
    contents=df_estimated_kpi_bytes['content'].tolist(),
    filenames=df_estimated_kpi_bytes['filename'].tolist(),
)

# ### Check uploaded quants data
df_estimated_kpi_uploaded = upload_resp.get_output()
df_estimated_kpi_uploaded = pd.DataFrame(df_estimated_kpi_uploaded)
estimated_kpi_file_path = df_estimated_kpi_uploaded['href'].tolist()[0]
"""
Now, we can access estimated KPI data from, `estimated_kpi_file_path`
'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_df_estimated_kpicsv/data_type=unstructured/format=csv/variable_desc=uploaded-document/source=stuartcullinan/df_estimated_kpicsv.csv'
"""

# ### Test reading data from quants_ranked_file_path
saved_estimated_kpis = bg_sync.read_file(estimated_kpi_file_path).get_output()
saved_estimated_kpis = pd.DataFrame(saved_estimated_kpis)
"""
Number of rows in saved_estimated_kpis, `len(saved_estimated_kpis)`: 752
Check that saved KPI data has the same number of rows as the data before saving, 
`len(saved_estimated_kpis) == len(df_estimated_kpi)`: True
"""

# ## Check estimated KPI data

# ### set columns to print
cols_to_print = [
    'doc_org_std', 'kpi', 'company name', 'similarity_score', 'recency_score',
    'quantity description', 'quantity name', 'quantitative value',
    'unit or currency of value', 'std_year',
    'pagenum', 'doc_name'
]
"""
Sample of df_estimated_kpi, `df_estimated_kpi[cols_to_print].head(50).to_dict('records')`
[{'doc_org_std': 'ABB', 'kpi': '% of female representation on the board', 'company name': 'Sweden',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Women in Board (percentage)',
  'quantity name': '% of female representation on the board', 'quantitative value': '20%',
  'unit or currency of value': 'percentage', 'std_year': 2021, 'pagenum': 94,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'Emissions to water', 'company name': 'LOW-CARBON SOCIETY', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'NOx from burning gas', 'quantity name': 'Emissions to water',
  'quantitative value': '93', 'unit or currency of value': '-', 'std_year': 2021, 'pagenum': 89,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'hazardous waste', 'company name': 'SELECTED', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Hazardous waste sent for disposal¹',
  'quantity name': 'Hazardous waste', 'quantitative value': '7', 'unit or currency of value': '-', 'std_year': 2021,
  'pagenum': 89, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Scope 2 GHG emissions from electricity (Market-based)',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '195.0', 'unit or currency of value': 'Kilotons CO2e',
  'std_year': 2021, 'pagenum': 17,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 3.0,
  'recency_score': 1.0, 'quantity description': 'Scope 2 GHG emissions from electricity (Location-based)',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '351.0', 'unit or currency of value': 'Kilotons CO2e',
  'std_year': 2021, 'pagenum': 17,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 1 emissions', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total Scope 1 and 2 GHG emissions',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '405', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total Scope 1 and 2 GHG emissions',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '405', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 1 emissions', 'company name': nan, 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Fuel and energy-related activities not in Scope 1/2',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '44', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 2 emissions', 'company name': nan, 'similarity_score': 4.0,
  'recency_score': 1.0, 'quantity description': 'Fuel and energy-related activities not in Scope 1/2',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '44', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 87,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 1 emissions', 'company name': 'ABB', 'similarity_score': 3.0,
  'recency_score': 1.0, 'quantity description': 'Identified areas where we can reduce our Scope 1 and 2...',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '80%', 'unit or currency of value': 'percent',
  'std_year': 2021, 'pagenum': 24,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Business travel', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '71', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 4.0,
  'recency_score': 1.0, 'quantity description': 'Employee commuting', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '175', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 7.0,
  'recency_score': 1.0, 'quantity description': 'Up-and downstream leased assets',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '233',
  'unit or currency of value': 'tonnes CO2 equivalent per million $ sales', 'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 3.0,
  'recency_score': 1.0, 'quantity description': 'Use of sold products', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '118,000', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 8.0,
  'recency_score': 1.0, 'quantity description': 'End-of-life treatment of sold products',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '148',
  'unit or currency of value': 'tonnes CO2 equivalent per million $ sales', 'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 6.0,
  'recency_score': 1.0, 'quantity description': 'Investments', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '54', 'unit or currency of value': 'tonnes CO2 equivalent per million $ sales',
  'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': nan, 'similarity_score': 5.0,
  'recency_score': 1.0, 'quantity description': 'Volatile organic compounds (VOC)',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '592',
  'unit or currency of value': 'tonnes CO2 equivalent per million $ sales', 'std_year': 2021, 'pagenum': 88,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'GHG Scope 3 emissions', 'company name': 'ABB', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Upstream Scope 3 emissions', 'quantity name': 'GHG Scope 3 emissions',
  'quantitative value': '6400 kilotons', 'unit or currency of value': 'CO2e', 'std_year': 2021, 'pagenum': 18,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'Non-renewable energy consumption', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total energy used',
  'quantity name': 'Non-renewable energy consumption', 'quantitative value': '1,553',
  'unit or currency of value': 'GWh', 'std_year': 2021, 'pagenum': 84,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'Percentage of non-renewable energy production', 'company name': nan,
  'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': '(Total energy used-Total energy from renewable sources) / Total energy used * 100',
  'quantity name': 'Percentage of non-renewable energy production', 'quantitative value': '67.8',
  'unit or currency of value': '%', 'std_year': 2021, 'pagenum': 84,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'hazardous waste', 'company name': 'ABB', 'similarity_score': 2.0, 'recency_score': 1.0,
  'quantity description': 'Amount of hazardous waste generated', 'quantity name': 'Hazardous Waste Generated',
  'quantitative value': '11 kilotons', 'unit or currency of value': 'Metric tons', 'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'hazardous waste', 'company name': 'ABB', 'similarity_score': 3.0, 'recency_score': 1.0,
  'quantity description': 'Percentage of hazardous waste that is recycled', 'quantity name': 'Hazardous Waste Recycled',
  'quantitative value': '36%', 'unit or currency of value': '%', 'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': nan, 'company name': 'ABB', 'similarity_score': nan, 'recency_score': nan,
  'quantity description': 'Number of reportable spills', 'quantity name': 'Reportable Spills',
  'quantitative value': '8 spills', 'unit or currency of value': nan, 'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ABB', 'kpi': 'hazardous waste', 'company name': 'ABB', 'similarity_score': 4.0, 'recency_score': 1.0,
  'quantity description': 'Quantity of hazardous substance recovered from reportable spills',
  'quantity name': 'Quantity Recovered from Spills', 'quantitative value': '438 kg', 'unit or currency of value': nan,
  'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'doc_org_std': 'ACCOR', 'kpi': 'GHG Scope 1 emissions', 'company name': 'Accor', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Reduction goal of absolute Scope 1 emissions by 46% by 2030',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '46%', 'unit or currency of value': nan,
  'std_year': 2030, 'pagenum': 66, 'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf'},
 {'doc_org_std': 'ACCOR', 'kpi': 'GHG Scope 2 emissions', 'company name': 'Accor', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Reduction goal of absolute Scope 2 emissions by 46% by 2030',
  'quantity name': 'GHG Scope 2 emissions', 'quantitative value': '46%', 'unit or currency of value': nan,
  'std_year': 2030, 'pagenum': 66, 'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf'},
 {'doc_org_std': 'ACCOR', 'kpi': 'GHG Scope 3 emissions', 'company name': 'Accor', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Reduction goal of absolute Scope 3 emissions by 28% by 2030',
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '28%', 'unit or currency of value': nan,
  'std_year': 2030, 'pagenum': 66, 'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf'},
 {'doc_org_std': 'ACCOR', 'kpi': '% of female representation on the board', 'company name': nan,
  'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': 'Estimated percentage of female representation on the board',
  'quantity name': '% of female representation on the board', 'quantitative value': '76.0',
  'unit or currency of value': 'percentage', 'std_year': 2021, 'pagenum': 75,
  'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf'},
 {'doc_org_std': 'ACCOR', 'kpi': 'Emissions to water', 'company name': 'ACCOR', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Total emissions to water', 'quantity name': 'Emissions to water',
  'quantitative value': '11', 'unit or currency of value': 'Thousand', 'std_year': 2021, 'pagenum': 10,
  'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf'},
 {'doc_org_std': 'AIG', 'kpi': '% of female representation on the board', 'company name': 'Company A',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Proportion of women analysts',
  'quantity name': '% of female representation on the board', 'quantitative value': '67%',
  'unit or currency of value': nan, 'std_year': 2021, 'pagenum': 2,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf'},
 {'doc_org_std': 'AIG', 'kpi': '% of female representation on the board', 'company name': 'Company B',
  'similarity_score': 2.0, 'recency_score': 1.0, 'quantity description': 'Proportion of women summer interns',
  'quantity name': '% of female representation on the board', 'quantitative value': '62%',
  'unit or currency of value': nan, 'std_year': 2021, 'pagenum': 2,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf'},
 {'doc_org_std': 'AIG', 'kpi': 'gender pay gap', 'company name': 'Company H', 'similarity_score': 2.0,
  'recency_score': 1.0, 'quantity description': 'Proportion of women promoted (2020)',
  'quantity name': 'gender pay gap', 'quantitative value': '39.8%', 'unit or currency of value': nan, 'std_year': 2021,
  'pagenum': 2, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf'},
 {'doc_org_std': 'AIG', 'kpi': 'gender pay gap', 'company name': 'Company I', 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Proportion of women promoted (2021)',
  'quantity name': 'gender pay gap', 'quantitative value': '43.8%', 'unit or currency of value': nan, 'std_year': 2021,
  'pagenum': 2, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': '% of female representation on the board', 'company name': 'Company X',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Female representation on the board',
  'quantity name': '% of female representation on the board', 'quantitative value': '43.0',
  'unit or currency of value': '%', 'std_year': 2021, 'pagenum': 107,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': '% of female representation on the board', 'company name': 'Company Y',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Female representation on the board',
  'quantity name': '% of female representation on the board', 'quantitative value': '20.0',
  'unit or currency of value': '%', 'std_year': 2021, 'pagenum': 107,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': '% of female representation on the board', 'company name': 'Aker Carbon',
  'similarity_score': 2.0, 'recency_score': 1.0,
  'quantity description': 'Female (or other gender minority) board members',
  'quantity name': '% of female representation on the board', 'quantitative value': '43.0',
  'unit or currency of value': '%', 'std_year': 2021, 'pagenum': 110,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'Emissions to water', 'company name': 'Aker Carbon Capture',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Emissions to water in tons',
  'quantity name': 'Emissions to water', 'quantitative value': '0.0', 'unit or currency of value': 'tons',
  'std_year': 2021, 'pagenum': 111,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'Non-renewable energy consumption',
  'company name': 'Aker Carbon Capture', 'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': 'Share of non-renewable energy consumption and production¹',
  'quantity name': 'Non-renewable energy consumption', 'quantitative value': '1.44', 'unit or currency of value': '%',
  'std_year': 2021, 'pagenum': 111,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'Percentage of non-renewable energy production',
  'company name': 'Aker Carbon Capture', 'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': 'Share of non-renewable energy consumption and production¹',
  'quantity name': 'Percentage of non-renewable energy', 'quantitative value': '1.44', 'unit or currency of value': '%',
  'std_year': 2021, 'pagenum': 111,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'gender pay gap', 'company name': 'Aker Carbon Capture',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Unadjusted gender pay gap21',
  'quantity name': 'Gender pay gap', 'quantitative value': '0.94', 'unit or currency of value': 'Ratio',
  'std_year': 2021, 'pagenum': 111,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'hazardous waste', 'company name': 'Aker Carbon Capture',
  'similarity_score': 2.0, 'recency_score': 1.0, 'quantity description': 'Hazardous waste in tons',
  'quantity name': 'Hazardous waste', 'quantitative value': '0.002', 'unit or currency of value': 'tons',
  'std_year': 2021, 'pagenum': 111,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'GHG Scope 1 emissions', 'company name': 'Aker Carbon Capture',
  'similarity_score': 1.0, 'recency_score': 1.0, 'quantity description': 'Direct greenhouse gas emissions.',
  'quantity name': 'GHG Scope 1 emissions', 'quantitative value': '0 tCO2e', 'unit or currency of value': 'tCO2e',
  'std_year': 2021, 'pagenum': 37,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'GHG Scope 2 emissions', 'company name': 'Aker Carbon Capture',
  'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': 'Indirect emissions from purchased electricity.', 'quantity name': 'GHG Scope 2 emissions',
  'quantitative value': '3.2 tCO2e', 'unit or currency of value': 'tCO2e', 'std_year': 2021, 'pagenum': 37,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'GHG Scope 3 emissions', 'company name': 'Aker Carbon Capture',
  'similarity_score': 1.0, 'recency_score': 1.0,
  'quantity description': "All other indirect emissions in company's value chain.",
  'quantity name': 'GHG Scope 3 emissions', 'quantitative value': '81.1 tCO2e', 'unit or currency of value': 'tCO2e',
  'std_year': 2021, 'pagenum': 37,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'AKER CARBON CAPTURE', 'kpi': 'hazardous waste', 'company name': nan, 'similarity_score': 1.0,
  'recency_score': 1.0, 'quantity description': 'Tons', 'quantity name': 'hazardous waste',
  'quantitative value': '0.002', 'unit or currency of value': '-', 'std_year': 2021, 'pagenum': 104,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'doc_org_std': 'ARKEMA', 'kpi': '% of female representation on the board', 'company name': 'company name',
  'similarity_score': 2.0, 'recency_score': 1.0, 'quantity description': 'After annual general meeting',
  'quantity name': '% of female representation on the board', 'quantitative value': '45%',
  'unit or currency of value': nan, 'std_year': 2022, 'pagenum': 94,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'doc_org_std': 'ARKEMA', 'kpi': '% of female representation on the board', 'company name': nan,
  'similarity_score': 4.0, 'recency_score': 2.0, 'quantity description': 'Gender balance on the Board of Directors',
  'quantity name': nan, 'quantitative value': '45', 'unit or currency of value': 'Percent', 'std_year': 2021,
  'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'doc_org_std': 'ARKEMA', 'kpi': nan, 'company name': nan, 'similarity_score': nan, 'recency_score': nan,
  'quantity description': 'Diversity-international profiles', 'quantity name': nan, 'quantitative value': '50',
  'unit or currency of value': 'Percent', 'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'doc_org_std': 'ARKEMA', 'kpi': nan, 'company name': nan, 'similarity_score': nan, 'recency_score': nan,
  'quantity description': 'Representation of employees and shareholder employees', 'quantity name': nan,
  'quantitative value': '1', 'unit or currency of value': nan, 'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'doc_org_std': 'ARKEMA', 'kpi': nan, 'company name': nan, 'similarity_score': nan, 'recency_score': nan,
  'quantity description': 'Representation of employees and shareholder employees', 'quantity name': nan,
  'quantitative value': '2', 'unit or currency of value': nan, 'std_year': 2021, 'pagenum': 97,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'}
]
"""

