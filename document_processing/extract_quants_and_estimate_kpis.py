# # Extract quants from pages, and estimate KPI values


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
    ## wait for 30 sec for each file to avoid rate limits
    time.sleep(len(quant_files) * 15)

# ### Get estimated value files
value_estimation_files = [resp.get_output() for resp in value_estimation_responses]
value_estimation_files = [file for file in value_estimation_files if file is not None]
## flatten files
value_estimation_files = [file for files in value_estimation_files for file in files]
"""
Total number of estimated value files, `len(value_estimation_files)`: 
First 5 estimated value files, `value_estimation_files[:5]`
"""

# ### Read estimated value files
tasks = [
    bg_sync.read_files(
        files=value_estimation_files,
        add_file=1
    )
]
df_estimated_kpi = utils.async_utils.run_async_tasks(tasks)
df_estimated_kpi = [resp.get_output() for resp in df_estimated_kpi]
df_estimated_kpi = pd.concat(df_estimated_kpi)
"""
Number of rows in df_estimated_kpi, `len(df_estimated_kpi)`: 
Columns of df_estimated_kpi, `list(df_estimated_kpi.columns)`

First few rows of df_estimated_kpi, `df_estimated_kpi.head().to_dict('records')`

"""

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
df_estimated_kpi = pd.merge(
    left=df_estimated_kpi,
    right=df_filtered_pages.drop_duplicates(),
    on=['doc_name', 'pagenum'],
    how='left',
    suffixes=('_kpi', '_page')
)


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
    by=['kpi', 'rank', 'std_year'],
    ascending=[True, True, False]
).reset_index(drop=True)
"""
A sample of sorted data
`df_estimated_kpi[['kpi', 'score', 'doc_org_std', 'variable description', 'variable', 'value', 'unit', 'std_year', 'relevant quote from text']].head().to_dict('records')`
[
    {'kpi': 'anti-bribery-policies', 'score': 1.0, 'doc_org_std': 'Samsung SDS', 'variable description': 'Disclosure of business principle guidelines, anti-corruption policy, and fair competition policy on the website Disclosure of the status of regular training for compliance and anti-corruption Sharing compliance terms, cases, and countermeasures through Compliance Management System(CPMS) Conducted regular audits on corruption and compliance Initiation of Declaration of Compliance', 'variable': 'Business Ethics', 'value': 'Bribery & Corruption Policy Bribery & Corruption Programs Business Ethics Programs', 'unit': '', 'std_year': '2022', 'relevant quote from text': "[Areas of, Areas of_2, Improvements Made, ['Business Ethics', 'Bribery & Corruption Policy Bribery & Corruption Programs Business Ethics Programs', 'Disclosure of business principle guidelines, anti-corruption policy, and fair competition policy on the website Disclosure of the status of regular training for compliance and anti-corruption Sharing compliance terms, cases, and countermeasures through Compliance Management System(CPMS) Conducted regular audits on corruption and compliance Initiation of Declaration of Compliance']]"}, 
    {'kpi': 'anti-bribery-policies', 'score': 1.0, 'doc_org_std': 'Samsung SDS', 'variable description': 'Operation of 24/7 whistle-blowing channel on the website in the languages of major countries where business is conducted Disclosure of whistle-blowing process Disclosure of the number of reports received through whistle-blowing channels and compliance/corruption guidelines violation cases', 'variable': 'Business Ethics', 'value': 'Whistleblower Programs', 'unit': '', 'std_year': '2022', 'relevant quote from text': "[Areas of, Areas of_2, Improvements Made, ['Business Ethics', 'Whistleblower Programs', 'Operation of 24/7 whistle-blowing channel on the website in the languages of major countries where business is conducted Disclosure of whistle-blowing process Disclosure of the number of reports received through whistle-blowing channels and compliance/corruption guidelines violation cases']]"}, 
    {'kpi': 'anti-bribery-policies', 'score': 1.0, 'doc_org_std': 'ECOLAB', 'variable description': '6 anti-corruption specific audits completed in 2021', 'variable': 'Anti-corruption Specific Audits', 'value': '6', 'unit': '', 'std_year': '2021', 'relevant quote from text': ''}, 
    {'kpi': 'anti-bribery-policies', 'score': 1.0, 'doc_org_std': 'Samsung SDS', 'variable description': 'In 2021, 25,320 employees participated in ethics management trainings.', 'variable': 'Ethics Management Trainings', 'value': '25,320', 'unit': '', 'std_year': '2021', 'relevant quote from text': 'Improved Employee Awareness on Ethics Through the Code of Conduct Guidelines, Samsung SDS discloses the regulation violation cases of suppliers, public funds and assets, working discipline, and information leaks. The company conducts promotional activities and provides ethics management trainings on a regular basis for employees. In 2021, 25,320 employees participated in the training.'}, 
    {'kpi': 'anti-bribery-policies', 'score': 1.0, 'doc_org_std': 'American Express', 'variable description': 'Operations assessed for risks related to corruption', 'variable': 'GRI 205', 'value': '205-1', 'unit': '', 'std_year': '2021', 'relevant quote from text': '2020-2021 ESG Report: Business Ethics pages 70-72'}
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
"""

# ### Test reading data from quants_ranked_file_path
saved_estimated_kpis = bg_sync.read_file(estimated_kpi_file_path).get_output()
saved_estimated_kpis = pd.DataFrame(saved_estimated_kpis)
"""
Number of rows in saved_estimated_kpis, `len(saved_estimated_kpis)`: 7485
Check that saved KPI data has the same number of rows as the data before saving, 
`len(saved_estimated_kpis) == len(df_estimated_kpi)`: True
"""

# ## Check estimated KPI data

# ### Re-arrange columns
cols_to_print = [
    'doc_org_std', 'variable description', 'variable', 'value',
    'date', 'unit', 'pagenum', 'doc_name'
]

"""
df_estimated_kpi.to_dict('recoreds')
"""