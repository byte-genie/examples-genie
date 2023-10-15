# # Structure quants from document pages
"""
In this example, we will use previously filtered document pages (see `document_processing/filter_relevant_pages.py`)
to rank quants by relevance to KPIs of interest. We will do so in the following steps:
- Extract and structure all quant metrics from filtered pages;
- Score extracted quants by relevance to KPIs;
- Merge document meta-data onto estimate KPI values;
- Standardise company names and dates;
- Sort values by standardised company names, score, and dates.
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
                (df_filtered_pages['page_rank'] <= 2)
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

# ## Rank data rows by relevance to KPIs
"""
Now that we have all the quants extracted and structured from the most relevant pages, 
we can do one more layer of ranking to rank quants by relevance to our KPIs.
"""
## identify the relevant quant files for each kpi, so we can rank them according to that kpi
quants_ranking_responses = []
for kpi_num, kpi in enumerate(kpis):
    logger.info(f"Ranking quants for ({kpi_num}/{len(kpis)}): {kpi}")
    tasks = [
        bg_async.async_rank_data(
            attr=kpi,
            files=df_filtered_pages[
                (df_filtered_pages['query'] == kpi) &
                (df_filtered_pages['page_rank'] <= 2)
                ]['file_quants'].unique().tolist(),
            method='llm-ranking',
            non_null_cols=['value'],
            cols_not_use=['context', 'relevant quote from text', 'pagenum', 'doc_name'],
        )
    ]
    quants_ranking_responses_ = utils.async_utils.run_async_tasks(tasks)
    quants_ranking_responses = quants_ranking_responses + quants_ranking_responses_
    # ## wait for some time to avoid rate limit errors
    # time.sleep(2 * 60)
## get ranked quant files
quants_ranking_files = [resp.get_output() for resp in quants_ranking_responses]
quants_ranking_files = [file for file in quants_ranking_files if file is not None]
## flatten quants_ranking_files
quants_ranking_files = [file for files in quants_ranking_files for file in files]
## take unique files
quants_ranking_files = list(set(quants_ranking_files))
"""
Number of ranked quants files, `len(quants_ranking_files)`: 836
First 5 ranked quants files, `quants_ranking_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-116_page-quants_structured-quant-summary_llm-scoring_query-anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf_pagenum-113_page-quants_structured-quant-summary_llm-scoring_query-anti-corruption-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf_pagenum-45_page-quants_structured-quant-summary_llm-scoring_query-hazardous-waste.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf_pagenum-282_page-quants_structured-quant-summary_llm-scoring_query-percentage-of-non-renewable-energy-production.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf_pagenum-113_page-quants_structured-quant-summary_llm-scoring_query-ghg-scope-2-emissions.csv'
]
"""
# ## Read ranked quants data
"""
Let's read a few ranked quants files, to understand the data format
"""
tasks = [
    bg_sync.async_read_files(
        files=quants_ranking_files,
        add_file=1
    )
]
df_quants_ranked = utils.async_utils.run_async_tasks(tasks)
df_quants_ranked = [resp.get_output() for resp in df_quants_ranked]
df_quants_ranked = [pd.DataFrame(df) for df in df_quants_ranked]
df_quants_ranked = pd.concat(df_quants_ranked)
## keep only relevant cols
cols = ['row_id', 'company name', 'category', 'value', 'variable', 'unit', 'date', 'variable description',
        'score', 'relevant quote from text', 'context', 'pagenum', 'doc_name', 'file']
df_quants_ranked = df_quants_ranked[[col for col in df_quants_ranked.columns if col in cols]]
df_quants_ranked = df_quants_ranked.drop_duplicates().reset_index(drop=True)
## get KPI
df_quants_ranked['kpi'] = [
    os.path.splitext(file)[0].split('_query-')[-1] for file in df_quants_ranked['file']
]
## sort data by rank
df_quants_ranked = df_quants_ranked.sort_values(['kpi', 'score'], ascending=False).reset_index(drop=True)
## filter over rows with non-empty values
df_quants_ranked = df_quants_ranked[df_quants_ranked['value'] != ''].reset_index(drop=True)
## set variable, if empty, to 'variable description'
mask = (df_quants_ranked['variable'] == '') & (df_quants_ranked['variable description'] != '')
df_quants_ranked.loc[mask, 'variable'] = df_quants_ranked.loc[mask, 'variable description']
## set 'variable description', if empty, to 'variable'
mask = (df_quants_ranked['variable description'] == '') & (df_quants_ranked['variable'] != '')
df_quants_ranked.loc[mask, 'variable description'] = df_quants_ranked.loc[mask, 'variable']
"""
df_quants_ranked columns, `list(df_quants_ranked.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'file', 'pagenum', 'relevant quote from text', 'row_id', 'score', 'unit', 'value', 'variable', 'variable description']
First few rows of df_quants_ranked, `df_quants_ranked[['kpi', 'score', 'company name', 'variable description', 'variable', 'value', 'relevant quote from text']].head().to_dict('records')`
[
    {'kpi': 'percentage-of-non-renewable-energy-production', 'score': '', 'company name': 'Mondi', 'variable description': "Nine of Mondi's 13 pulp and paper mills fall under the EU Emissions Trading Scheme (EU ETS)...", 'variable': 'GHG regulatory changes', 'value': '25-65', 'relevant quote from text': '5. GHG regulatory changes (net impact), Timeframe: Medium-term'}, 
    {'kpi': 'percentage-of-non-renewable-energy-production', 'score': 1.0, 'company name': '', 'variable description': 'RusHydro supplies at least 25% of the electricity consumed at the Noginsk warehouse, resulting in a significant positive impact on net carbon emissions.', 'variable': '', 'value': 'at least 25%', 'relevant quote from text': 'Hydro Electricity-Noginsk, Moscow'}, 
    {'kpi': 'percentage-of-non-renewable-energy-production', 'score': 1.0, 'company name': '', 'variable description': '', 'variable': 'Intensity ratio, tonnes COe / m²', 'value': '0.056', 'relevant quote from text': 'GHG emissions Scope 1 and 2'}, 
    {'kpi': 'percentage-of-non-renewable-energy-production', 'score': 1.0, 'company name': '', 'variable description': '', 'variable': 'Intensity ratio, tonnes COe / m²', 'value': '0.042', 'relevant quote from text': 'GHG emissions Scope 1 and 2'}, 
    {'kpi': 'percentage-of-non-renewable-energy-production', 'score': 1.0, 'company name': '', 'variable description': '', 'variable': 'Intensity ratio, tonnes COe / m²', 'value': '0.033', 'relevant quote from text': 'GHG emissions Scope 1 and 2'}
]
"""

# ### Add document info to ranked quants from filtered pages dataframe
df_quants_ranked = pd.merge(
    left=df_quants_ranked,
    right=df_filtered_pages[['doc_name', 'doc_org_std', 'doc_year', 'doc_type', 'num_pages']].drop_duplicates(),
    on=['doc_name'],
    how='left',
    suffixes=('_pages', '_quants'),
)

# ### Add rank based on the score

## convert score to float
df_quants_ranked.loc[df_quants_ranked['score'] == '', 'score'] = np.nan
## calculate rank
df_quants_ranked['rank'] = df_quants_ranked.groupby(
    by=['kpi'],
    group_keys=False
)['score'].rank('dense', ascending=False)

"""
First few rows of df_quants_ranked for 'gender-pay-gap', 
`df_quants_ranked[df_quants_ranked['kpi'] == 'gender-pay-gap'][['kpi', 'score', 'rank', 'doc_org_std', 'doc_year', 'company name', 'variable description', 'variable', 'value', 'date']].head().to_dict('records')`
[
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express', 'doc_year': '2021', 'company name': '', 'variable description': 'The mean gender pay gap is 14.7% for the year 2021.', 'variable': 'MEAN', 'value': '14.7%', 'date': ''}, 
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express', 'doc_year': '2021', 'company name': '', 'variable description': 'The median gender pay gap is 16.7% for the year 2021.', 'variable': 'MEDIAN', 'value': '16.7%', 'date': ''}, 
    {'kpi': 'gender-pay-gap', 'score': 0.5, 'rank': 2.0, 'doc_org_std': 'American Express', 'doc_year': '2021', 'company name': '', 'variable description': '97.5% of women are receiving a bonus.', 'variable': 'WOMEN', 'value': '97.5%', 'date': ''}, 
    {'kpi': 'gender-pay-gap', 'score': 0.5, 'rank': 2.0, 'doc_org_std': 'American Express', 'doc_year': '2021', 'company name': '', 'variable description': '98.6% of men are receiving a bonus.', 'variable': 'MEN', 'value': '98.6%', 'date': ''}, 
    {'kpi': 'gender-pay-gap', 'score': 0.5, 'rank': 2.0, 'doc_org_std': 'American Express', 'doc_year': '2021', 'company name': '', 'variable description': 'The mean bonus pay gap is 43.7% for the year 2021.', 'variable': 'MEAN', 'value': '43.7%', 'date': ''}
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
mask = df_quants_ranked['date'].isin([''])
df_quants_ranked.loc[mask, 'date'] = df_quants_ranked.loc[mask, 'doc_year']

# ### Extract standardised 4-digit years from dates
date_std_resp = bg_async.standardise_years(
    data=df_quants_ranked[['date', 'doc_year']].drop_duplicates().to_dict('records'),
    time_cols=['date', 'doc_year'],
)
df_date_std = date_std_resp.get_output()
df_date_std = pd.DataFrame(df_date_std)

## Merge standardised dates onto ranked quants data
df_quants_ranked = pd.merge(
    left=df_quants_ranked,
    right=df_date_std[['date', 'doc_year', 'std_year']].drop_duplicates(),
    on=['date', 'doc_year'],
    how='left',
    suffixes=('', '_std')
)

# ### Sort data by standardised year
df_quants_ranked = df_quants_ranked.sort_values(
    by=['kpi', 'rank', 'std_year'],
    ascending=[True, True, False]
).reset_index(drop=True)
"""
A sample of sorted data
`df_quants_ranked[['kpi', 'score', 'doc_org_std', 'variable description', 'variable', 'value', 'unit', 'std_year', 'relevant quote from text']].head().to_dict('records')`
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
os.makedirs("/tmp/ranked-quants", exist_ok=True)
df_quants_ranked.to_csv(f"/tmp/ranked-quants/df_quants_ranked.csv", index=False)

# ### read data in bytes
df_quants_ranked_bytes = utils.common.read_file_contents("/tmp/ranked-quants")

# ### upload data
upload_resp = bg_sync.upload_data(
    contents=df_quants_ranked_bytes['content'].tolist(),
    filenames=df_quants_ranked_bytes['filename'].tolist(),
)

# ### Check uploaded quants data
df_quants_ranked_uploaded = upload_resp.get_output()
df_quants_ranked_uploaded = pd.DataFrame(df_quants_ranked_uploaded)
quants_ranked_file_path = df_quants_ranked_uploaded['href'].tolist()[0]
"""
Now, we can access ranked quants data from, `quants_ranked_file_path`
'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_df_quants_rankedcsv/data_type=unstructured/format=csv/variable_desc=uploaded-document/source=stuartcullinan/df_quants_rankedcsv.csv'
"""

# ### Test reading data from quants_ranked_file_path
saved_ranked_quants = bg_sync.read_file(quants_ranked_file_path).get_output()
saved_ranked_quants = pd.DataFrame(saved_ranked_quants)
"""
Number of rows in saved_ranked_quants, `len(saved_ranked_quants)`: 7485
Check that saved quants data has the same number of rows as the data before saving, 
`len(saved_ranked_quants) == len(df_quants_ranked)`: True
"""

# ## Check the first 5 rows for each company and KPI

## set columns to print
cols_to_print = [
    'kpi', 'score', 'rank', 'doc_org_std', 'variable description', 'variable', 'value', 'unit', 'std_year',
    'relevant quote from text', 'pagenum', 'doc_name',
]

# ### KPI = 'emissions-to-water'
kpi_mask = (df_quants_ranked['kpi'] == 'emissions-to-water')
score_mask = (df_quants_ranked['score'] > df_quants_ranked[kpi_mask]['score'].quantile(0.9))
df_kpi = df_quants_ranked[kpi_mask & score_mask].groupby(
    by=['doc_org_std'],
    group_keys=False,
).apply(
    lambda x: x[cols_to_print].head(5)
).reset_index(drop=True).to_dict('records')
"""
`df_kpi` for 'emissions-to-water'
[{'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ABB',
  'variable description': "'Emissions to air'", 'variable': "'Emissions to air'", 'value': '-', 'unit': '',
  'std_year': '2021', 'relevant quote from text': "GRI ref.: ['306-3', 'Emissions to air', nan, '-', '-', 6.0]",
  'pagenum': 89, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AKER CARBON CAPTURE',
  'variable description': 'Activities negatively affecting biodiversity-sensitive areas in number',
  'variable': 'Number', 'value': '0', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['Unit', '2021', 'nan', ['Greenhouse gas emissions18', 'tCO2e', '37.1'], ['Carbon footprint', nan, 'Not relevant for Aker Carbon Capture'], ['GHG intensity of investee companies', nan, 'Not relevant for Aker Carbon Capture'], ['Share of investments in companies active in the fossil fuel sector', nan, 'Not relevant for Aker Carbon Capture'], ['Share of non-renewable energy consumption and production¹', '%', '1.44'], ['Energy consumption intensity per high impact climate sector20', 'Ratio', '0.007'], ['Activities negatively affecting biodiversity-sensitive areas', 'Number', '0'], ['Emissions to water', 'tons', '0'], ['Hazardous waste', 'tons', '0.002'], ['Violations of UNGC principles and OECD Guidelines for Multinational Enterprises', 'Number', '0'], ['Lack of processes and compliance mechanisms to monitor compliance with UNGC principles and OECD Guidelines for Multinational Enterprises', 'Number', '0'], ['Unadjusted gender pay gap21', 'Ratio', '0.94'], ['Board gender diversity, female representation', '%', '43%'], ['Exposure to controversial weapons', nan, nan]]",
  'pagenum': 111, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 0.8, 'rank': 2.0, 'doc_org_std': 'AKER CARBON CAPTURE',
  'variable description': 'Emissions to water in tons', 'variable': 'tons', 'value': '0', 'unit': '',
  'std_year': '2021',
  'relevant quote from text': "['Unit', '2021', 'nan', ['Greenhouse gas emissions18', 'tCO2e', '37.1'], ['Carbon footprint', nan, 'Not relevant for Aker Carbon Capture'], ['GHG intensity of investee companies', nan, 'Not relevant for Aker Carbon Capture'], ['Share of investments in companies active in the fossil fuel sector', nan, 'Not relevant for Aker Carbon Capture'], ['Share of non-renewable energy consumption and production¹', '%', '1.44'], ['Energy consumption intensity per high impact climate sector20', 'Ratio', '0.007'], ['Activities negatively affecting biodiversity-sensitive areas', 'Number', '0'], ['Emissions to water', 'tons', '0'], ['Hazardous waste', 'tons', '0.002'], ['Violations of UNGC principles and OECD Guidelines for Multinational Enterprises', 'Number', '0'], ['Lack of processes and compliance mechanisms to monitor compliance with UNGC principles and OECD Guidelines for Multinational Enterprises', 'Number', '0'], ['Unadjusted gender pay gap21', 'Ratio', '0.94'], ['Board gender diversity, female representation', '%', '43%'], ['Exposure to controversial weapons', nan, nan]]",
  'pagenum': 111, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Chemical oxygen demand (COD)', 'variable': 'COD', 'value': '1,740', 'unit': 'tO2',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 196,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Chemical oxygen demand (COD)', 'variable': 'COD', 'value': '1,640', 'unit': 'tO2',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 196,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Chemical oxygen demand (COD)', 'variable': 'COD', 'value': '1,950', 'unit': 'tO2',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 196,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Suspended solids', 'variable': 'Suspended solids', 'value': '465', 'unit': 't',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 196,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Suspended solids', 'variable': 'Suspended solids', 'value': '500', 'unit': 't',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 196,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited',
  'variable description': 'Increased water consumption by 4%, but avoided 2970 million liters of fresh water by utilizing desalinated sea water and other sources',
  'variable': 'Water Consumption Reductions', 'value': 2970.0, 'unit': 'million liters', 'std_year': '2021',
  'relevant quote from text': '2,970 million liter of fresh water has been avoided by withdrawing desalinated sea water, by utilizing other related treated water and Rain water harvesting during FY 2020-21.',
  'pagenum': 269,
  'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
  'variable description': '2 million cubic meters of water were discharged into the natural environment in 2020',
  'variable': 'Cubic meters of water', 'value': '2 million', 'unit': '', 'std_year': '2020',
  'relevant quote from text': 'Releases into water and the soil Biomass facilities', 'pagenum': 65,
  'doc_name': 'userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
  'variable description': '8.6 million cubic meters of water were consumed in 2020',
  'variable': 'Cubic meters of water', 'value': '8.6 million', 'unit': '', 'std_year': '2020',
  'relevant quote from text': 'Releases into water and the soil Biomass facilities', 'pagenum': 65,
  'doc_name': 'userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
  'variable description': 'Water consumption', 'variable': 'GRI 303', 'value': '303-5', 'unit': '', 'std_year': '2021',
  'relevant quote from text': '2020-2021 ESG Report: Environmental Performance Data Summary page 63', 'pagenum': 83,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_srpdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Bayer',
  'variable description': 'Percentage of industrial and mixed wastewater purified in wastewater treatment plants',
  'variable': 'Wastewater', 'value': '79.6%', 'unit': '', 'std_year': '2021',
  'relevant quote from text': 'text-segments', 'pagenum': 96,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_26_bayer_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 0.8, 'rank': 2.0, 'doc_org_std': 'Bayer',
  'variable description': 'Total volume of water recycled', 'variable': 'Water Recycled', 'value': '376 million',
  'unit': 'Million m³', 'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 96,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_26_bayer_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'BillerudKorsnäs',
  'variable description': 'Emissions to water (303-4)-Process water', 'variable': 'Process water', 'value': '135',
  'unit': 'million m³', 'std_year': '2021', 'relevant quote from text': "[2021.0, 135, 'million m³']", 'pagenum': 112,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'BillerudKorsnäs',
  'variable description': 'Emissions to water (303-4)-COD', 'variable': 'COD', 'value': '27,156', 'unit': 'tonnes',
  'std_year': '2021', 'relevant quote from text': "[2021.0, 27,156, 'tonnes']", 'pagenum': 112,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'BillerudKorsnäs',
  'variable description': 'Emissions to water (303-4)-TSS', 'variable': 'TSS', 'value': '3,830', 'unit': 'tonnes',
  'std_year': '2021', 'relevant quote from text': "[2021.0, 3,830, 'tonnes']", 'pagenum': 112,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'BillerudKorsnäs',
  'variable description': 'Emissions to water (303-4)-Organically bound chlorine',
  'variable': 'Organically bound chlorine', 'value': '131', 'unit': 'tonnes', 'std_year': '2021',
  'relevant quote from text': "[2021.0, 131, 'tonnes']", 'pagenum': 112,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'BillerudKorsnäs',
  'variable description': 'Emissions to water (303-4)-Nitrogen (N)', 'variable': 'Nitrogen (N)', 'value': '434',
  'unit': 'tonnes', 'std_year': '2021', 'relevant quote from text': "[2021.0, 434, 'tonnes']", 'pagenum': 112,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Total water consumption', 'variable': 'Water-Abs', 'value': '386', 'unit': 'm³',
  'std_year': '2021', 'relevant quote from text': 'tablenum-1', 'pagenum': 9,
  'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Total water consumption', 'variable': 'Water-Abs', 'value': '254', 'unit': 'm³',
  'std_year': '2021', 'relevant quote from text': 'tablenum-1', 'pagenum': 9,
  'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Like-for-like water consumption', 'variable': 'Water-LFL', 'value': '386', 'unit': 'm³',
  'std_year': '2021', 'relevant quote from text': 'tablenum-1', 'pagenum': 9,
  'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Like-for-like water consumption', 'variable': 'Water-LFL', 'value': '254', 'unit': 'm³',
  'std_year': '2021', 'relevant quote from text': 'tablenum-1', 'pagenum': 9,
  'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Ledlenser',
  'variable description': 'Reduction in metals to air compared to previous year', 'variable': 'Metal Intensity',
  'value': '<41', 'unit': '', 'std_year': '2021',
  'relevant quote from text': 'The intensity of metals to air shall be reduced compared to the previous year.',
  'pagenum': 12, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_22_boliden_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Ledlenser',
  'variable description': 'Reduction in metals to water compared to previous year', 'variable': 'Metal Intensity',
  'value': '<25', 'unit': '', 'std_year': '2021',
  'relevant quote from text': 'The intensity of metals to water shall be reduced compared to the previous year.',
  'pagenum': 12, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_22_boliden_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing',
  'variable description': 'Organic chemicals from waste streams from the pulp production process are extracted early on in the biorefinery process at the Lenzing site (Austria) which significantly reduces the chemical oxygen demand (COD) of effluent water. This is one example of best practices where potential waste streams are converted into useful products, thereby avoiding pollution and reducing the amount of waste to be treated at the wastewater treatment plant.',
  'variable': 'Specific', 'value': '100 %', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['2014.0', '2019.0', '2020.0', '2021.0', 'nan', ['COD', '100 %', '86.2%', '99.6%', '91.7%'], ['SO4', '100 %', '87.5%', '112.6%', '104.0 %'], ['Amines', '100 %', '104.4%', '130.1%', '123.3 %']]",
  'pagenum': 86, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing', 'variable description': 'Absolute',
  'variable': 'Absolute', 'value': '6,110', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['2014.0', '2019.0', '2020.0', '2021.0', 'nan', ['COD', '6,110', '5,286', '5,510', '5,666'], ['SO4', '173,648', '152,519', '177,003', '182,576'], ['Amines', '198', '208', '233', '247']]",
  'pagenum': 86, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing', 'variable description': 'Specific',
  'variable': 'Specific', 'value': '100 %', 'unit': '', 'std_year': '2021', 'relevant quote from text': '',
  'pagenum': 86, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing', 'variable description': 'Absolute',
  'variable': 'Absolute', 'value': '173,648', 'unit': '', 'std_year': '2021', 'relevant quote from text': '',
  'pagenum': 86, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing', 'variable description': 'Specific',
  'variable': 'Specific', 'value': '100 %', 'unit': '', 'std_year': '2021', 'relevant quote from text': '',
  'pagenum': 86, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
  'variable description': "Mondi's emissions to water included 34,141 tonnes of Chemical Oxygen Demand (COD).",
  'variable': 'COD', 'value': '34,141 tonnes', 'unit': 'tonnes', 'std_year': '2021',
  'relevant quote from text': 'SASB Emissions to water 34,141 tonnes COD', 'pagenum': 75,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
  'variable description': "Mondi's emissions to water included 109 tonnes of Adsorbable Organic Halogens (AOX).",
  'variable': 'AOX', 'value': '109 tonnes', 'unit': 'tonnes', 'std_year': '2021',
  'relevant quote from text': 'SASB Emissions to water 109 tonnes AOX', 'pagenum': 75,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
  'variable description': "Mondi's emissions to air included 4.4 million tonnes of Carbon Dioxide Equivalent (CO2e).",
  'variable': 'CO2e', 'value': '4.4 million tonnes', 'unit': 'tonnes', 'std_year': '2021',
  'relevant quote from text': 'SASB Emissions to air 4.4 million tonnes CO2e', 'pagenum': 75,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
  'variable description': 'Reduce specific effluent load to the environment by 5%',
  'variable': 'Reduction in effluent load to the environment (measure COD)', 'value': '5%', 'unit': '',
  'std_year': '2021',
  'relevant quote from text': 'Environmental Performance Commitment: Reduce specific effluent load to the environment (measure COD) by 5% by 2025 compared to 2020 baseline',
  'pagenum': 76, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
  'variable description': 'Water crisis (automation, robotics, Al)', 'variable': 'No.', 'value': 3.0, 'unit': '',
  'std_year': '2020', 'relevant quote from text': "[3.0, 'Water crisis', '(automation, robotics, Al)']", 'pagenum': 28,
  'doc_name': 'userid_stuartcullinan_uploadfilename_13_capita_mrpdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG', 'variable description': 'ESG Rating',
  'variable': 'ESG Rating', 'value': '382', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['ESG Rating', nan, '382', nan, nan, nan, '401', '382', nan]", 'pagenum': 10,
  'doc_name': 'userid_stuartcullinan_uploadfilename_3_cgcpdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG', 'variable description': 'ESG Rating',
  'variable': 'ESG Rating', 'value': '228', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['ESG Rating', '228', nan, nan, nan, nan, nan, nan, '202']", 'pagenum': 10,
  'doc_name': 'userid_stuartcullinan_uploadfilename_3_cgcpdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG', 'variable description': 'ESG Rating',
  'variable': 'ESG Rating', 'value': '63', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['ESG Rating', '63', '00-44', '33-30', nan, '40-44 46-49', nan, '36-30', '50']",
  'pagenum': 10, 'doc_name': 'userid_stuartcullinan_uploadfilename_3_cgcpdf'},
 {'kpi': 'emissions-to-water', 'score': 0.7, 'rank': 3.0, 'doc_org_std': 'Samsung SDS',
  'variable description': '2021 Key Figures Digital Technology for ESG Sustainability Impacts Materiality Analysis',
  'variable': 'Digital Technology', 'value': 'ESG Sustainability Impacts Materiality Analysis', 'unit': '',
  'std_year': '2022',
  'relevant quote from text': "['Our Business', 'Commitments', '2021 Key Figures Digital Technology for ESG Sustainability Impacts Materiality Analysis']",
  'pagenum': 20, 'doc_name': 'userid_stuartcullinan_uploadfilename_16_samsung_sdspdf'},
 {'kpi': 'emissions-to-water', 'score': 0.6, 'rank': 4.0, 'doc_org_std': 'THE a2 MILK COMPANY LIMITED',
  'variable description': 'Total water usage', 'variable': 'Water Usage', 'value': '28,361', 'unit': "'000 litres",
  'std_year': '2021',
  'relevant quote from text': '[\'Metric\', \'FY21\', \'FY20\', \'FY19\', [\'Smeaton Grange\', \'Smeaton Grange\', \'Smeaton Grange\', \'Smeaton Grange\'], ["Total water usage (\'000 litres)", \'28,361\', \'27,662\', \'24,744\'], [\'Water efficiency (litres/litre of milk)\', \'0.6\', \'0.7\', \'0.5\'], [\'Waste water diverted to beneficial land application (litres)\', \'813,600\', \'919,900\', \'516,500\'], [\'Waste produced (tonnes)\', \'28.0\', \'28.9\', \'25.6\'], [\'Waste diversion\', \'96.9%\', \'97.1%\', \'95.4%\'], [\'Energy consumption (kWh)\', \'1.8m\', \'1.7m\', \'1.7m\'], [nan, nan, nan, nan]]',
  'pagenum': 15, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_07_a2-milk-company_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to water', 'variable': 'Process Wastewater', 'value': '190m m³', 'unit': '',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 49,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to water', 'variable': 'Cooling Water', 'value': '190m m³', 'unit': '',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 49,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to water', 'variable': 'Biological Oxygen Demand (7days)', 'value': '7,300 t',
  'unit': '', 'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 49,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to water', 'variable': 'Chemical Oxygen Demand', 'value': '59,000 t', 'unit': '',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 49,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to water', 'variable': 'Absorbable Organic Halogens', 'value': '280 t', 'unit': '',
  'std_year': '2021', 'relevant quote from text': 'tablenum-0', 'pagenum': 49,
  'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf'},
 {'kpi': 'emissions-to-water', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'adesso SE',
  'variable description': 'Water consumption per square meter in 2021',
  'variable': 'Water consumption per square meter in 2021', 'value': '0.18', 'unit': 'm³/m²', 'std_year': '2021',
  'relevant quote from text': '', 'pagenum': 41, 'doc_name': 'userid_stuartcullinan_uploadfilename_1_adesso_sepdfpdf'}
]
"""

# ### KPI = 'gender-pay-gap'
kpi_mask = (df_quants_ranked['kpi'] == 'gender-pay-gap')
score_mask = (df_quants_ranked['score'] >= 0.75)
df_kpi = df_quants_ranked[kpi_mask & score_mask].groupby(
    by=['doc_org_std'],
    group_keys=False,
).apply(
    lambda x: x[cols_to_print].head(5)
).reset_index(drop=True).to_dict('records')
"""
`df_kpi` for 'gender-pay-gap'
[
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AIG',
     'variable description': 'The mean average salary pay gap for 2021', 'variable': 'Salary Pay Gap', 'value': '2021',
     'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Mean; 2021', 'Mean; 20201', 'Mean; 20191', 'Mean; 20182', 'Mean; 20172']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AIG',
     'variable description': 'The percentage of hourly fixed pay at 23%', 'variable': 'Hourly fixed pay',
     'value': '23%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Hourly fixed pay', '23%', '26%', '25%', '27%', '29%', '31%', '32%', '23%', '32%', '34%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AIG',
     'variable description': 'The percentage of hourly fixed pay at 26%', 'variable': 'Hourly fixed pay',
     'value': '26%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Hourly fixed pay', '23%', '26%', '25%', '27%', '29%', '31%', '32%', '23%', '32%', '34%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AIG',
     'variable description': 'The percentage of hourly fixed pay at 25%', 'variable': 'Hourly fixed pay',
     'value': '25%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Hourly fixed pay', '23%', '26%', '25%', '27%', '29%', '31%', '32%', '23%', '32%', '34%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AIG',
     'variable description': 'The percentage of hourly fixed pay at 27%', 'variable': 'Hourly fixed pay',
     'value': '27%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Hourly fixed pay', '23%', '26%', '25%', '27%', '29%', '31%', '32%', '23%', '32%', '34%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AKER CARBON CAPTURE',
     'variable description': 'Equal split between male and female employees',
     'variable': 'Equal split between male and female employees', 'value': '50/50', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Current Operations of Aker Carbon Capture'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
     'variable description': 'Women in the total headcount', 'variable': 'Diversity', 'value': '26.2', 'unit': '%',
     'std_year': '2021', 'relevant quote from text': "['Women in the total headcount', '%', '26.2', '25.6', '25.3']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
     'variable description': "Proportion of women on Arkema's payroll at 31 Dec 2021",
     'variable': "Proportion of women on Arkema's payroll at 31 Dec 2021", 'value': '26.2%', 'unit': '',
     'std_year': '2021', 'relevant quote from text': 'ARKE/MA UNIVERSAL REGISTRATION DOCUMENT 2021'},
    {'kpi': 'gender-pay-gap', 'score': 0.8, 'rank': 5.0, 'doc_org_std': 'Admiral Group plc',
     'variable description': '49% Male (2020: 53% Female, 47% Male)', 'variable': 'Gender split across the Group',
     'value': '51% Female,', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Gender split across the Group 51% Female, 49% Male (2020: 53% Female, 47% Male)'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'Aggreko plc',
     'variable description': 'Number of males in Board', 'variable': 'No.', 'value': '7', 'unit': '',
     'std_year': '2020', 'relevant quote from text': 'Gender of Board No. Male 7 Female 4'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'Aggreko plc',
     'variable description': 'Number of females in Board', 'variable': 'No.', 'value': '4', 'unit': '',
     'std_year': '2020', 'relevant quote from text': 'Gender of Board No. Male 7 Female 4'},
    {'kpi': 'gender-pay-gap', 'score': 0.8, 'rank': 5.0, 'doc_org_std': 'Aggreko plc',
     'variable description': 'Number of males in Executive Committee', 'variable': 'No.', 'value': '7', 'unit': '',
     'std_year': '2020', 'relevant quote from text': 'Gender of Executive Committee No. Male 7 Female 1'},
    {'kpi': 'gender-pay-gap', 'score': 0.8, 'rank': 5.0, 'doc_org_std': 'Aggreko plc',
     'variable description': 'Number of females in Executive Committee', 'variable': 'No.', 'value': '1', 'unit': '',
     'std_year': '2020', 'relevant quote from text': 'Gender of Executive Committee No. Male 7 Female 1'},
    {'kpi': 'gender-pay-gap', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Air New Zealand',
     'variable description': 'The average pay differential between male and female employees is 1.69%.',
     'variable': 'Average Pay Differential', 'value': '1.69%', 'unit': '%', 'std_year': '2021',
     'relevant quote from text': 'Overall our males are paid 1.69% more than females'},
    {'kpi': 'gender-pay-gap', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Air New Zealand',
     'variable description': 'The number of female employees in Air New Zealand is 32.',
     'variable': 'The number of female employees in Air New Zealand is 32.', 'value': '32', 'unit': '',
     'std_year': '2021', 'relevant quote from text': ''},
    {'kpi': 'gender-pay-gap', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Air New Zealand',
     'variable description': 'The number of male employees in Air New Zealand is 112.',
     'variable': 'The number of male employees in Air New Zealand is 112.', 'value': '112', 'unit': '',
     'std_year': '2021', 'relevant quote from text': ''},
    {'kpi': 'gender-pay-gap', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Air New Zealand',
     'variable description': 'The average pay differential for females compared to males in Air New Zealand is 3.24%.',
     'variable': 'The average pay differential for females compared to males in Air New Zealand is 3.24%.',
     'value': '3.24%', 'unit': '%', 'std_year': '2021',
     'relevant quote from text': 'Number of Females Number of Males Average Pay Differential'},
    {'kpi': 'gender-pay-gap', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Air New Zealand',
     'variable description': 'The average pay differential for females compared to males in Air New Zealand is 2.53%.',
     'variable': 'The average pay differential for females compared to males in Air New Zealand is 2.53%.',
     'value': '2.53%', 'unit': '%', 'std_year': '2021',
     'relevant quote from text': 'Number of Females Number of Males Average Pay Differential'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
     'variable description': 'Percentage of female employees', 'variable': 'Percentage of female employees',
     'value': '19%', 'unit': '', 'std_year': '2020', 'relevant quote from text': '...'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
     'variable description': 'Percentage of female executives', 'variable': 'Women as a percentage of executives',
     'value': '22%', 'unit': '', 'std_year': '2020', 'relevant quote from text': '...'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
     'variable description': 'Percentage of female Directors', 'variable': 'Percentage of female Directors',
     'value': '38%', 'unit': '', 'std_year': '2020', 'relevant quote from text': '...'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
     'variable description': 'Percentage of newly recruited employees who are female',
     'variable': 'Women as a percentage of newly recruited employees', 'value': '33%', 'unit': '', 'std_year': '2020',
     'relevant quote from text': '...'}, 
     {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
                                          'variable description': 'Percentage of employees on permanent contracts',
                                          'variable': 'Permanent employment contract', 'value': '92.1%', 'unit': '',
                                          'std_year': '2020', 'relevant quote from text': '...'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
     'variable description': 'Mean gender pay gap for AMEX Services Europe Ltd AESEL', 'variable': 'MEAN GENDER PAY',
     'value': '14%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['AMEX SERVICES EUROPE LTD AESEL', 'MEAN GENDER PAY', '14%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
     'variable description': 'Median gender pay gap for AMEX Services Europe Ltd AESEL',
     'variable': 'MEDIAN GENDER PAY', 'value': '15.8%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['AMEX SERVICES EUROPE LTD AESEL', 'MEDIAN GENDER PAY', '15.8%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
     'variable description': 'Mean bonus pay gap for AMEX Services Europe Ltd AESEL', 'variable': 'MEAN BONUS PAY GAP',
     'value': '45%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['AMEX SERVICES EUROPE LTD AESEL', 'MEAN BONUS PAY GAP', '45%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
     'variable description': 'Median bonus pay gap for AMEX Services Europe Ltd AESEL', 'variable': 'MEDIAN BONUS PAY',
     'value': '45.7%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['AMEX SERVICES EUROPE LTD AESEL', 'MEDIAN BONUS PAY', '45.7%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
     'variable description': 'Proportion of women employees receiving bonus for AMEX Services Europe Ltd AESEL',
     'variable': 'PROPORTION', 'value': '97.3%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['AMEX SERVICES EUROPE LTD AESEL', 'PROPORTION', '97.3%']"},
    {'kpi': 'gender-pay-gap', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'American International Group, Inc.',
     'variable description': "AIG increased the AIG Women's Open prize money by $1.3 million",
     'variable': 'Increase in prize money', 'value': '$1.3 million', 'unit': 'USD', 'std_year': '2021',
     'relevant quote from text': 'Impact at Glance, AIG 2021 ESG REPORT'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'American International Group, Inc.',
     'variable description': 'AIG achieved a 0.6 percentage point increase in global gender representation',
     'variable': 'Increase in global gender representation across all employee categories',
     'value': '0.6 percentage points', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Impact at Glance, AIG 2021 ESG REPORT'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'American International Group, Inc.',
     'variable description': '52% of job placements at AIG globally were female', 'variable': 'Job placements globally',
     'value': '52%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Impact at Glance, AIG 2021 ESG REPORT'},
    {'kpi': 'gender-pay-gap', 'score': 0.8, 'rank': 5.0, 'doc_org_std': 'American International Group, Inc.',
     'variable description': 'AIG draws upon internal processes and monitoring, regularly conducted internal reviews and external benchmarking studies to identify and address any gender, race or ethnicity-related pay gap issues. AIG actively monitors changes in laws and regulations regarding pay transparency with the intent to fully comply with applicable legal requirements.',
     'variable': 'Gender Pay Gap', 'value': '81', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Review and Accountability, AIG also provides our colleagues with ample resources to report or discuss concerns related to compensation and pay equity. These include our Employee Relations group, Human Resources department and Compliance Helpline. To encourage meaningful reporting, we give our colleagues the option of maintaining anonymity throughout the reporting process.'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Arch Insurance Group Inc.',
     'variable description': 'Reduction in mean gender pay gap compared to 2021', 'variable': 'Mean', 'value': '-2.03%',
     'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Our mean gender pay gap, or the difference in mean pay between male and female colleagues, reduced by 2.03% compared to 2021'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva',
     'variable description': 'The mean gender pay gap at Aviva in April 2021 was 25.9%.', 'variable': 'Mean',
     'value': '25.9%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Mean', '25.9%', '26.0%', '26.7%', '27.2%', '28.5%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva',
     'variable description': 'The median gender pay gap at Aviva in April 2021 was 25.8%.', 'variable': 'Median',
     'value': '25.8%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Median', '25.8%', '26.7%', '27.3%', '27.8%', '27.6%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva',
     'variable description': 'The mean gender pay gap in April 2020 was 26.0%.', 'variable': 'Mean', 'value': '26.0%',
     'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Mean', '25.9%', '26.0%', '26.7%', '27.2%', '28.5%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva',
     'variable description': 'The median gender pay gap in April 2020 was 26.7%.', 'variable': 'Median',
     'value': '26.7%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Median', '25.8%', '26.7%', '27.3%', '27.8%', '27.6%']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva',
     'variable description': 'The mean gender pay gap in April 2019 was 26.7%.', 'variable': 'Mean', 'value': '26.7%',
     'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Mean', '25.9%', '26.0%', '26.7%', '27.2%', '28.5%']"},
    {'kpi': 'gender-pay-gap', 'score': 0.75, 'rank': 6.0, 'doc_org_std': 'Aviva plc',
     'variable description': 'Board membership, Male: 7 (58.3%)', 'variable': 'Male', 'value': '7', 'unit': '',
     'std_year': '2021', 'relevant quote from text': 'tablenum-0'},
    {'kpi': 'gender-pay-gap', 'score': 0.75, 'rank': 6.0, 'doc_org_std': 'Aviva plc',
     'variable description': 'Board membership, Female: 5 (41.7%)', 'variable': 'Female', 'value': '5', 'unit': '',
     'std_year': '2021', 'relevant quote from text': 'tablenum-0'},
    {'kpi': 'gender-pay-gap', 'score': 0.975, 'rank': 2.0, 'doc_org_std': 'BillerudKorsnäs',
     'variable description': 'Employee engagement score from the Group-wide employee survey',
     'variable': 'Employee Engagement Score', 'value': '77', 'unit': '', 'std_year': '2021',
     'relevant quote from text': '"Employee survey 2021 During the year the Group-wide employee survey was carried out, with the results showing continued stable engagement 77 (78) and sustainable leadership 78 (77). The frequency high, response was at 82%. When following up the results of this year\'s employee survey, improvement and development areas were identified and action plans were drawn up.'},
    {'kpi': 'gender-pay-gap', 'score': 0.975, 'rank': 2.0, 'doc_org_std': 'BillerudKorsnäs',
     'variable description': 'Sustainable leadership score from the Group-wide employee survey',
     'variable': 'Sustainable Leadership Score', 'value': '78', 'unit': '', 'std_year': '2021',
     'relevant quote from text': '"Employee survey 2021 During the year the Group-wide employee survey was carried out, with the results showing continued stable engagement 77 (78) and sustainable leadership 78 (77). The frequency high, response was at 82%. When following up the results of this year\'s employee survey, improvement and development areas were identified and action plans were drawn up.'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'BillerudKorsnäs',
     'variable description': 'Percentage of employees who responded to the Group-wide employee survey',
     'variable': 'Response Rate', 'value': '82%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': '"Employee survey 2021 During the year the Group-wide employee survey was carried out, with the results showing continued stable engagement 77 (78) and sustainable leadership 78 (77). The frequency high, response was at 82%. When following up the results of this year\'s employee survey, improvement and development areas were identified and action plans were drawn up.'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
     'variable description': '% of total employees who received regular performance and career development reviews during the reporting period',
     'variable': '% of total workforce', 'value': 'na', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Employee performance appraisals', 'Emp-Dev', '% of total workforce', '% of total employees who received regular performance and career development reviews during the reporting period', 'na']"},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'KIN +CARTA',
     'variable description': 'An equality measure that shows the difference in average earnings between women and men.',
     'variable': 'Gender Pay Gap', 'value': '15%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Mean gender pay gap'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'KIN +CARTA',
     'variable description': 'An equality measure that shows the difference in average earnings between women and men.',
     'variable': 'Gender Pay Gap', 'value': '14%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'FY22 target'},
    {'kpi': 'gender-pay-gap', 'score': 0.9, 'rank': 4.0, 'doc_org_std': 'KIN +CARTA',
     'variable description': 'An equality measure that shows the difference in average earnings between women and men.',
     'variable': 'Gender Pay Gap', 'value': '5%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': 'Long-term goal'},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
     'variable description': 'Employees', 'variable': 'Male', 'value': '266', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Male', '%', 'Female', '%_2', 'nan', ['Directors', '5', '56%', '4', '44%'], ['Senior managers', '266', '80%', '68', '20%'], ['Employees', '21,237', '79%', '5,585', '21%']]"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
     'variable description': 'Employees', 'variable': 'Female', 'value': '68', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "['Male', '%', 'Female', '%_2', 'nan', ['Directors', '5', '56%', '4', '44%'], ['Senior managers', '266', '80%', '68', '20%'], ['Employees', '21,237', '79%', '5,585', '21%']]"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
     'variable description': 'Average difference in pay between genders', 'variable': 'Mean (average) pay differential',
     'value': '24.2%', 'unit': '', 'std_year': '2020',
     'relevant quote from text': "['2020.0', '2019.0', '2018.0', 'nan', ['Mean (..."},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
     'variable description': 'Median difference in pay between genders',
     'variable': 'Median (mid-point) pay differential', 'value': '20.2%', 'unit': '', 'std_year': '2020',
     'relevant quote from text': "['2020.0', '2019.0', '2018.0', 'nan', ['Mean (..."},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG',
     'variable description': 'Percentage of female employees in the top 10% positions of responsibility',
     'variable': 'Gender diversity in the top 10% of positions of responsibility', 'value': '20%', 'unit': '',
     'std_year': '2021', 'relevant quote from text': "'20%', '2021'"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Samsung SDS',
     'variable description': 'Announcement of no gender gap in compensation', 'variable': 'Human Capital',
     'value': 'Gender Pay Equality Program', 'unit': '', 'std_year': '2022',
     'relevant quote from text': "[Areas of, Areas of_2, Improvements Made, ['Human Capital', 'Gender Pay Equality Program', 'Announcement of no gender gap in compensation']]"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Samsung SDS',
     'variable description': "['Mean', 'Gender pay gap', '%', 100.0]", 'variable': "Mean 'Gender pay gap'",
     'value': '%', 'unit': '%', 'std_year': '2022', 'relevant quote from text': "['tablenum-5']"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
     'variable description': 'Gender distribution of the Board members',
     'variable': 'Gender distribution of the Board members', 'value': '46% 54%', 'unit': '', 'std_year': '2021',
     'relevant quote from text': "Input data segment: 'Gender parity (**), 46% 54%'"},
    {'kpi': 'gender-pay-gap', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
     'variable description': 'Average number of training hours per employee in 2021',
     'variable': 'Hours of Training per Employee', 'value': '19', 'unit': 'Hours', 'std_year': '2021',
     'relevant quote from text': 'Hours of training per employee'}
]
"""

# ### KPI = 'ghg-scope-1-emissions'
kpi_mask = (df_quants_ranked['kpi'] == 'ghg-scope-1-emissions')
score_mask = (df_quants_ranked['score'] >= 0.75)
var_mask = (df_quants_ranked['variable'] != '')
df_kpi = df_quants_ranked[kpi_mask & score_mask & var_mask].groupby(
    by=['doc_org_std'],
    group_keys=False,
).apply(
    lambda x: x[cols_to_print].head(5)
).reset_index(drop=True).to_dict('records')
"""
`df_kpi` for 'ghg-scope-1-emissions'
[{'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ABB', 'variable description': 'SELECTED,',
  'variable': 'SELECTED,', 'value': '405', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['305-2', 'Total Scope 1 and 2 GHG emissions', 'SELECTED,', '405', '561', '998']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'ACCOR',
  'variable description': 'Reduction goal of absolute Scope 1 emissions by 46% by 2030 compared to the 2019 reference year',
  'variable': 'Scope 1', 'value': '46%', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "ACCOR'S RESPONSE"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'ACCOR',
  'variable description': 'Reduction goal of absolute Scope 2 emissions by 46% by 2030 compared to the 2019 reference year',
  'variable': 'Scope 2', 'value': '46%', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "ACCOR'S RESPONSE"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AKER CARBON CAPTURE',
  'variable description': 'Direct greenhouse gas emissions.', 'variable': 'CO2 emissions', 'value': '0 tCO2e',
  'unit': 'tCO2e', 'std_year': '2021', 'relevant quote from text': "['Scope 1', '0 tCO2e', 'tCO2e']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'AKER CARBON CAPTURE',
  'variable description': 'Total CO2 emissions across all scopes.', 'variable': 'CO2 emissions', 'value': '84.4 tCO2e',
  'unit': 'tCO2e', 'std_year': '2021',
  'relevant quote from text': "['Total, scope 1,2,3', '84.4 tCO,e', '37.1 tCO2e']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Greenhouse gas emissions (kt CO2 eq.) under the Montreal Protocol', 'variable': 'CO2 eq.',
  'value': '234', 'unit': 'kt CO2 eq.', 'std_year': '2021',
  'relevant quote from text': "tablenum-1: ['Montreal Protocol', '2021', '2020', '2019', ['Greenhouse gas emissions (kt CO2 eq.)', 234, 257, 247]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Greenhouse gas emissions (kt CO2 eq.) under the Montreal Protocol', 'variable': 'CO2 eq.',
  'value': '257', 'unit': 'kt CO2 eq.', 'std_year': '2021',
  'relevant quote from text': "tablenum-1: ['Montreal Protocol', '2021', '2020', '2019', ['Greenhouse gas emissions (kt CO2 eq.)', 234, 257, 247]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Greenhouse gas emissions (kt CO2 eq.) under the Montreal Protocol', 'variable': 'CO2 eq.',
  'value': '247', 'unit': 'kt CO2 eq.', 'std_year': '2021',
  'relevant quote from text': "tablenum-1: ['Montreal Protocol', '2021', '2020', '2019', ['Greenhouse gas emissions (kt CO2 eq.)', 234, 257, 247]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Scope 1 CO2 emissions in the year 2021', 'variable': 'CO2 eq.', 'value': '1,436',
  'unit': 'kt CO2 eq.', 'std_year': '2021',
  'relevant quote from text': "tablenum-0: ['2021', '2020', '2019', 'nan', ['Total', '1,822', '2,268', '2,698'], ['Of which CO2', '1,436', '1,495', '1,490'], ['Of which HFC', '349', '742', '1,174'], ['Others', '37', '31', '34']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ARKEMA',
  'variable description': 'Scope 1 CO2 emissions in the year 2020', 'variable': 'CO2 eq.', 'value': '1,495',
  'unit': 'kt CO2 eq.', 'std_year': '2021',
  'relevant quote from text': "tablenum-0: ['2021', '2020', '2019', 'nan', ['Total', '1,822', '2,268', '2,698'], ['Of which CO2', '1,436', '1,495', '1,490'], ['Of which HFC', '349', '742', '1,174'], ['Others', '37', '31', '34']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited',
  'variable description': 'Direct emissions from fuel used in port operations', 'variable': 'Scope 1', 'value': '32.5',
  'unit': 'Tonnes of CO2e', 'std_year': '2021', 'relevant quote from text': 'Total carbon emissions (Tonnes CO e) 2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited',
  'variable description': 'Direct emissions from various port equipment and activities', 'variable': 'Scope 1',
  'value': '2,54,702', 'unit': 'Tonnes of CO2e', 'std_year': '2021',
  'relevant quote from text': 'Total carbon emissions (Tonnes CO e) 2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited',
  'variable description': 'Direct emissions from harbouring and dredging activities', 'variable': 'Scope 1',
  'value': '1,58,941', 'unit': 'Tonnes of CO2e', 'std_year': '2021',
  'relevant quote from text': 'Total carbon emissions (Tonnes CO e) 2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited',
  'variable description': 'Direct emissions from DG sets and company vehicles', 'variable': 'Scope 1',
  'value': '95,761', 'unit': 'Tonnes of CO2e', 'std_year': '2021',
  'relevant quote from text': 'Total carbon emissions (Tonnes CO e) 2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited',
  'variable description': 'Direct emissions from various port equipment and activities', 'variable': 'Scope 1',
  'value': '1,02,101', 'unit': 'Tonnes of CO2e', 'std_year': '2021',
  'relevant quote from text': 'Total carbon emissions (Tonnes CO e) 2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Admiral Group plc',
  'variable description': 'Total measured Scope 3 emissions for the year', 'variable': 'Scope 3', 'value': '952',
  'unit': '', 'std_year': '2021',
  'relevant quote from text': "['This year, we have expanded our reporting boundary to include Category 3: Fuel and Energy-Related Activities...', 'Scope 3: FERA Waste', '115', 'Water Business Travel']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Admiral Group plc',
  'variable description': 'FY2020; UK', 'variable': 'FY2020; UK', 'value': '1,121', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['Scope 1', nan, nan, '1,121', '1,285', '192', '1,477']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Admiral Group plc',
  'variable description': 'FY2021; UK', 'variable': 'FY2021; UK', 'value': '1,285', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['Scope 1', nan, nan, '1,121', '1,285', '192', '1,477']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Admiral Group plc',
  'variable description': 'FY2021; Total', 'variable': 'FY2021; Total', 'value': '1,477', 'unit': '',
  'std_year': '2021', 'relevant quote from text': "['Scope 1', nan, nan, '1,121', '1,285', '192', '1,477']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'variable description': 'Emissions from fleet', 'variable': 'Scope 1', 'value': '12,159,451', 'unit': 'tCO2e/year',
  'std_year': '2020', 'relevant quote from text': 'Total GHG emissions by fleet'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'variable description': 'Total greenhouse gas emissions in 2020', 'variable': 'tCO2e/year', 'value': '12,257,395',
  'unit': '', 'std_year': '2020', 'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'variable description': 'Scope 1 emissions in 2020', 'variable': 'tCO2e/year', 'value': '114,775', 'unit': '',
  'std_year': '2020', 'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'variable description': 'Scope 3 emissions in 2020', 'variable': 'tCO2e/year', 'value': '12,126,681', 'unit': '',
  'std_year': '2020', 'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'variable description': 'Fleet GHG emissions in 2020', 'variable': 'tCOe/year', 'value': '12,159,451', 'unit': '',
  'std_year': '2020', 'relevant quote from text': 'tablenum-2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'variable description': 'Total Scope 1 emissions', 'variable': 'Scope 1', 'value': '3,176,635',
  'unit': 'Tonnes CO2-e', 'std_year': '2022',
  'relevant quote from text': "['Total Scope 1 emissions', '3,176,635', '1,333,191', '1,512,885']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'variable description': 'Total Scope 1 emissions', 'variable': 'Scope 1', 'value': '1,512,885',
  'unit': 'Tonnes CO2-e', 'std_year': '2022',
  'relevant quote from text': "['Total Scope 1 emissions', '3,176,635', '1,333,191', '1,512,885']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'variable description': 'Total Scope 1 emissions', 'variable': 'Scope 1', 'value': '1,333,191',
  'unit': 'Tonnes CO2-e', 'std_year': '2021',
  'relevant quote from text': "['Total Scope 1 emissions', '3,176,635', '1,333,191', '1,512,885']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'variable description': 'Total GHG emissions from aviation fuel, LPG, natural gas, ground diesel, ground bio diesel, ground petrol in 2018',
  'variable': 'GHG emissions sources', 'value': '3,733,290', 'unit': 'Tonnes CO2-e', 'std_year': '2020',
  'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'variable description': 'Total GHG emissions from aviation fuel, LPG, natural gas, ground diesel, ground bio diesel, ground petrol in 2019',
  'variable': 'GHG emissions sources', 'value': '3,928,748', 'unit': 'Tonnes CO2-e', 'std_year': '2020',
  'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
  'variable description': 'Total direct greenhouse gas emissions', 'variable': 'Total Emissions', 'value': '1,894',
  'unit': '', 'std_year': '2020',
  'relevant quote from text': "['2020', '2019', '2018', 'nan', ['Direct greenhouse gas emissions', '1,894', '2,004', '2,041'], ['of which carbon dioxide (CO2) emissions', '1,860', '1,971', '2,010'], ['of which nitrous oxide (N20) emissions', '24', '22', '22'], ['of which methane (CH4) emissions', '10', '11', '9']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Albioma',
  'variable description': 'CO2 Equivalent emissions', 'variable': 'CO2 Equivalent emissions', 'value': '2,041',
  'unit': '', 'std_year': '2018',
  'relevant quote from text': "['2018.0', 'nan', ['Scope 1 emissions', '2,041'], ['Scope 2 emissions', '2'], ['Scope 3 emissions', '298']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.95, 'rank': 2.0, 'doc_org_std': 'Allianz Group',
  'variable description': 'Achieved a 60% reduction in greenhouse gas (GHG) emissions per employee by year-end 2021 against a target reduction of 30 percent per employee by year-end 2025 (2020: 42 percent reduction) against a 2019 baseline across Scopes 1, 2, and selected Scope 3 emissions (currently energy-related emissions, business travel, and paper use). Full details in Table ENV-2.',
  'variable': 'GHG emissions per employee', 'value': '60% reduction', 'unit': '', 'std_year': '2021',
  'relevant quote from text': ''},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'Allianz Group',
  'variable description': 'Reduction of GHG emissions per employee by 2021 vs 2019',
  'variable': 'GHG emissions reduction per employee', 'value': '14', 'unit': '', 'std_year': '2021',
  'relevant quote from text': 'Section 02.6'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'Allianz Group',
  'variable description': 'Reduction of GHG emissions per employee by 2022 vs 2019',
  'variable': 'GHG emissions reduction per employee', 'value': '18', 'unit': '', 'std_year': '2021',
  'relevant quote from text': 'Section 02.6'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'American Express',
  'variable description': 'Scope 2 Emissions-Location Based mt CO2e', 'variable': 'Scope 2 Emissions',
  'value': '90,024', 'unit': 'mt CO2e', 'std_year': '2021', 'relevant quote from text': 'nan_2'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'variable description': 'Emissions of natural gas', 'variable': 'Natural Gas', 'value': '253',
  'unit': 'metric tonnes CO2e', 'std_year': '2021',
  'relevant quote from text': "['Scope 1', 'Natural Gas', '253', '213']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'variable description': 'Emissions of diesel', 'variable': 'Diesel', 'value': '3', 'unit': 'metric tonnes CO2e',
  'std_year': '2021', 'relevant quote from text': "['Scope 1', 'Diesel', '3', '3']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'variable description': 'Emissions of jet fuel', 'variable': 'Jet Fuel', 'value': '176', 'unit': 'metric tonnes CO2e',
  'std_year': '2021', 'relevant quote from text': "['Scope 1', 'Jet Fuel', '176', '352']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'variable description': 'Emissions of gasoline', 'variable': 'Gasoline', 'value': '6', 'unit': 'metric tonnes CO2e',
  'std_year': '2021', 'relevant quote from text': "['Scope 1', 'Gasoline', '6', '6']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'variable description': 'Emissions of refrigerant leakages', 'variable': 'Refrigerant Leakages', 'value': '591',
  'unit': 'metric tonnes CO2e', 'std_year': '2021',
  'relevant quote from text': "['Scope 1', 'Refrigerant Leakages', '591', '591']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Ashtead Group plc',
  'variable description': 'Total greenhouse gas emissions (Scope 1)', 'variable': 'tCO2e/year*', 'value': '30,099',
  'unit': '', 'std_year': '2022',
  'relevant quote from text': "['2022; UK', '2022; Total', '2021; UK', '2021; Total', 'nan', 'nan_2', ['Scope 1', 'tCO2e/year*', '30,099', '302,843', '30,610', '288,438'], ['Scope 2', 'tCO2e/year*', '357', '26,977', '2,409', '30,532'], ['Total', 'tCO2e/year*', '30,456', '329,820', '33,019', '318,970'], [nan, nan, nan, nan, nan, nan], ['Energy consumption used to calculate emissions', 'mWh', '131,148', '1,317,129', '139,912', '1,266,179']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Ashtead Group plc',
  'variable description': 'Current carbon intensity level', 'variable': 'current', 'value': '54.0',
  'unit': 'tCO2e/$m on a constant currency basis', 'std_year': '2022',
  'relevant quote from text': 'Ashtead Group plc Annual Report & Accounts 2022'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva plc',
  'variable description': 'Offshore', 'variable': 'Scope 1', 'value': '1,760', 'unit': 'tCO2e', 'std_year': '2021',
  'relevant quote from text': "['2021; Offshore', '2021; UK', '2021; Total', '2021; Restated2 Offshore', '2021; UK_2', '2021; Restated2 Total', 'nan', ['Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3']..."},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva plc', 'variable description': 'UK',
  'variable': 'Scope 1', 'value': '8,870', 'unit': 'tCO2e', 'std_year': '2021',
  'relevant quote from text': "['2021; Offshore', '2021; UK', '2021; Total', '2021; Restated2 Offshore', '2021; UK_2', '2021; Restated2 Total', 'nan', ['Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3']..."},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aviva plc',
  'variable description': 'Total', 'variable': 'Scope 1', 'value': '10,630', 'unit': 'tCO2e', 'std_year': '2021',
  'relevant quote from text': "['2021; Offshore', '2021; UK', '2021; Total', '2021; Restated2 Offshore', '2021; UK_2', '2021; Restated2 Total', 'nan', ['Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3', 'Emissions3']..."},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'Aviva plc',
  'variable description': 'Reduction in carbon intensity compared to the baseline year (2019)',
  'variable': 'Warming Potential', 'value': '-16% (compared to 2019)', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "Weighted average carbon intensity We use this metric to assess our investment portfolio's sensitivity to increase in carbon prices."},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.8, 'rank': 4.0, 'doc_org_std': 'Aviva plc',
  'variable description': 'Reduction targets for carbon intensity by 2025 and 2030', 'variable': 'Warming Potential',
  'value': '25% by 2025, 60% by 2030', 'unit': 'Percentage', 'std_year': '2021',
  'relevant quote from text': 'Our carbon intensity has reduced compared to 2019 (our baseline year) by 16% in line with Sustainability Ambition'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Bayer',
  'variable description': 'Direct emissions of carbon dioxide (CO2) in 2020', 'variable': 'Scope 1', 'value': 2.01,
  'unit': 'Million metric tons of CO2 equivalents', 'std_year': '2021',
  'relevant quote from text': "['2020.0', '2021.0', 'nan', ['Scope 1: Direct emissions ¹', 2.01, 1.93], ['of which carbon dioxide (CO2)', 1.96, 1.9], ['of which ozone-depleting substances', 0.012, 0.011], ['of which partially fluorinated hydrocarbons (HFCs)', 0.022, 0.014], ['of which nitrous oxide (N2O)', 0.008, 0.007], ['of which methane (CH4)', 0.003, 0.003], ['Scope 2: Indirect emissions2 according to the location-based method', 1.75, 1.56], ['Scope 2: Indirect emissions² according to the market-based method3', 1.57, 1.24], ['Total greenhouse gas emissions (Scope 1 and 2) according to the market-based method³', 3.58, 3.17], ['of which offset greenhouse gas emissions', 0.2, 0.3], ['Specific greenhouse gas emissions (kg COe/ € thousand external sales) according to the market-based method3, 4', 86.55, 71.95]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Bayer',
  'variable description': 'Direct emissions of carbon dioxide (CO2) in 2021', 'variable': 'Scope 1', 'value': 1.93,
  'unit': 'Million metric tons of CO2 equivalents', 'std_year': '2021', 'relevant quote from text': ''},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Bayer',
  'variable description': 'Direct emissions of carbon dioxide (CO2) in 2020', 'variable': 'Scope 1', 'value': 1.96,
  'unit': 'Million metric tons of CO2 equivalents', 'std_year': '2021', 'relevant quote from text': ''},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Bayer',
  'variable description': 'Direct emissions of carbon dioxide (CO2) in 2021', 'variable': 'Scope 1', 'value': 1.9,
  'unit': 'Million metric tons of CO2 equivalents', 'std_year': '2021', 'relevant quote from text': ''},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Bayer',
  'variable description': 'Total greenhouse gas emissions (Scope 1 and 2) according to the market-based method in 2020',
  'variable': 'Total', 'value': 3.58, 'unit': 'Million metric tons of CO2 equivalents', 'std_year': '2021',
  'relevant quote from text': ''},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.8, 'rank': 4.0, 'doc_org_std': 'BillerudKorsnäs',
  'variable description': 'Reduction in emissions in Scopes 1 and 2 compared to the base year (2016)',
  'variable': 'Science Based Targets: Reduction in emissions in Scopes 1 and 2, %', 'value': 59, 'unit': '',
  'std_year': '2021',
  'relevant quote from text': 'Target 20301: Science Based Targets: Reduction in emissions in Scopes 1 and 2, % 13'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'COMPASS GROUP',
  'variable description': 'Scope 1-Emissions from the combustion of fuel or the operation of any facility including fugitive emissions from refrigerants use / tCO2e',
  'variable': 'Scope 1-Emissions from the combustion of fuel or the operation of any facility including fugitive emissions from refrigerants use / tCO2e',
  'value': '174,627', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['ENERGY AND GREENHOUSE GAS EMISSIONS; Greenhouse gas emissions-Scope 1 and 2', '2021; UK and Offshore', '2021; Global', '2020; UK and Offshore', '2020; Global', '2019; Global', ['Scope 1-Emissions from the combustion of fuel or the operation of any facility including fugitive emissions from refrigerants use / tCO2e', '5,614', '88,616', '5,912', '106,047', '174,627'], ['Scope 2-Emissions resulting from the purchase of electricity, heat, steam of cooling (location based)/ tCOe', '2,096', '38,298', '3,300', '39,703', '45,875'], ['Scope 2-Emissions resulting from the purchase of electricity, heat, steam of cooling (market based)/ tCO2e', '3,119', '40,525', nan, nan, nan], ['Total gross emissions (location based) / tCO2e', '7,710', '126,914', '9,212', '145,750', '220,502'], ['tCO2e per million £ turnover', '5.3', '7.2', '6.1', '7,5', '9.1'], ['Energy', 'Energy', 'Energy', 'Energy', 'Energy', 'Energy'], ['Energy consumption used to calculate above emissions / kWh', '32,881,076', '480,805,034', '41,968,394', '556,869,904', nan], ['Energy consumption /kWh per million £ turnover', nan, '27,109', nan, '28,487', nan]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Direct GHG emissions (total) Scope 1', 'variable': 'GHG-Dir-Abs', 'value': '0',
  'unit': 'tCO2', 'std_year': '2021', 'relevant quote from text': 'tablenum-1'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Direct GHG emissions (total) Scope 1', 'variable': 'GHG-Dir-Abs', 'value': '0',
  'unit': 'tCO2', 'std_year': '2021', 'relevant quote from text': 'tablenum-1'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Direct GHG emissions (total) Scope 1', 'variable': 'GHG-Dir-Abs', 'value': '5,205',
  'unit': 'tCO2', 'std_year': '2021',
  'relevant quote from text': "['Direct GHG emissions (total) Scope 1', 'GHG-Dir-Abs', 'tCO2', '5,205', '100%', '5,575', '100%', '7%']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'variable description': 'Indirect GHG emissions (total) Scope 2', 'variable': 'GHG-Indir-Abs', 'value': '13,639',
  'unit': 'tCO2 (market based)', 'std_year': '2021',
  'relevant quote from text': "['Indirect GHG emissions (total) Scope 2', 'GHG-Indir-Abs', 'tCO2 (market based)', '13,639', '100%', '13,164', '100%', '-3%']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.95, 'rank': 2.0, 'doc_org_std': 'ECOLAB',
  'variable description': 'Direct (Scope 1) Emissions', 'variable': 'Emissions', 'value': '319,180', 'unit': 'MT CO2e',
  'std_year': '2021',
  'relevant quote from text': "['Direct (Scope 1) Emissions', 'MT COe', '319,180', '281,241', '282,199'], \\['Biogenic Emissions', 'MT CO2e', '127', '129.5', '215.8']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.95, 'rank': 2.0, 'doc_org_std': 'ECOLAB',
  'variable description': 'Direct (Scope 1) Emissions', 'variable': 'Emissions', 'value': '281,241', 'unit': 'MT COe',
  'std_year': '2021',
  'relevant quote from text': "['Direct (Scope 1) Emissions', 'MT COe', '319,180', '281,241', '282,199'], \\['Biogenic Emissions', 'MT CO2e', '127', '129.5', '215.8']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.95, 'rank': 2.0, 'doc_org_std': 'ECOLAB',
  'variable description': 'Direct (Scope 1) Emissions', 'variable': 'Emissions', 'value': '282,199', 'unit': 'MT COe',
  'std_year': '2021',
  'relevant quote from text': "['Direct (Scope 1) Emissions', 'MT COe', '319,180', '281,241', '282,199'], \\['Biogenic Emissions', 'MT CO2e', '127', '129.5', '215.8']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'ECOLAB', 'variable description': 'Scope 1',
  'variable': 'Scope 1', 'value': '3.6%', 'unit': '', 'std_year': '2021', 'relevant quote from text': 'Appendix'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'ECOLAB', 'variable description': 'Scope 2',
  'variable': 'Scope 2', 'value': '0.6%', 'unit': '', 'std_year': '2021', 'relevant quote from text': 'Appendix'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'KIN +CARTA',
  'variable description': 'Scope 1 emissions from UK and offshore operations', 'variable': 'Scope 1 Emissions (tCO2e)',
  'value': '8.91', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['UK and offshore', 'Global (excluding UK and offshore)', '% UK', 'nan', ['Scope 1 emissions (tCO2e)', '8.91', '0.07', 99], ['Scope 2 emissions (tCO2e)', '115.53', '32.04', 78], ['Total emissions (tCO2e)', '124.44', '32.11', 79], ['Energy consumption (kWh)', '542,227', '137,816', 80]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'KIN +CARTA',
  'variable description': 'Emissions from the combustion of fuel', 'variable': 'Scope 1-Combustion', 'value': '9 (6%)',
  'unit': '', 'std_year': '2021',
  'relevant quote from text': "['Scope 1-combustion of fuel', 'Scope 2-electricity', 'Total emissions', 'nan', [2021, '9 (6%)', '148 (94%)', 157], [2020, '78 (14%)', '490 (86%)', 567]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'KIN +CARTA',
  'variable description': 'Total emissions from all sources', 'variable': 'Total Emissions', 'value': '157', 'unit': '',
  'std_year': '2021',
  'relevant quote from text': "['Scope 1-combustion of fuel', 'Scope 2-electricity', 'Total emissions', 'nan', [2021, '9 (6%)', '148 (94%)', 157], [2020, '78 (14%)', '490 (86%)', 567]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'KIN +CARTA',
  'variable description': 'Emissions from the combustion of fuel', 'variable': 'Scope 1-Combustion',
  'value': '78 (14%)', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['Scope 1-combustion of fuel', 'Scope 2-electricity', 'Total emissions', 'nan', [2021, '9 (6%)', '148 (94%)', 157], [2020, '78 (14%)', '490 (86%)', 567]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'KIN +CARTA',
  'variable description': 'Total emissions from all sources', 'variable': 'Total Emissions', 'value': '567', 'unit': '',
  'std_year': '2021',
  'relevant quote from text': "['Scope 1-combustion of fuel', 'Scope 2-electricity', 'Total emissions', 'nan', [2021, '9 (6%)', '148 (94%)', 157], [2020, '78 (14%)', '490 (86%)', 567]]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Kier Group plc',
  'variable description': 'Combustion of fuel and operation of facilities', 'variable': 'Scope 1', 'value': '74,139',
  'unit': 'tonnes CO2e', 'std_year': '2021',
  'relevant quote from text': "['UK', 'Global', 'UK_2', 'Global_2', 'UK_3', 'Global_3', 'nan', ['Ghg emissions data', '18-19', '18-19', '19-20', '19-20', '20-21', '20-21'], ['Scope 1 (tonnes CO Combustion of fuel and operation of facilities', '74,139', '86,839', '54,669', '70,993', '44,315', '53,175']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Kier Group plc',
  'variable description': 'Combustion of fuel and operation of facilities', 'variable': 'Scope 1', 'value': '54,669',
  'unit': 'tonnes CO2e', 'std_year': '2021',
  'relevant quote from text': "['UK', 'Global', 'UK_2', 'Global_2', 'UK_3', 'Global_3', 'nan', ['Ghg emissions data', '18-19', '18-19', '19-20', '19-20', '20-21', '20-21'], ['Scope 1 (tonnes CO Combustion of fuel and operation of facilities', '74,139', '86,839', '54,669', '70,993', '44,315', '53,175']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Kier Group plc',
  'variable description': 'Combustion of fuel and operation of facilities', 'variable': 'Scope 1', 'value': '44,315',
  'unit': 'tonnes CO2e', 'std_year': '2021',
  'relevant quote from text': "['UK', 'Global', 'UK_2', 'Global_2', 'UK_3', 'Global_3', 'nan', ['Ghg emissions data', '18-19', '18-19', '19-20', '19-20', '20-21', '20-21'], ['Scope 1 (tonnes CO Combustion of fuel and operation of facilities', '74,139', '86,839', '54,669', '70,993', '44,315', '53,175']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'Kier Group plc',
  'variable description': 'Carbon emissions from fuel used on projects', 'variable': 'Fuel (Project)', 'value': 58.0,
  'unit': '', 'std_year': '2021', 'relevant quote from text': 'KIKIER'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing',
  'variable description': "Direct emissions from Lenzing's pulp and fiber production facilities.",
  'variable': 'Direct Emissions', 'value': '15%', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "Scope 1 Direct emissions from Lenzing's pulp and fiber production facilities"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Lenzing',
  'variable description': 'Fossil energy consumed by Lenzing Group', 'variable': 'Million GJ', 'value': 23.39,
  'unit': '', 'std_year': '2021',
  'relevant quote from text': "['2014', '2019', '2020', '2021', 'nan', ['Fossil primary energy', '23.39', '22.21', 18.3, '21.78'], ['Renew..."},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'variable description': 'Total Scope 1 emissions for the year ended 2020.', 'variable': 'Scope 1', 'value': 135.0,
  'unit': '', 'std_year': '2020',
  'relevant quote from text': 'GHG\'s: CO2, N20, and CH4. Direct energy: IPCC 2006 Guidelines for National Greenhouse Gas Inventories. Natural gas: DEFRA 2020 conversion factor for cubic meters natural gas. Diesel: DEFRA 2020 conversion factor for litres diesel. LPG: DEFRA 2020 conversion factor for litres LPG. Purchased electricity: UK Defra 2020, Russia and Cyprus, IEA Fuel Combustion 2019, and Foreign Electricity Emission Factors. European market emission factors for electricity: AIB, European Residuals Mixes for 2018. Office car: DEFRA 20120 conversion factor for kilometers of unknown fuel (average car). District heating: electricity factors were adjusted using the same ratio as between UK electricity and district heating (from DEFRA 2020 conversion factors for UK electricity, and district heat and steam). Business travel: DEFRA 2020 GHG Conversion Factors for flights and rail travel. Sawdust emissions calculated by Trucost using FAO and IPCC. Sawdust is an "out of scope" item and is not included in the total emissions.'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'variable description': 'tonnes CO2e', 'variable': 'tonnes CO2e', 'value': '25,317', 'unit': 'tonnes CO2e',
  'std_year': '2020', 'relevant quote from text': "['Scope 1', 'tonnes CO2e', '30,976', '27,597', '25,317']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'variable description': 'Energy Consumption', 'variable': 'Energy Consumption', 'value': '64,680',
  'unit': 'tonnes CO2e', 'std_year': '2020',
  'relevant quote from text': "['Scope 2 (location-based)', 'tonnes CO2e', '62,605', '63,643', '64,680']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'variable description': 'GHG Emissions', 'variable': 'GHG Emissions', 'value': '231', 'unit': 'tonnes CO2e',
  'std_year': '2020', 'relevant quote from text': "['Scope 3', 'tonnes CO2e', '231', '200', '89']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'variable description': 'GHG Emissions', 'variable': 'GHG Emissions', 'value': '200', 'unit': 'tonnes CO2e',
  'std_year': '2020', 'relevant quote from text': "['Scope 3', 'tonnes CO2e', '231', '200', '89']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
  'variable description': 'Scope 1 emissions', 'variable': 'Scope 1 emissions', 'value': '18,979.00*',
  'unit': 'Tonnes of CO2e', 'std_year': '2020',
  'relevant quote from text': "['Scope 1 emissions', 'Tonnes of CO2e', '18,979.00*', '18,960.67*', '18,819.24']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
  'variable description': 'Scope 2 emissions (location-based)', 'variable': 'Scope 2 emissions (location-based)',
  'value': '28,359.00*', 'unit': 'Tonnes of CO2e', 'std_year': '2020',
  'relevant quote from text': "['Scope 2 emissions (location-based)', 'Tonnes of CO2e', '28,359.00*', '41,894.14*', '45,174.51']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
  'variable description': 'Scope 2 emissions (market-based)', 'variable': 'Scope 2 emissions (market-based)',
  'value': '23,526.00*', 'unit': 'Tonnes of CO2e', 'std_year': '2020',
  'relevant quote from text': "['Scope 2 emissions (market-based)', 'Tonnes of CO2e', '23,526.00*', '27,651.00*', 'n/a']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Responsible Business Report',
  'variable description': 'Reduce absolute Scope 1, 2 and 3 (business travel) GHG emissions 46% by 2030 from a 2019 base year',
  'variable': 'GHG Emissions', 'value': '46%', 'unit': '', 'std_year': '2020',
  'relevant quote from text': "Capita's climate-related financial disclosures 2020 continued"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG',
  'variable description': 'Direct GHG Emissions (excluding Scope 3)',
  'variable': 'Direct GHG Emissions (excluding Scope 3)', 'value': '2 kt eq. CO2', 'unit': '', 'std_year': '2021',
  'relevant quote from text': 'Natural-Direct & Indirect GHG emissions (excluding Scope 3):-Scope 1: 2 kt eq. CO2-Scope 2: 43 kt eq. CO2-Power efficiency (PUE): 1.35-% of revenues dedicated to sustainable activities: 7.8%'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Samsung SDS',
  'variable description': 'GHG Emissions (Scope 1,2)', 'variable': 'GHG Emissions (Scope 1,2)',
  'value': '101,882 tCO2eq', 'unit': 'tCO2eq', 'std_year': '2021', 'relevant quote from text': '2021 Key Figures'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'Savills plc',
  'variable description': 'Emissions from business travel by the Group owned or leased vehicles and the combustion of fuels within our occupied offices.',
  'variable': 'Scope 1 emissions', 'value': 62.0, 'unit': '', 'std_year': '2021', 'relevant quote from text': ''},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.95, 'rank': 2.0, 'doc_org_std': 'THE a2 MILK COMPANY LIMITED',
  'variable description': 'Total emissions for Scope 1, 2, and 3', 'variable': 'Greenhouse Gas Emissions (tCOe)',
  'value': '356,587', 'unit': 'tCOe', 'std_year': '2021',
  'relevant quote from text': "['FY21', 'FY207', 'FY197', 'nan', ['Total', '356,587', '509,533', '420,600'], ['Scope 12', '250', '228', '206'], ['Scope 2 superscript(3)', '1,720', '1,613', '1,507'], ['Scope 34', '354,617', '507,693', '418,887'], ['Direct operations (Scope 1, 2 and 3)', '2,862', '3,867', '4,923'], ['Third-party processing and freight', '76,140', '127,177', '103,863'], ['On-farm6', '277,585', '378,489', '311,814']]"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Reduction achieved through increase in electricity purchased from CO2-free sources and closure/sale of mills',
  'variable': 'Fossil CO2 emissions', 'value': '8% decrease', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['EGY', 'BUSINESSES', 'EUR 1.9 (0.8) million, 6,100 (8,600) tonnes of CO2 and 35,000 (44,000) MWh of energy. Reducing emissions to air in production...']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to air', 'variable': 'Fossil CO2 emissions (Scope 1)', 'value': '2.7m t',
  'unit': '', 'std_year': '2021', 'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'UPM',
  'variable description': 'Emissions to air', 'variable': 'Biogenic CO2 emissions (Scope 1)', 'value': '9.8m t',
  'unit': '', 'std_year': '2021', 'relevant quote from text': 'tablenum-0'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
  'variable description': 'Emissions calculated using the market-based approach in 2021',
  'variable': 'Scope 1 and Scope 2', 'value': '2.3 MtCO2eq', 'unit': 'Tonnes of CO2 equivalent', 'std_year': '2021',
  'relevant quote from text': 'text-segments'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
  'variable description': 'Market-based Scope 1 emissions for VINCI Concessions in 2021',
  'variable': 'Market-based Scope 1 emissions', 'value': '117,558 tonnes', 'unit': 'Tonnes', 'std_year': '2021',
  'relevant quote from text': 'tablenum-1'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
  'variable description': 'Location-based Scope 1 emissions for VINCI Concessions in 2021',
  'variable': 'Location-based Scope 1 emissions', 'value': '138,106 tonnes', 'unit': 'Tonnes', 'std_year': '2021',
  'relevant quote from text': 'tablenum-1'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
  'variable description': 'Market-based Scope 1 emissions for VINCI Autoroutes in 2021',
  'variable': 'Market-based Scope 1 emissions', 'value': '20,951 tonnes', 'unit': 'Tonnes', 'std_year': '2021',
  'relevant quote from text': 'tablenum-1'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'VINCI',
  'variable description': 'Market-based Scope 1 emissions for VINCI Airports in 2021',
  'variable': 'Market-based Scope 1 emissions', 'value': '91,911 tonnes', 'unit': 'Tonnes', 'std_year': '2021',
  'relevant quote from text': 'tablenum-1'},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'WEBUILD',
  'variable description': 'Absolute GHG emissions (scope 1&255) have decreased by 18% compared to 2020',
  'variable': 'Emissions', 'value': '18%', 'unit': '', 'std_year': '2021',
  'relevant quote from text': "['nearly all its activities are EU taxonomy-eligible as they contribute to the mitigation and adaptation to climate change (see the dedicated box in this section);', 'absolute GHG emissions (scope 1&255) have decreased by 18% compared to 2020;']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'WEBUILD',
  'variable description': 'GHG emissions (scope 182)* (kt CO2)', 'variable': 'Emissions', 'value': '476.6',
  'unit': 'kt CO2', 'std_year': '2021',
  'relevant quote from text': "['GHG emissions (scope 182)* (kt CO2)', '476.6 430,0 353,5']"},
 {'kpi': 'ghg-scope-1-emissions', 'score': 0.9, 'rank': 3.0, 'doc_org_std': 'adesso SE',
  'variable description': 'CO2 emissions from Heating (district heating) in 2018', 'variable': 'CO2 (Scope 1)',
  'value': '56,034', 'unit': 'kg', 'std_year': '2021',
  'relevant quote from text': "['2', 'Heating (district heating)', 'kWh', '1,203,253', '327', '359,773', '98', '348,685'], ['2-3', 'Total', '-', '-', '-', '7,470,352', '2,093', '4,130,624']"}
  ]
"""

# ## Next steps
"""
Here, we have seen one example of filtering quantitative info relevant to KPIs of interest. 
However, the relevant quants can be filtered in other ways. 
Another example can be found in `document_processing/extract_quants_and_estimate_kpis.py` 
"""