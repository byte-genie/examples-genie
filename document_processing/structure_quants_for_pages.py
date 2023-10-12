# # Structure quants from document pages


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
df_filtered_pages[['query', 'score', 'pagenum', 'doc_name', 'file_page', 'file_quants']].head().to_dict('records')
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
        add_file_path=1,
    )
    for file in structured_quants_files[:5]
]
df_structured_quants_sample = utils.async_utils.run_async_tasks(tasks)
df_structured_quants_sample = [resp.get_output() for resp in df_structured_quants_sample]
df_structured_quants_sample = [pd.DataFrame(df) for df in df_structured_quants_sample]
df_structured_quants_sample = pd.concat(df_structured_quants_sample)
## filter over rows with non-empty values
df_structured_quants_sample = df_structured_quants_sample[df_structured_quants_sample['value'] != '']
df_structured_quants_sample = df_structured_quants_sample.reset_index(drop=True)
"""
Length of df_structured_quants_sample, `len(df_structured_quants_sample)`: 80
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
First 5 ranked quants files:
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
        files=quants_ranking_files[:10],
        add_file_path=1
    )
]
df_quants_ranked = utils.async_utils.run_async_tasks(tasks)
df_quants_ranked = [resp.get_output() for resp in df_quants_ranked]
df_quants_ranked = [pd.DataFrame(df) for df in df_quants_ranked]
df_quants_ranked = pd.concat(df_quants_ranked)
## get KPI
df_quants_ranked['kpi'] = [
    os.path.splitext(file)[0].split('_query-')[-1] for file in df_quants_ranked['file']
]
## sort data by rank
df_quants_ranked = df_quants_ranked.sort_values(['kpi', 'score'], ascending=False).reset_index(drop=True)
## filter over rows with non-empty values
df_quants_ranked = df_quants_ranked[df_quants_ranked['value'] != ''].reset_index(drop=True)

"""
df_quants_ranked columns, `list(df_quants_ranked.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'file', 'pagenum', 'relevant quote from text', 'row_id', 'score', 'unit', 'value', 'variable', 'variable description']
First few rows of df_quants_ranked, `df_quants_ranked[['company name', 'variable description', 'variable', 'value', 'score']].head().to_dict('records')`
[
    {'company name': '', 'variable description': 'The mean gender pay gap is 14.7% for the year 2021.', 'variable': 'MEAN', 'value': '14.7%', 'score': 1.0}, 
    {'company name': '', 'variable description': 'The median gender pay gap is 16.7% for the year 2021.', 'variable': 'MEDIAN', 'value': '16.7%', 'score': 1.0}, 
    {'company name': '', 'variable description': '97.5% of women are receiving a bonus.', 'variable': 'WOMEN', 'value': '97.5%', 'score': 0.5}, 
    {'company name': '', 'variable description': '98.6% of men are receiving a bonus.', 'variable': 'MEN', 'value': '98.6%', 'score': 0.5}, 
    {'company name': '', 'variable description': 'The mean bonus pay gap is 43.7% for the year 2021.', 'variable': 'MEAN', 'value': '43.7%', 'score': 0.5}
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

# ## Summarise data for qualitative KPIs

# ### Set qualitative KPIs
qual_kpis = ['anti-corruption policies', 'anti-bribery policies']

# ### Summarise data by qualitative KPIs
"""
Since, both KPIs are quite similar, we will summarise each page that is relevant to either KPI, both both KPIs.
"""
text_summary_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"Summarising pages for ({doc_num}/{len(doc_names)}): {doc_name}")
    ## get top 2 page numbers for the document that are most relevant to these KPIs
    page_numbers = df_filtered_pages[
        (df_filtered_pages['doc_name'] == doc_name) &
        (df_filtered_pages['query'].isin(qual_kpis)) &
        (df_filtered_pages['page_rank'] <= 2)
        ]['pagenum'].unique().tolist()
    ## if no relevant pages in this document, move to next one
    if len(page_numbers) <= 0:
        logger.info(f'No pages in the document, {doc_name}, that are ranked in top 2 for {qual_kpis}')
        continue
    ## define tasks
    tasks = [
        bg_async.async_summarise_pages(
            doc_name=doc_name,
            page_numbers=page_numbers,
            summary_type='summarise by topics',
            summary_topics=qual_kpis,
        )
    ]
    ## run tasks
    text_summary_responses_ = utils.async_utils.run_async_tasks(tasks)
    text_summary_responses = text_summary_responses + text_summary_responses_
    ## wait for some time to avoid rate limit errors
    time.sleep(2 * 60)

# ### Read text summary output
text_summary_files = [resp.get_output() for resp in text_summary_responses]
text_summary_files = [file for file in text_summary_files if file is not None]
"""
Number of documents with text summary files, `len(text_summary_files)`: 43
text summary files for first document, `text_summary_files[0]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=structured/format=csv/variable_desc=summary-by-topics/source=summarise_text/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-116_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=structured/format=csv/variable_desc=summary-by-topics/source=summarise_text/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-41_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=structured/format=csv/variable_desc=summary-by-topics/source=summarise_text/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-42_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies.csv'
]
"""
## flatten text summary files
text_summary_files = [file for files in text_summary_files for file in files]
"""
Total number of text summary files: `len(text_summary_files)`: 103
"""

# ### Read text summary files
tasks = [
    bg_sync.async_read_files(
        files=text_summary_files,
        add_file_path=1,
    )
]
df_text_summary = utils.async_utils.run_async_tasks(tasks)
df_text_summary = [resp.get_output() for resp in df_text_summary]
df_text_summary = [pd.DataFrame(df) for df in df_text_summary]
df_text_summary = pd.concat(df_text_summary)
## drop rows with empty summary
df_text_summary = df_text_summary[df_text_summary['summary'] != ''].reset_index(drop=True)
"""
Columns in df_text_summary_files, `list(df_text_summary_files.columns)`:
['context', 'file', 'relevant quote from text', 'summary', 'topic']
"""
## re-order columns
df_text_summary = df_text_summary[['topic', 'summary', 'relevant quote from text', 'context', 'file']]
## add pagenum
df_text_summary['pagenum'] = [
    int(file.split('_pagenum-')[-1].split('_')[0])
    for file in df_text_summary['file']
]
## add doc_name
df_text_summary['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_text_summary['file']
]

# ### Merge document info onto `df_text_summary_files`
df_text_summary = pd.merge(
    left=df_text_summary,
    right=df_filtered_pages[['doc_org_std', 'doc_org', 'doc_type', 'doc_year', 'pagenum', 'doc_name']].drop_duplicates(),
    on=['pagenum', 'doc_name'],
    how='left'
)
## drop duplicate summaries for the same company
df_text_summary = df_text_summary.drop_duplicates(['doc_org_std', 'summary']).reset_index(drop=True)
"""
A sample of df_text_summary_files, `df_text_summary[['doc_org_std', 'topic', 'summary', 'relevant quote from text', 'pagenum', 'doc_name']].head().to_dict('records')`
[
    {'doc_org_std': 'BillerudKorsnäs', 'topic': 'Anti-corruption Policies', 'summary': 'Policies and management systems', 'relevant quote from text': 'Policies and management systems', 'pagenum': 116, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'}, 
    {'doc_org_std': 'BillerudKorsnäs', 'topic': 'Anti-corruption policies', 'summary': "BillerudKorsnäs has a Group-wide Code of Conduct based on international standards regarding human rights, working conditions, environment, and anti-corruption. The Code of Conduct is integrated into the company's policy framework and provides basic guidelines on responsible business practices. The company conducts risk assessments and implements measures to address corruption risks.", 'relevant quote from text': "['Responsible business', 'BillerudKorsnäs seeks to act responsibly...', 'Target 2021 Outcome...', 'Our Code of Conduct is partly based on international standards...', 'Responsible Business Compliance programme...', 'Follow-up of risk assessment and action plans...']", 'pagenum': 41, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'}, 
    {'doc_org_std': 'BillerudKorsnäs', 'topic': 'Anti-corruption Policies', 'summary': 'BillerudKorsnäs conducts continuous training initiatives on responsible business with a set training plan. E-learning in anti-corruption was completed by about 130 persons in 2021. Classroom training on responsible business was carried out for more than 220 employees.', 'relevant quote from text': 'Employees and sales agents are trained regularly in line with a set training plan to mitigate higher risks in responsible business areas. In 2021, about 130 persons completed e-learning in anti-corruption. Adapted classroom training on responsible business was carried out for more than 220 employees.', 'pagenum': 42, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'}, 
    {'doc_org_std': 'BillerudKorsnäs', 'topic': 'Anti-bribery Policies', 'summary': 'No specific information provided.', 'relevant quote from text': '', 'pagenum': 42, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf'}, 
    {'doc_org_std': 'UPM', 'topic': 'Anti-corruption Policies', 'summary': 'UPM has a zero-tolerance attitude towards corruption and bribery. It has an Anti-Corruption Code of Conduct and conducts regular risk assessments.', 'relevant quote from text': 'UPM Code of Conduct, UPM Anti-Corruption Rules, due diligence procedures for suppliers and third parties, risk assessments', 'pagenum': 68, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf'}
]
"""