# # Summarise text from document pages
"""
In this example, we will use previously filtered document pages (see `document_processing/filter_relevant_pages.py`)
to summarise page text by KPIs of interest. We will do so in the following steps:
- Summarise text from filtered pages;
- Score summarised text by relevance to KPIs;
- Merge document meta-data onto estimate KPI values;
- Standardise company names;
- Sort values by standardised company names, and score.
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
    # ## wait for some time to avoid rate limit errors
    # time.sleep(2 * 60)

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
        add_file=1,
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

# ### Rank text summaries by relevance to KPIs
text_ranking_responses = []
for kpi_num, kpi in enumerate(qual_kpis):
    logger.info(f"Ranking text for ({kpi_num}/{len(qual_kpis)}): {kpi}")
    tasks = [
        bg_async.async_rank_data(
            attr=kpi,
            files=text_summary_files,
            method='llm-ranking',
            non_null_cols=['summary'],
            cols_to_use=['summary', 'relevant quote from text'],
            cols_not_use=['topic', 'context'],
        )
    ]
    text_ranking_responses_ = utils.async_utils.run_async_tasks(tasks)
    text_ranking_responses = text_ranking_responses + text_ranking_responses_
    # ## wait for some time to avoid rate limit errors
    # time.sleep(15 * 60)
## get ranked quant files
text_ranking_files = [resp.get_output() for resp in text_ranking_responses]
text_ranking_files = [file for file in text_ranking_files if file is not None]
## flatten text_ranking_files
text_ranking_files = [file for files in text_ranking_files for file in files]
## take unique files
text_ranking_files = list(set(text_ranking_files))
"""
Total number of ranked text files, `len(text_ranking_files)`: 135
First 5 ranked text files, `text_ranking_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-03-lse_rav_2020pdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_karishma-03-lse_rav_2020pdf_pagenum-45_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies_llm-scoring_query-anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_srpdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_jason_08_srpdf_pagenum-70_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies_llm-scoring_query-anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jaime_allianz-group_sustainability-reportpdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_jaime_allianz-group_sustainability-reportpdf_pagenum-97_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies_llm-scoring_query-anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_16_samsung_sdspdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_16_samsung_sdspdf_pagenum-83_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies_llm-scoring_query-anti-corruption-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_srpdf/data_type=similarity/format=csv/variable_desc=llm-scoring/source=rank_data/userid_stuartcullinan_uploadfilename_jason_09_srpdf_pagenum-120_page-data_summary-by-topics_topics-anti-corruption-policies_anti-bribery-policies_llm-scoring_query-anti-corruption-policies.csv'
]
"""


# ### Read ranked text files
tasks = [
    bg_sync.async_read_files(
        files=text_ranking_files,
        add_file=1,
    )
]
df_text_ranked = utils.async_utils.run_async_tasks(tasks)
df_text_ranked = [resp.get_output() for resp in df_text_ranked]
df_text_ranked = [pd.DataFrame(df) for df in df_text_ranked]
df_text_ranked = pd.concat(df_text_ranked)
"""
Number of rows in df_text_ranked, `len(df_text_ranked)`: 359
Columns in df_text_ranked, `list(df_text_ranked.columns)`
['context', 'file', 'relevant quote from text', 'row_id', 'score', 'score_query', 'summary', 'topic']
A sample of df_text_ranked, `df_text_ranked[['score_query', 'score', 'topic', 'summary']].head().to_dict('records')`
[
    {'score_query': 'anti-bribery policies', 'score': 0.9, 'topic': 'Anti-corruption Policies', 'summary': 'The Group has introduced or updated policies relating to the environment, health & safety, conflicts of interest and modern slavery and human trafficking. Employees have refreshed their knowledge of existing policies and procedures, including interactive training on anti-bribery and corruption.'}, 
    {'score_query': 'anti-bribery policies', 'score': 0.5, 'topic': 'Anti-bribery Policies', 'summary': 'The Group has conducted interactive training on anti-bribery and corruption in the second half of 2020, as part of the rolling program of training and development. Employees have refreshed their knowledge of existing policies and procedures.'}, 
    {'score_query': 'anti-bribery policies', 'score': 1.0, 'topic': 'Anti-Corruption Policies', 'summary': 'We have a zero-tolerance policy for bribery. All colleagues must complete anti-corruption and bribery training tied to their annual compensation.'}, 
    {'score_query': 'anti-bribery policies', 'score': 1.0, 'topic': 'Anti-Bribery Policies', 'summary': 'We have a zero-tolerance policy for bribery. All colleagues must complete anti-corruption and bribery training tied to their annual compensation.'}, 
    {'score_query': 'anti-bribery policies', 'score': 0.35, 'topic': 'Data Privacy and Ethics', 'summary': 'Privacy Champions will be appointed in all business units that process personal data across Allianz Group companies. Privacy Champions are employees who dedicate a portion of their time to dealing with privacy-related topics.'}
]
"""
## add pagenum
df_text_ranked['pagenum'] = [
    int(file.split('_pagenum-')[-1].split('_')[0])
    for file in df_text_ranked['file']
]
## add doc_name
df_text_ranked['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_text_ranked['file']
]

# ### Merge document info onto `df_text_summary_files`
df_text_ranked = pd.merge(
    left=df_text_ranked,
    right=df_filtered_pages[['doc_org_std', 'doc_org', 'doc_type', 'doc_year', 'pagenum', 'doc_name']].drop_duplicates(),
    on=['pagenum', 'doc_name'],
    how='left'
)
## drop duplicate summaries for the same company
df_text_ranked = df_text_ranked.drop_duplicates(['doc_org_std', 'summary']).reset_index(drop=True)
## fill missing score with np.nan
df_text_ranked.loc[df_text_ranked['score'] == '', 'score'] = np.nan
## rename score_query column to kpi
df_text_ranked = df_text_ranked.rename(
    columns={'score_query': 'kpi'}
)
## rank summaries by score for each company
df_text_ranked['rank'] = df_text_ranked.groupby(
    by=['kpi'],
    group_keys=False
)['score'].rank('dense', ascending=False)
## add summary length
df_text_ranked['summary_len'] = [len(str(summary)) for summary in df_text_ranked['summary']]
## sort data by kpi, and score
df_text_ranked = df_text_ranked.sort_values(['kpi', 'score', 'summary'], ascending=False).reset_index(drop=True)
"""
Check the top ranked rows for KPIs
`df_text_ranked[['kpi', 'score', 'doc_org_std', 'topic', 'summary', 'relevant quote from text', 'context', 'pagenum', 'doc_name']].head().to_dict('records')`
[
    {'kpi': 'anti-corruption policies', 'score': 1.0, 'doc_org_std': 'Samsung SDS', 'topic': 'Anti-Corruption Policies', 'summary': 'Disclosure of business principle guidelines, anti-corruption policy, and fair competition policy on the website', 'relevant quote from text': "['Business Ethics', 'Bribery & Corruption Policy Bribery & Corruption Programs Business Ethics Programs']", 'context': '## tablenum-0\n\n[\'Areas of\', \'Areas of_2\', \'Improvements Made\', [\'Corporate Governance\', \'Board Structure\', \'Risk management through the BOD and the Risk Management Council\'], [\'Corporate Governance\', \'Stakeholder Governance\', \'Disclosure of tax strategy, risk management, etc. Disclosure of the ESG goals within Sustainability Report\'], [\'Data Privacy and Security\', \'Data privacy and security policy\', \'Disclosure of policies related to information protection and information security Disclosure of information protection management system, process, and training status Disclosure of organizational and responding status to countermeasure information protection regulations in each country\'], [\'Data Privacy and Security\', \'Data privacy programs\', \'Disclosure of training status for employees in charge of information protection and security Disclosure of regular/ad hoc security check process\'], [\'Data Privacy and Security\', \'Cybersecurity program\', \'. ISO 27001 certification published on the website\'], [\'Human Capital\', \'Discrimination Policy\', \'ILO Convention-based employee protection clauses published on the website\'], [\'Human Capital\', \'Diversity Programs\', \'Disclosure of the current status of employee diversity-related training and related organizational operation status\'], [\'Human Capital\', \'Gender Pay Equality Program\', \'Announcement of no gender gap in compensation\'], [\'Business Ethics\', \'Bribery & Corruption Policy Bribery & Corruption Programs Business Ethics Programs\', \'Disclosure of business principle guidelines, anti-corruption policy, and fair competition policy on the website Disclosure of the status of regular training for compliance and anti-corruption Sharing compliance terms, cases, and countermeasures through Compliance Management System(CPMS) Conducted regular audits on corruption and compliance Initiation of Declaration of Compliance\'], [\'Business Ethics\', \'Whistleblower Programs\', \'Operation of 24/7 whistle-blowing channel on the website in the languages of major countries where business is conducted Disclosure of whistle-blowing process Disclosure of the number of reports received through whistle-blowing channels and compliance/corruption guidelines violation cases\'], [\'Business Ethics\', \'Political Involvement Policy\', \'Disclosure of the political neutrality policy specified in the Code of Conduct Guidelines on the website\']]\n---\n## text-segments\n\n[\'Our Business Commitments Material Highlights ESG Factbook Environmental Social Governance Board of Directors Protection of Shareholders Rights and Interests Integrated Risk Management Ethics and Compliance Management Information Protection Appendix\', \'81\', "Financial Risk Samsung SDS closely monitors and responds to factors of market, credit, and liquidity risks based on risk management policy. The Finan- financial cial Management Team supervises risk management and establishes global financial risk management policies in cooperation with the company\'s business divisions and individual domestic and foreign companies to measure, evaluate, and hedge financial risks.", \'Tax Risk As global business expands, the importance of tax risk management along. Samsung SDS faithfully fulfills its tax filing and emerges pay- ment in accordance with applicable statutes, and prepares counter pol- icies by identifying tax risks by country through the specialized depart- In addition, by conducting business, the contributes ment. company to the development of local communities through rightful tax payment and local job creation.\', "Foreign Exchange Risk Due to its global operations, Samsung SDS is exposed to foreign exchange risks which affect future business transactions, assets, may and liabilities. Samsung SDS the occurrence of foreign suppresses exchange positions by prioritizing local currency transactions and by matching deposit-withdrawal currency principle. If such position is unavoidable, it is managed in accordance with applicable statutes and procedures. The company\'s exchange risk management regulations include the definition, measurement cycle, managing body, and man- procedures of foreign exchange risk. Foreign exchange trans- agement actions are strictly limited and speculative transactions are prohibited. In addition, the company manages and reports foreign exchange risks on a monthly basis through a global exchange management system.", \'Credit Risk Credit risk arises when counterparty fails to comply with its obligations under the terms of the contract. To manage credit risks, Samsung SDS periodically evaluates the financial credibility of counterparties in con- sideration of factors such financial status and transaction histories, as and accordingly sets credit limits. Credit risk also can occur in financial product transactions with financial institutions. In order to reduce such risks, the company transacts only with banks with high international credit ratings under the approval and supervision of the Financial Man- agement Team.\', \'Risk Management According to External Evaluation Samsung SDS conducts risk management according to external ESG ratings. As Samsung SDS was rated as Medium Risk from the Sustain- ESG Risk Rating Medium Risk 20.1 -3.6 alytics ESG Risk Rating in 2021, the company is working to improve Updated 2021 Momentum HIGH SEVERE 20-30 30-40 management risks.\', \'Areas of Improvements Improvements Made Corporate Board Structure Risk management through the BOD and the Risk Management Council Governance Stakeholder Governance Disclosure of tax strategy, risk management, etc. Disclosure of the ESG goals within Sustainability Report Data Privacy Data privacy and Disclosure of policies related to information protection and information security and Security security policy Disclosure of information protection management system, process, and training status Disclosure of organizational and responding status to countermeasure information protection regulations in each country\', \'Discrimination Policy Diversity Programs\', "G\'D", \'Bribery Corruption Policy & Bribery & Corruption Programs Business Ethics Programs\', \'Disclosure of the current status of employee diversity-related training and related organizational operation status Announcement of no gender gap in compensation\', \'Operation of 24/7 whistle-blowing channel on the website in the languages of major countries where business is conducted Disclosure of whistle-blowing process Disclosure of the number of reports received through whistle-blowing channels and compliance/corruption guidelines violation cases Disclosure of the political neutrality policy specified in the Code of Conduct Guidelines on the website\']\n---\n', 'pagenum': 83, 'doc_name': 'userid_stuartcullinan_uploadfilename_16_samsung_sdspdf'}, 
    {'kpi': 'anti-corruption policies', 'score': 1.0, 'doc_org_std': 'Lenzing', 'topic': 'Anti-bribery Policies', 'summary': 'Prevent and report bribery and other forms of corruption', 'relevant quote from text': 'Anti-Bribery and Corruption Directive (ABC Directive), Local Guidance Document for the ABC Directive, Compliance trainings for employees, Compliance Register Tool (e.g. gifts and hospitality)', 'context': '## tablenum-0\n\n[\'nan\', \'nan_2\', [\'MANAGEMENT APPROACH\', nan], [\'Material topic: Business ethics\', nan], [\'Importance for Lenzing\', \'Guiding principles\'], [\'Compliance at the Lenzing Group not only stands for compliance\', \'Global Code of Business Conduct\'], [\'Compliance at the Lenzing Group not only stands for compliance\', \'Global Supplier Code of Conduct\'], [\'Compliance at the Lenzing Group not only stands for compliance\', \'Policy on Human Rights and Labor Standards\'], [\'Lenzing aims to deal honestly and with integrity in its behavior\', \'Modern Slavery Act Transparency Statement\'], [\'Lenzing aims to deal honestly and with integrity in its behavior\', \'Sustainability Policy\'], [nan, \'Quality Policy\'], [\'Opportunities\', \'Policy for Wood and Pulp\'], [\'Compliance through a shared culture of values\', \'Policy for Safety, Health and Environment (SHE)\'], [\'Preventive measures via whistleblowing\', \'Anti-Bribery and Corruption Directive (ABC Directive)\'], [\'Prevent retaliation against those who raise a concern\', \'Local Guidance Document for the ABC Directive\'], [\'Promote trust and confidence in business dealings\', \'(e.g. registration system for gifts/hospitality)\'], [\'Maintain corporate reputation\', \'Antitrust Directive\'], [\'Avoid conflicts of interest, misrepresentation, bias and negligence\', \'Whistleblower Directive\'], [\'Prevent and report bribery and other forms of corruption\', \'Issuer Compliance Directive\'], [nan, \'Anti Money Laundering Directive (AML Directive)\'], [\'Risks\', \'Know-How Protection Directive\'], [\'A constantly changing internal and external business environment\', nan], [\'Violation of fair and compliant business practices leading to\', \'Due diligence processes and (ongoing) measures\'], [\'reputational damage and resultant loss of public trust loss of clients and business partners\', \'Compliance with Lenzing Global Code of Business Conduct and internal group-wide directives\'], [\'value depreciation in the capital market\', \'Reporting incidents via BKMS® whistleblower system ("Tell us")\'], [nan, \'Following up procedure for reported incidents\'], [\'Non-compliance with laws, regulations and obligations\', "Transparent reporting within Lenzing\'s Corporate Governance Report"], [\'Costs and damage arising from involvement in bribery or breaches against (antitrust) law\', \'Leading by example: supervisors, leaders, and managers act as role models\'], [\'Fines, invalidity of contracts, claims for compensation from competitors and customers\', \'Compliance trainings for employees\'], [nan, \'Compliance Register Tool (e.g. gifts and hospitality)\'], [nan, \'Objectives\'], [nan, \'Finalization and publication of Compliance Organization\'], [nan, \'Continued penetration of compliance throughout the entire Lenzing Group\'], [nan, \'Enhancement of compliance training for employees\'], [nan, \'Short rule books for compliance issues (e.g. gifts and hospitality)\'], [nan, \'Introduction of a new Know Your Counterpart (KYC) system\']]\n---\n## text-segments\n\n[\'Business ethics\', \'Importance for Lenzing Compliance at the Lenzing Group not only stands for compliance with legal regulations and regulatory standards, but for the active responsibility of all employees and executives as well as a shared culture of values that are firmly anchored in the entire Group Lenzing aims to deal honestly and with integrity in its behavior towards business partners and shareholders\', \'Opportunities Compliance through a shared culture of values Preventive measures via whistleblowing Prevent retaliation against those who raise a concern Promote trust and confidence in business dealings Maintain corporate reputation Avoid conflicts of interest, misrepresentation, bias and negligence Prevent and report bribery and other forms of corruption\', \'Risks A constantly changing internal and external business environment Violation of fair and compliant business practices leading to reputational damage and resultant loss of public trust loss of clients and business partners value depreciation in the capital market Non-compliance with laws, regulations and obligations Costs and damage arising from involvement in bribery or breaches against (antitrust) law Fines, invalidity of contracts, claims for compensation from competitors and customers\', \'106 Sustainability Report 2021 Lenzing Group\', \'Guiding principles Global Code of Business Conduct\', \'Global Supplier Code of Conduct Policy on Human Rights and Labor Standards Modern Slavery Act Transparency Statement Sustainability Policy Quality Policy Policy for Wood and Pulp Policy for Safety, Health and Environment (SHE) Anti-Bribery and Corruption Directive (ABC Directive) Local Guidance Document for the ABC Directive (e.g. registration system for gifts/hospitality) Antitrust Directive\', \'Issuer Compliance Directive Anti Money Laundering Directive (AML Directive) Know-How Protection Directive\', \'Due diligence processes and (ongoing) measures Compliance with Lenzing Global Code of Business Conduct and internal group-wide directives Reporting incidents via BKMS® whistleblower system ("Tell us") Following up procedure for reported incidents Transparent reporting within Lenzing\\\'s Corporate Governance Report Leading by example: supervisors, leaders, and managers act as role models\', \'Compliance trainings for employees Compliance Register Tool (e.g. gifts and hospitality)\', \'Objectives Finalization and publication of Compliance Organization Continued penetration of compliance throughout the entire Lenzing Group Enhancement of compliance training for employees Short rule books for compliance issues (e.g. gifts and hospitality) Introduction of a new Know Your Counterpart (KYC) system\']\n---\n', 'pagenum': 105, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf'}, 
    {'kpi': 'anti-corruption policies', 'score': 1.0, 'doc_org_std': 'Kier Group plc', 'topic': 'Anti-Corruption', 'summary': 'Kier Group has a no-tolerance approach to all forms of bribery and corruption. They are committed to complying with all applicable anti-bribery and corruption laws.', 'relevant quote from text': '-Policy: Anti-Bribery and Corruption (including Gifts and Hospitality) Policy -Compliance: Require all third parties engaging with the Group to comply with the policy to ensure compliance with applicable laws', 'context': '## text-segments\n\n[\'Strategic report Non-financial information statement\', \'Non-financial information statement\', \'The table below summarises how we comply with non-financial performance reporting requirements and is produced to comply with sections 414CA and 414CB of the Companies Act 2006. All Kier Group policies are also available on the Kier website: https://www.kier.co.uk/investors/corporate-governance/group-policies/. Additional information necessary to understand our business and its Reporting Companies Act impact, policies, due diligence parameter requirement Kier governance Kier policy/standards processes and outcomes\', \'Anti-corruption Yes and anti-bribery\', \'70\', \'Employees\', \'Environmental matters\', \'Yes\', \'Yes\', "Risk Anti-Bribery and Corruption (including Gifts and Hospitality) Policy See www.kier.co.uk/investors/ Management Kier has a no tolerance approach to all forms of bribery and corruption and is corporate-governance/group- and Audit committed to complying with all applicable anti-bribery and corruption laws. In policies/ Committee addition to ensuring that our employees and contractors comply with the Anti-Bribery and Corruption Policy, we require all third parties engaging with a member of the Group to comply with this policy in order to ensure compliance with applicable anti-bribery and corruption laws and preserve our own and our customers\' reputations.", \'Kier Group plc Report and Accounts 2021\', \'Board\', \'Safety, Health and Environment Committee\', "Business Assurance Policy See www.kier.co.uk/investors/ Kier recognises the importance of achieving good standards of quality management corporate-governance/group- and quality control and the impact this has on the effectiveness and sustainability policies/ of our business. The Business Assurance Policy sets out the management systems and quality arrangements that Kier expects everyone working for it, or on its behalf, to comply with. Code of Conduct Kier is committed to developing a culture within the Group where everyone does the right thing and takes personal responsibility for their actions. The Code of Conduct sets out the standards of behaviour and business conduct expected from all employees and provides direction on a number of issues employees encounter in their day-to-day activities. Equality, Diversity and Inclusion Policy The Equality, Diversity and Inclusion Policy applies to all aspects of Kier\'s relationship with its employees and to relations between employees at all levels and covers recruitment, disability, development and training, bullying and harassment, victimisation and human rights. The policy also sets out details of the employee assistance programme which helps employees in enforcing the policy. Safety and Health Policy Statement Kier recognises its responsibility under health and safety legislation and ensures that all workplace risks are identified and mitigated to an acceptable level. Kier is committed to the provision of strong and active leadership, the engagement of the workforce in the promotion and achievement of safe and healthy conditions and the formal assessment and review of the Group\'s performance. The Safety and Health Policy Statement sets out how the Group identifies risks and mitigates them to an acceptable level. Whistleblowing Policy The Whistleblowing Policy encourages Kier\'s employees to report suspected wrongdoing, in the knowledge that their concerns will be taken seriously and investigated and that their confidentiality will be respected. Kier believes that a culture of openness and accountability is essential.", "Sustainability Policy See www.kier.co.uk/investors/ Kier is committed to preventing environmental and social harm and having a corporate-governance/group- positive impact on the communities in which it operates. Kier\'s Sustainability Policy policies/ sets out its ambitions for corporate social responsibility and environmental management and recognises that sustainable value creation is fundamental to business success. See pages 50 to 61"]\n---\n', 'pagenum': 70, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_6_kier-2021-ara-finalpdf'}, 
    {'kpi': 'anti-corruption policies', 'score': 1.0, 'doc_org_std': 'American International Group, Inc.', 'topic': 'Anti-Corruption Policies', 'summary': 'Apply to all employees and ensure that AIG business is conducted in compliance with all applicable anti-corruption laws and regulations in the U.S. and in other jurisdictions in which AIG operates or does business', 'relevant quote from text': '[\'ESG STRATEGY SUSTAINABLE SOLUTIONS AND INNOVATION\', \'GOVERNANCE\', "AIG\'s Global Anti-Corruption Policy and accompanying Global Anti-Corruption Standards and Due Diligence Procedures for Third Parties, issued by AIG\'s Corporate Compliance Group and approved by senior management:", \'Apply to all employees and ensure that AIG business is conducted in compliance with all applicable anti-corruption laws and regulations in the U.S. and in other jurisdictions in which AIG operates or does business\', "Set forth minimum requirements for employees to follow to ensure no bribery or corruption-related activities occur when employees directly or indirectly interact with U.S. and non-U.S. Government Officials, Other Persons and Third Parties acting on AIG\'s behalf", "AIG\'s Global Anti-Corruption Compliance Program includes the following elements:"]', 'context': '## tablenum-0\n\n[\'nan\', \'nan_2\', \'nan_3\', [\'AIG 2021 ESG REPORT\', \'Anti-Corruption\', "AIG\'s Global Anti-Corruption Compliance Program includes"], [\'LEADERSHIP MESSAGES\', "AIG\'s Global Anti-Corruption Policy and accompanying Global Anti-Corruption Standards and Due Diligence Procedures for Third Parties, issued by AIG\'s", "AIG\'s Global Anti-Corruption Compliance Program includes"], [\'EXECUTIVE SUMMARY\', "AIG\'s Global Anti-Corruption Policy and accompanying Global Anti-Corruption Standards and Due Diligence Procedures for Third Parties, issued by AIG\'s", "AIG\'s Global Anti-Corruption Compliance Program includes"], [\'PURPOSE AND VALUES\', "AIG\'s Global Anti-Corruption Policy and accompanying Global Anti-Corruption Standards and Due Diligence Procedures for Third Parties, issued by AIG\'s", "AIG\'s Global Anti-Corruption Compliance Program includes"], [\'OUR BUSINESS\', \'Apply to all employees and ensure that AIG business is conducted in\', \'An annual compliance risk assessment program\'], [\'ESG STRATEGY SUSTAINABLE SOLUTIONS\', \'Apply to all employees and ensure that AIG business is conducted in\', \'Periodic anti-corruption training for all AIG employees\'], [\'AND INNOVATION\', \'Set forth minimum requirements for employees to follow to ensure no\', \'Gift and entertainment reporting requirements\'], [\'ENVIRONMENTAL\', \'Set forth minimum requirements for employees to follow to ensure no\', \'Requirements related to the hiring of Government Officials\'], [\'SOCIAL\', \'Set forth minimum requirements for employees to follow to ensure no\', \'Requirements related to the hiring of Government Officials\'], [\'GOVERNANCE ESG Governance Structure\', \'Describe the roles and responsibilities of employees and Compliance as they relate to the Policy, including reporting violations, reviewing potential\', \'Records retention requirements\'], [\'GOVERNANCE ESG Governance Structure\', \'Describe the roles and responsibilities of employees and Compliance as they relate to the Policy, including reporting violations, reviewing potential\', nan], [\'Anti-Corruption Lobbying and Public Policy Cybersecurity and Data Privacy\', \'Outline potentially permissible expenditures and activities that may be allowed under certain circumstances, including gifts, meals and other hospitality for Government Officials, political contributions, charitable contributions, Government Officials as customers and Government\', nan], [\'ABOUT THIS REPORT\', \'Outline potentially permissible expenditures and activities that may be allowed under certain circumstances, including gifts, meals and other hospitality for Government Officials, political contributions, charitable contributions, Government Officials as customers and Government\', nan], [nan, "Require that all third parties that have, or may have, interactions with Government Officials or Government Entities on AIG\'s behalf undergo appropriate due diligence prior to being retained or doing business with AIG", nan]]\n---\n## text-segments\n\n[\'ESG STRATEGY\', \'SUSTAINABLE SOLUTIONS AND INNOVATION\', \'GOVERNANCE\', \'ESG Governance Structure Corporate Governance Business Ethics Anti-Corruption Lobbying and Public Policy Cybersecurity and Data Privacy\', "AIG\'s Global Anti-Corruption Policy and accompanying Global Anti-Corruption Standards and Due Diligence Procedures for Third Parties, issued by AIG\'s Corporate Compliance Group and approved by senior management:", \'Apply to all employees and ensure that AIG business is conducted in compliance with all applicable anti-corruption laws and regulations in the U.S. and in other jurisdictions in which AIG operates or does business\', "Set forth minimum requirements for employees to follow to ensure no bribery or corruption-related activities occur when employees directly or indirectly interact with U.S. and non-U.S. Government Officials, Other Persons and Third Parties acting on AIG\'s behalf", \'Describe the roles and responsibilities of employees and Compliance as they relate to the Policy, including reporting violations, reviewing potential issues and oversight of the program\', \'Outline potentially permissible expenditures and activities that may be allowed under certain circumstances, including gifts, meals and other hospitality for Government Officials, political contributions, charitable contributions, Government Officials as customers and Government Officials as employees\', "AIG\'s Global Anti-Corruption Compliance Program includes the following elements:", \'Requirements related to the hiring of Government Officials or relatives thereof\']\n---\n', 'pagenum': 106, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_srpdf'}, 
    {'kpi': 'anti-corruption policies', 'score': 1.0, 'doc_org_std': 'KIN +CARTA', 'topic': 'Anti-corruption policies', 'summary': 'Policies and practices in place to prevent and mitigate the risks of corruption in client and partner engagement.', 'relevant quote from text': 'See page 111 for information on our Anti-Bribery and Corruption Policy', 'context': '## text-segments\n\n[\'Data domain allowing us to provide end- to-end data transformation services to our clients to support their needs.\', "We also recognise that we must provide the highest level of advice, service and expertise to solve problems for our clients with resourcing models that meet our clients\' needs. Our Board, therefore, gives consideration to talent and the training and development of our people, allocating budget to these needs. In line with client demand for nearshore delivery models, our Board also considers acquisition and other expansion opportunities to increase nearshore delivery capabilities. During the year, the Board approved the establishment of nearshore delivery facilities in Greece and Colombia.", \'Promoting Responsible Business with clients our Key achievements with clients in 2021:\', \'In Europe, we increased the number of clients we engaged with on Designing with Empathy (accessibility) projects from a single client in 2019/2020 to seven engagements in 2020/2021 (see page 83 for more information on Designing with Empathy). Development of our sustainability proposition, including introductory conversations with clients.\', \'Introduction of a new role, Responsible Business Enablement Lead in the US, to focus on positive impact clients and projects.\', \'In addition to our positive impact initiatives, a core element of our promotion of Responsible Business with our clients is maintaining well-established practices, supported by our policies:\', \'E\', \'See page 111 for information on our Anti-Bribery and Corruption Policy\', \'IN\', \'See page 115 for information on our Code of Ethics\', \'E\', \'See page 113 for information on our Environmental and Social Risk Policy for Client and Partner Engagement\', "Development of Kin + Carta\'s sustainability proposition The development of sustainability client proposition is a one of our key strategic development areas within Responsible Business, given its alignment with our values, the moral imperative as a result of climate change, the market potential, and the opportunity for impact and craft leadership. Our goal is for our clients to receive a partnership with Kin + Carta that includes as standard ethical, sustainable practices for their business, people and the planet.", "We rising to this emerging opportunity through the are development of capabilities and tools to support people in our advising and acting in the most sustainable to meet client way outcomes throughout engagements. This includes Kin our + Carta undertaking assessment of the current sustainability an performance of clients\' digital assets and their aspirations in this This will allow to benchmark the organisation area. us to industry and policy standards, develop goals, understand the scope of opportunities, and distil objectives or targets for sustainability efforts. The assessment culminates in an action- ready plan and sustainability roadmap, which we will aim to fulfil with the client.", \'Back to contents\', \'Building a world that works better for everyone\', \'Strategic Report\', \'81\']\n---\n', 'pagenum': 82, 'doc_name': 'userid_stuartcullinan_uploadfilename_4_kim_cartapdfpdf'}
]
"""


# ## Save ranked data to the cloud

# ### write the data locally first
os.makedirs("/tmp/ranked-text", exist_ok=True)
df_text_ranked.to_csv(f"/tmp/ranked-text/df_text_ranked.csv", index=False)

# ### read data in bytes
df_text_ranked_bytes = utils.common.read_file_contents("/tmp/ranked-text")

# ### upload data
upload_resp = bg_sync.upload_data(
    contents=df_text_ranked_bytes['content'].tolist(),
    filenames=df_text_ranked_bytes['filename'].tolist(),
)

# ### Check uploaded quants data
df_text_ranked_uploaded = upload_resp.get_output()
df_text_ranked_uploaded = pd.DataFrame(df_text_ranked_uploaded)
text_ranked_file_path = df_text_ranked_uploaded['href'].tolist()[0]
"""
Now, we can access ranked quants data from, `text_ranked_file_path`
'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_df_text_rankedcsv/data_type=unstructured/format=csv/variable_desc=uploaded-document/source=stuartcullinan/df_text_rankedcsv.csv'
"""

# ### Test reading data from quants_ranked_file_path
saved_ranked_text = bg_sync.read_file(text_ranked_file_path).get_output()
saved_ranked_text = pd.DataFrame(saved_ranked_text)
"""
Number of rows in saved_ranked_quants, `len(saved_ranked_text)`: 194
Check that saved quants data has the same number of rows as the data before saving, 
`len(saved_ranked_text) == len(df_text_ranked)`: True
"""


# ## Check the first 5 rows for each company and KPI

## set columns to print

cols_to_print = [
    'kpi', 'score', 'rank', 'doc_org_std', 'topic', 'summary',
    'relevant quote from text'
]

# ### KPI = 'emissions-to-water'
kpi_mask = (df_text_ranked['kpi'] == 'anti-corruption policies')
score_mask = (df_text_ranked['score'] >= 0.75)
length_mask = (df_text_ranked['summary_len'] >= 75)
df_kpi = df_text_ranked[kpi_mask & score_mask & length_mask].groupby(
    by=['doc_org_std'],
    group_keys=False,
).apply(
    lambda x: x[cols_to_print].head(5)
).reset_index(drop=True).to_dict('records')
"""
`df_kpi` for 'anti-corruption policies'
[{'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'topic': 'Anti-corruption Policies',
  'summary': 'The Group has introduced or updated policies relating to the environment, health & safety, conflicts of interest and modern slavery and human trafficking. Employees have refreshed their knowledge of existing policies and procedures, including interactive training on anti-bribery and corruption.',
  'relevant quote from text': '"We have either introduced new or updated existing policies relating to the environment, health & safety, conflicts of interest and modern slavery and human trafficking in recent months and all employees across the Group have continued to refresh their knowledge of existing policies and procedures this included interactive training on anti bribery and corruption in the second half of 2020."'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'RAVEN PROPERTY GROUP LIMITED',
  'topic': 'Anti-bribery Policies',
  'summary': 'The Group has conducted interactive training on anti-bribery and corruption in the second half of 2020, as part of the rolling program of training and development. Employees have refreshed their knowledge of existing policies and procedures.',
  'relevant quote from text': '"A rolling programme of training and development to maintain awareness of all policies is underway, using the Group\'s intranet, which serves as the conduit to circulate Group communications and news stories on a daily basis. This included interactive training on anti bribery and corruption in the second half of 2020."'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'topic': 'Anti-corruption policies',
  'summary': 'The Ethics & Corporate Responsibility Committee oversees the completion of internal investigations and the findings from an external review of the sanctions compliance framework',
  'relevant quote from text': 'Reviewed the findings and actions from the annual Be Heard survey; Monitored the effectiveness of the Third-Party Sales Representatives Policy; Monitored the effectiveness of the Speaking Up Policy and procedures; Reviewed the findings from an external review of the sanctions compliance framework and the implementation of measures to further strengthen the framework'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'topic': 'Anti-bribery policies',
  'summary': 'The Committee reviewed the effectiveness of the policy and procedures designed to manage the risk of the facilitation of tax evasion',
  'relevant quote from text': 'Reviewed the effectiveness of the policy and procedures designed to manage the risk of the facilitation of tax evasion; Review the effectiveness of the anti-bribery and corruption compliance framework; Engage with external advisers on emerging ethical risks relevant to the business; Conduct a review of the policies which fall under the remit of the Committee.'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'KIN +CARTA',
  'topic': 'Anti-corruption policies',
  'summary': 'Policies and practices in place to prevent and mitigate the risks of corruption in client and partner engagement.',
  'relevant quote from text': 'See page 111 for information on our Anti-Bribery and Corruption Policy'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'KIN +CARTA',
  'topic': 'Anti-bribery policies',
  'summary': 'Policies and practices in place to prevent and mitigate the risks of bribery in client and partner engagement.',
  'relevant quote from text': 'See page 111 for information on our Anti-Bribery and Corruption Policy'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Samsung SDS',
  'topic': 'Anti-Corruption Policies',
  'summary': 'Disclosure of business principle guidelines, anti-corruption policy, and fair competition policy on the website',
  'relevant quote from text': "['Business Ethics', 'Bribery & Corruption Policy Bribery & Corruption Programs Business Ethics Programs']"},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American International Group, Inc.',
  'topic': 'Anti-Corruption Policies',
  'summary': 'Apply to all employees and ensure that AIG business is conducted in compliance with all applicable anti-corruption laws and regulations in the U.S. and in other jurisdictions in which AIG operates or does business',
  'relevant quote from text': '[\'ESG STRATEGY SUSTAINABLE SOLUTIONS AND INNOVATION\', \'GOVERNANCE\', "AIG\'s Global Anti-Corruption Policy and accompanying Global Anti-Corruption Standards and Due Diligence Procedures for Third Parties, issued by AIG\'s Corporate Compliance Group and approved by senior management:", \'Apply to all employees and ensure that AIG business is conducted in compliance with all applicable anti-corruption laws and regulations in the U.S. and in other jurisdictions in which AIG operates or does business\', "Set forth minimum requirements for employees to follow to ensure no bribery or corruption-related activities occur when employees directly or indirectly interact with U.S. and non-U.S. Government Officials, Other Persons and Third Parties acting on AIG\'s behalf", "AIG\'s Global Anti-Corruption Compliance Program includes the following elements:"]'},
 {'kpi': 'anti-corruption policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Allianz Group',
  'topic': 'Anti-corruption policies',
  'summary': 'Allianz takes a zero-tolerance approach to fraud and corruption. They comply with local and international anti-corruption and anti-bribery laws. The Allianz Anti-Corruption Program sets standards for a group-wide approach in every jurisdiction. Anti-Corruption training is compulsory for all employees.',
  'relevant quote from text': '"Zero tolerance of fraud and corruption Allianz takes a zero-tolerance approach to fraud and corruption. We are committed to complying fully with local and international anti-corruption and anti-bribery laws. Our aim is to go beyond complying with the minimum standards of the law, such that the Allianz Anti-Corruption Program sets standards for consistent and comprehensive a group-wide approach in jurisdiction. every The program requires that employees and certain third parties with whom Allianz does business prohibited from offering, accepting, paying are authorizing bribe other form of or any or any corruption, be it with the private sector with or government officials. Anti-Corruption training is compulsory for all employees with online and classroom training delivered in multiple languages. To ensure online trainings achieve acceptable levels of understanding and they include awareness, mandatory test component that must be a completed and passed for the training to be deemed to have been completed by the employee. In 2022, will introduce Anti-we Corruption training KPIs in line with the World Economic Forum (WEF) requirements. No material violations of corruption laws corresponding official proceedings or were reported to Group in 2021 which would have been required to be disclosed to Allianz Group\'s Audit Committee."'},
 {'kpi': 'anti-corruption policies', 'score': 0.96, 'rank': 2.0, 'doc_org_std': 'VINCI',
  'topic': 'Anti-corruption policies',
  'summary': 'Measures to prevent corruption and policies implemented with regard to tax matters',
  'relevant quote from text': 'Initiatives to prevent corruption Measures in place to prevent corruption Policies implemented with regard to tax matters'},
 {'kpi': 'anti-corruption policies', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Albioma', 'topic': 'Anti-corruption',
  'summary': 'The Group implemented an ethics and compliance program to combat corruption and influence peddling.',
  'relevant quote from text': "['The implementation, since end-2018, of an ethics and compliance programme, including in particular bringing the Group into compliance with the new provisions of the Sapin 2 Act concerning efforts to combat corruption and influence peddling, led to the distribution of an initial framework to Group employees in the form of a Code of Ethics incorporating principles of conduct pertaining to corruption and influence peddling, and to launch awareness sessions for Group employees on these matters. The Group also has a whistleblower mechanism and appointed an internal Ethics and Professional Conduct Lead responsible for these matters.']"},
 {'kpi': 'anti-corruption policies', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'DEG Deutsche EuroShop',
  'topic': 'Anti-bribery policies',
  'summary': 'Processes to ensure that conflicts of interest are avoided and managed in the highest governance body and how they are reported',
  'relevant quote from text': "['Process for managing conflicts of interest', 'Gov-COI.', 'Narrative description', 'Processes to ensure that conflicts of interest are avoided and managed in the highest governance body and how they are reported', 'Corporate Governance Section (p. 26)']"},
 {'kpi': 'anti-corruption policies', 'score': 0.925, 'rank': 4.0, 'doc_org_std': 'VINCI',
  'topic': 'Anti-corruption Policies',
  'summary': '-VINCI has an Anti-corruption Code of Conduct that employees are expected to follow<br>-VINCI has a whistleblowing procedure for reporting infringements of rules and commitments<br>-Business ethics-related risks are assessed and ranked<br>-Accounting controls are in place to prevent corruption<br>-Internal audits are conducted to ensure the effectiveness of anti-corruption arrangements',
  'relevant quote from text': "['VINCI\\'s anti-corruption arrangements', 'Anti-corruption Code of Conduct', 'Whistleblowing procedure', 'Risk assessments', 'Accounting controls and audits']"},
 {'kpi': 'anti-corruption policies', 'score': 0.9, 'rank': 5.0, 'doc_org_std': 'Lenzing',
  'topic': 'Anti-Corruption Policies',
  'summary': 'The Anti-Bribery and Corruption Directive ("ABC Directive") supplements Lenzing\'s Global Code of Business Conduct by providing global minimum standards to ensure that Lenzing\'s activities are conducted ethically and with integrity.',
  'relevant quote from text': 'Anti-Bribery and Corruption Directive (ABC Directive).'},
 {'kpi': 'anti-corruption policies', 'score': 0.9, 'rank': 5.0, 'doc_org_std': 'American International Group, Inc.',
  'topic': 'Anti-Corruption Policies',
  'summary': 'Businesses should work against corruption in all its forms, including extortion and bribery.',
  'relevant quote from text': "['Principle 10', 'Businesses should work against corruption in all its forms, including extortion and bribery.', 'See the Anti-Corruption section of this report']"},
 {'kpi': 'anti-corruption policies', 'score': 0.9, 'rank': 5.0, 'doc_org_std': 'BillerudKorsnäs',
  'topic': 'Anti-corruption policies',
  'summary': "BillerudKorsnäs has a Group-wide Code of Conduct based on international standards regarding human rights, working conditions, environment, and anti-corruption. The Code of Conduct is integrated into the company's policy framework and provides basic guidelines on responsible business practices. The company conducts risk assessments and implements measures to address corruption risks.",
  'relevant quote from text': "['Responsible business', 'BillerudKorsnäs seeks to act responsibly...', 'Target 2021 Outcome...', 'Our Code of Conduct is partly based on international standards...', 'Responsible Business Compliance programme...', 'Follow-up of risk assessment and action plans...']"},
 {'kpi': 'anti-corruption policies', 'score': 0.9, 'rank': 5.0, 'doc_org_std': 'Aviva plc',
  'topic': 'Anti-Bribery Policies',
  'summary': "Aviva has a risk management framework that includes policies and standards to prevent, detect, and report financial crime, including instances of bribery and corruption. Aviva provides risk-based training to employees and others acting on their behalf to manage bribery and corruption risks. The Chief Risk Officer provides regular reporting on financial crime matters, including Aviva's anti-bribery and anti-corruption program, to the Customer, Conduct and Reputation Committee (CCRC). Aviva conducts due diligence when recruiting and engaging external partners. They expect their registered suppliers to abide by their Third Party Business Code of Behavior, which outlines how Aviva behaves in dealings with others and provides guidance on financial crime laws and regulations. Aviva's own Third Party Business Code of Behavior applies to their interactions with each other and includes guidance on financial crime laws and regulations.",
  'relevant quote from text': "Aviva uses the Financial Crime Business Standard and supporting Minimum Compliance Standards to guide their risk-based financial crime programs, which include anti-bribery and anti-corruption measures (Source: Input Data Segment 1). Aviva provides risk-based training to employees and others acting on their behalf to manage bribery and corruption risks (Source: Input Data Segment 2). The Chief Risk Officer provides regular reporting on financial crime matters, including Aviva's anti-bribery and anti-corruption program, to the Customer, Conduct and Reputation Committee (CCRC) (Source: Input Data Segment 2). Aviva conducts due diligence when recruiting and engaging external partners (Source: Input Data Segment 2). 99% of Aviva's UK, Canada, and Ireland registered suppliers have agreed to abide by their Third Party Business Code of Behavior or provided satisfactory reasons why they didn't (Source: Input Data Segment 2). Aviva's Third Party Business Code of Behavior outlines their behavior in dealings with each other and includes guidance on financial crime laws and regulations (Source: Input Data Segment 2)."},
 {'kpi': 'anti-corruption policies', 'score': 0.857, 'rank': 6.0, 'doc_org_std': 'ARKEMA',
  'topic': 'Anti-corruption Policies',
  'summary': 'The Group has a Code of Conduct and Anti-Corruption Policy that sets out best business practices expected of all employees. It covers areas such as bribery prevention, compliance with antitrust laws, and adherence to import and export regulations. The policy includes definitions of corruption and influence peddling, provides examples of behaviors to avoid, and outlines rules for gifts and hospitality.',
  'relevant quote from text': '\'4.6.2.1 The Code of Conduct and Anti-Corruption Policy The Group\\\'s Business Conduct and Ethics Code (also known simply as the "Code of Conduct"), which includes the Anti-Corruption Policy sets out Arkema\\\'s best business practices expected of all employees at all times. The Code of Conduct covers the following main points: employees must not offer, provide or accept, directly or indirectly, any undue advantage, be it pecuniary or otherwise.\''},
 {'kpi': 'anti-corruption policies', 'score': 0.85, 'rank': 7.0, 'doc_org_std': 'UPM',
  'topic': 'Anti-corruption Policies',
  'summary': "Corruption related risks are identified and assessed in connection with the company's risk management process.",
  'relevant quote from text': 'KEY PERFORMANCE INDICATOR: 100% coverage of participating in UPM Code of Conduct training (continuous)'},
 {'kpi': 'anti-corruption policies', 'score': 0.8, 'rank': 8.0, 'doc_org_std': 'Albioma',
  'topic': 'Anti-corruption policies',
  'summary': 'The Group has implemented an ethics and compliance programme to minimize its exposure to acts of corruption and influence peddling. A specific map of the corruption risk was produced in 2020.',
  'relevant quote from text': '["Preventing and combating corruption", "In this context, and in order to comply with the French law of 9 December 2016 relating to transparency, combating corruption and modernising economic life, referred to as the "Sapin 2" law, Albioma began to implement an ethics and compliance programme at the end of 2018...", "Ethics and compliance programme", "A specific map of the corruption risk was produced in 2020...", "The Group\'s Code of Ethics, which-in accordance with the provisions of the Sapin 2 Act is accompanied by whist-...", "A training plan on combating corruption is currently being rolled out..."]'},
 {'kpi': 'anti-corruption policies', 'score': 0.8, 'rank': 8.0, 'doc_org_std': 'BillerudKorsnäs',
  'topic': 'Anti-corruption Policies',
  'summary': 'BillerudKorsnäs conducts continuous training initiatives on responsible business with a set training plan. E-learning in anti-corruption was completed by about 130 persons in 2021. Classroom training on responsible business was carried out for more than 220 employees.',
  'relevant quote from text': 'Employees and sales agents are trained regularly in line with a set training plan to mitigate higher risks in responsible business areas. In 2021, about 130 persons completed e-learning in anti-corruption. Adapted classroom training on responsible business was carried out for more than 220 employees.'},
 {'kpi': 'anti-corruption policies', 'score': 0.8, 'rank': 8.0, 'doc_org_std': 'Aviva plc',
  'topic': 'Anti-Corruption Policies',
  'summary': 'Aviva has a zero-tolerance approach to acts of bribery and corruption. They have a risk management framework that sets out policies and standards across all markets. These policies and standards apply to Aviva, and it is the responsibility of CEOs (or equivalent) to ensure that their business operates in line with them. Aviva uses the Financial Crime Business Standard and supporting Minimum Compliance Standards to guide their risk-based financial crime programs. The programs seek to prevent, detect, and report financial crime, including instances of bribery and corruption, while complying with relevant legislation and regulation. Aviva provides risk-based training to employees and others acting on their behalf to ensure they know what is expected of them and how to manage bribery and corruption risks. Aviva has a Speak Up helpline, which makes it easy to report any concerns in confidence, with all reports referred to an independent investigation team.',
  'relevant quote from text': 'In April 2021, Aviva attained the Good Shopping Guide Ethical Company Award and was recognized by the Good Business Charter. They offer 69 green or accessible products and services to enable customers to be more environmentally responsible or have easier access to protection (Source: Input Data Segment 1). The Aviva Business Ethics Code 2021 outlines the high standards of ethical behavior expected by Aviva. All employees are required to read and sign-up to the Code every year (99.6% of employees did so in 2021) (Source: Input Data Segment 1). Aviva has a risk management framework and a zero-tolerance approach to acts of bribery and corruption. They have policies and standards across all markets, and CEOs (or equivalent) are responsible for ensuring their business operates in line with them (Source: Input Data Segment 1). Aviva uses the Financial Crime Business Standard and supporting Minimum Compliance Standards to guide their risk-based financial crime programs, which include anti-bribery and anti-corruption measures (Source: Input Data Segment 1). Aviva provides risk-based training to employees and others acting on their behalf to manage bribery and corruption risks (Source: Input Data Segment 2). Aviva has a Speak Up helpline for reporting concerns in confidence, and an independent investigation team handles all reports (Source: Input Data Segment 2).'},
 {'kpi': 'anti-corruption policies', 'score': 0.75, 'rank': 9.0,
  'doc_org_std': 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'topic': 'Anti-corruption policies',
  'summary': 'The Group resolutely opposes illegal operations such as bribery, extortion, fraud, and money laundering. They strictly abide by national and regional anti-corruption laws and regulations. They have internal policies to regulate professional behaviors and ethics of employees. During the reporting period, no cases related to bribery, extortion, fraud, or money laundering were involved.',
  'relevant quote from text': '"4.7 Anti-corruption 4.7.1 General", "We also formulated internal policies, such as Notice on the Duties of the Printing Department (Compilation) and the Provisions on Employees\' Compensation and Penalty, Notice on Prohibition of Accepting Gifts, Gift Money and Flowers and Honesty and Self-discipline Commitment of the Faculty, in order to regulate the professional behaviors and professional ethics of all employees of the Group, to establish a good atmosphere of integrity and diligence and to prevent frauds.", "During the reporting period, the Group and our employees did not involve any cases related to bribery, extortion, fraud, or money laundering."'}
  ]
"""


# ### KPI = 'emissions-to-water'
kpi_mask = (df_text_ranked['kpi'] == 'anti-bribery policies')
score_mask = (df_text_ranked['score'] >= 0.75)
length_mask = (df_text_ranked['summary_len'] >= 75)
df_kpi = df_text_ranked[kpi_mask & score_mask & length_mask].groupby(
    by=['doc_org_std'],
    group_keys=False,
).apply(
    lambda x: x[cols_to_print].head(5)
).reset_index(drop=True).to_dict('records')
"""
`df_kpi` for 'anti-bribery policies'
[{'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'WEBUILD',
  'topic': 'Anti-corruption policies',
  'summary': 'Webuild has a zero tolerance policy for all types of corruption and is committed to complying with the anti-corruption laws ruling in the countries where it operates. It requires its stakeholders to act with honesty and integrity at all times. The Company never condones behaviour designed to improperly influence the decisions taken by representatives of public or private bodies.',
  'relevant quote from text': 'Prevention and monitoring systems in line with the most stringent international standards Internal policies Webuild has an Anti-corruption System which meets the ISO 37001 requirements and is certified by an independent certification body. The Company is committed to adopting preventive protocols to minimise the risk of corruption and to ensure compliance with the principles introduced by anti-corruption laws and international best practices. These principles are enshrined in its Code of Ethics and reiterated in its Anti-corruption Policy, adopted voluntarily and in compliance with international best practices.'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG', 'topic': 'Anti-Bribery Policies',
  'summary': 'We have identified our key corruption risks and developed corresponding procedures to mitigate them and continued to reinforce our anti-corruption approach at Group level in compliance with Sapin Il law...',
  'relevant quote from text': 'The Compliance Department, with the BLs and Finance teams, Internal Control, Group Internal Audit and Enterprise Risk Management (ERM) have worked very closely to review, update and release our anti-corruption risk matrix validated by the Executive Leadership team and the Audit and Risk Management Committee. Since 2021, we have been working with an external advisor to migrate this matrix onto a digitalized format (software application) with the aim of facilitating the reporting to the Management and follow-up of our action plans, while confirming our compliance with Sapin Il requirements. In this process, we have updated and reviewed all potential corruption scenarios identified, in collaboration with the relevant departments/functions. The full update of this version will be new finalized in 2022. In 2021, trainings/workshops have been organized with employees most exposed to corruption risks to confirm their awareness and additional sessions are planned for 2022 to cover all functions.'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'American Express',
  'topic': 'Anti-Corruption Policies',
  'summary': 'We have a zero-tolerance policy for bribery. All colleagues must complete anti-corruption and bribery training tied to their annual compensation.',
  'relevant quote from text': "'71 OPERATING RESPONSIBLY', 'Key Policies', 'We have a zero-tolerance policy for bribery. All colleagues must complete anti-corruption and bribery training tied to their annual compensation that includes information on applicable laws and regulations for their location.'"},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'SCGG', 'topic': 'Anti-corruption Policies',
  'summary': 'The company has implemented measures such as general awareness and targeted training, a Business Code of Conduct, a secure and confidential reporting process, due diligence screening processes, periodic audits of policies, and compliance with relevant laws to manage legal, regulatory, and non-compliance risks. These measures aim to prevent violations of codes and laws, including the Foreign Corrupt Practices Act of 1977.',
  'relevant quote from text': 'Our internal controls, operational support procedures, and employee training are focused on understanding and complying with applicable restrictions and obligations. We conduct business in countries with government corruption and have implemented a Business Code of Conduct. There are measures in place to manage legal, regulatory, and non-compliance risks, including training, secure reporting process, due diligence screening, and compliance with relevant laws and codes.'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'topic': 'Anti-Corruption Policies',
  'summary': 'The company has an Anti-bribery and Corruption policy in place. Employees are required to comply with the policy and report any concerns or breaches.',
  'relevant quote from text': 'Anti-bribery and Corruption'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'WEBUILD',
  'topic': 'Anti-corruption Policies',
  'summary': 'The Code of Ethics and the Company\'s commitment to anti-corruption principles, based on the fundamental tenet of "zero tolerance".',
  'relevant quote from text': '"Anti-corruption: the anti-corruption principles to be adhered to by employees, based on the fundamental tenet of \'zero tolerance\'."'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'KIN +CARTA',
  'topic': 'Anti-corruption policies',
  'summary': 'Sets out standards in areas, such as the prohibition of facilitation payments, and political donations and minimum standards in relation to charitable donations, and gifts and entertainment.',
  'relevant quote from text': "[['Anti-Bribery and Corruption', 'Sets out standards in areas, such as the prohibition of facilitation payments, and political donations and minimum standards in relation to charitable donations, and gifts and entertainment.', 'Issued Group-wide with recipients required to confirm they acknowledge and understand the policy. Senior management team is responsible for implementing standards and enforcing them throughout the Group. Furthermore, senior managers respond to an internal controls questionnaire that includes questions on engagements with politically exposed people and client jurisdictions. This is reviewed by the Internal Audit function on an annual basis. 2021 annual review found all businesses within Kin + Carta to be deemed low risk.', nan]]"},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Savills plc',
  'topic': 'Anti-Bribery Policies',
  'summary': 'Savills has a zero tolerance approach to bribery and other forms of corruption. Our Code of Conduct sets out our commitment to operate responsibly wherever we work in the world and to work professionally, fairly, and with integrity.',
  'relevant quote from text': 'Savills has a zero tolerance approach to bribery and other forms of corruption. Our Code of Conduct sets out our commitment to operate responsibly wherever we work in the world, to work professionally, fairly and with integrity and to engage with our stakeholders to manage the social, environmental and ethical impact of our activities in the different markets in which we operate. We empower and support our employees to always make the right decisions consistent with this policy.'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
  'topic': 'Anti-bribery policies',
  'summary': 'Mondi has a zero tolerance policy for bribery and corruption and has a Business Integrity Policy defining ethical business practices.',
  'relevant quote from text': '["Code of business ethics Mondi\'s code of business ethics is based on a system of voluntary codes comprising the following five principles: legal compliance; honesty and integrity; human rights; stakeholders; and sustainability. Application of the code is documented in Mondi\'s policies and procedures-in particular the Business Integrity Policy which defines our values and key principles of ethical business practices, along with Mondi\'s zero tolerance of bribery and corruption."]'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Mondi Group',
  'topic': 'Anti-corruption policies',
  'summary': 'Mondi has a code of business ethics based on voluntary codes, including principles of legal compliance, honesty and integrity, and sustainability.',
  'relevant quote from text': '["Our due diligence processes ensure alignment between practices and our policies... and externally certified standards at operational and Group level.", "Code of business ethics Mondi\'s code of business ethics is based on a system of voluntary codes comprising the following five principles: legal compliance; honesty and integrity; human rights; stakeholders; and sustainability."]'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Kier Group plc',
  'topic': 'Anti-Corruption',
  'summary': 'Kier Group has a no-tolerance approach to all forms of bribery and corruption. They are committed to complying with all applicable anti-bribery and corruption laws.',
  'relevant quote from text': '-Policy: Anti-Bribery and Corruption (including Gifts and Hospitality) Policy -Compliance: Require all third parties engaging with the Group to comply with the policy to ensure compliance with applicable laws'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Air New Zealand',
  'topic': 'Anti-bribery policies',
  'summary': "Bribery is the offer, promise or giving of anything of value in order to improperly influence a person's actions or decisions.",
  'relevant quote from text': "['3.2 Zero Tolerance approach to Bribery 3.2.1 Bribery is the offer, promise or giving of anything of value in order to improperly influence a person\\'s actions or decisions to gain or retain a business benefit.']"},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Aggreko plc',
  'topic': 'Anti-bribery Policies',
  'summary': 'Aggreko has anti-bribery policies in place. The company complies with the non-financial reporting requirements contained in sections 414CA and 414CB of the Companies Act 2006.',
  'relevant quote from text': "['Non-financial information statement We comply with the non-financial Business model description reporting requirements contained 08 Our business model in sections 414CA and 414CB of the Risk Companies Act 2006.']"},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'Admiral Group plc',
  'topic': 'Anti-corruption Policies',
  'summary': 'Admiral has a General Standards of Conduct policy that outlines conduct standards for all colleagues. They also have a Whistleblowing policy to raise concerns about malpractice or wrongdoing.',
  'relevant quote from text': 'General Standards of Conduct Health and Safety Procurement and Outsourcing Anti-Bribery Gifts and Gratuities Whistleblowing Financial Crime Anti-Slavery, Exploitation and Human Trafficking Policies'},
 {'kpi': 'anti-bribery policies', 'score': 1.0, 'rank': 1.0, 'doc_org_std': 'ACCOR', 'topic': 'Anti-corruption',
  'summary': 'Accor, as a major economic player, operates in 110 countries and must consistently apply the highest ethical standards in its operations.',
  'relevant quote from text': "'As a major economic player, Accor operates in 110 countries, interacting with many established economic and public partners. It has been expanding its activities in the digital world for several years. An industry leader, it must consistently apply the highest ethical standards in its operations.'"},
 {'kpi': 'anti-bribery policies', 'score': 0.958, 'rank': 2.0,
  'doc_org_std': 'Adani Ports and Special Economic Zone Limited', 'topic': 'Anti-corruption',
  'summary': 'Businesses should work against corruption in all its forms including extortion and bribery',
  'relevant quote from text': "['Principle 10 Businesses should work against corruption in all its forms including extortion and bribery', '102-16 Values principles standards, and norms of behavior 205-1 Operations assessed for risks related to corruption forms including extortion bribery 205-2 Communication and training about anti-corruption policies and and procedures 205-3 Confirmed incidents of corruption and actions taken', 'Principle 10 Businesses should work against corruption in all its forms including extortion and bribery', '102-16 Values principles standards, and norms of behavior Principle 10 Businesses should work against corruption in all its forms including extortion bribery 205-1 Operations assessed for risks related to corruption forms including extortion bribery 205-2 Communication and training about anti-corruption policies and and procedures 205-3 Confirmed incidents of corruption and actions taken']"},
 {'kpi': 'anti-bribery policies', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'topic': 'Anti-Bribery Policies',
  'summary': 'Policies and guidelines that prevent bribery and unethical business practices',
  'relevant quote from text': '-Code of Business Conduct (Code) includes commitment to deal honestly and ethically, and to abide by anti-bribery and anti-corruption laws'},
 {'kpi': 'anti-bribery policies', 'score': 0.95, 'rank': 3.0, 'doc_org_std': 'Arch Capital Group Ltd.',
  'topic': 'Anti-Corruption Policies',
  'summary': 'Policies and guidelines that ensure compliance with laws against corruption',
  'relevant quote from text': '-Code of Business Conduct (Code) includes commitment to deal honestly and ethically, and to abide by anti-bribery and anti-corruption laws'},
 {'kpi': 'anti-bribery policies', 'score': 0.917, 'rank': 4.0, 'doc_org_std': 'UPM',
  'topic': 'Anti-corruption Policies',
  'summary': 'UPM has a zero-tolerance attitude towards corruption and bribery. It has an Anti-Corruption Code of Conduct and conducts regular risk assessments.',
  'relevant quote from text': 'UPM Code of Conduct, UPM Anti-Corruption Rules, due diligence procedures for suppliers and third parties, risk assessments'},
 {'kpi': 'anti-bribery policies', 'score': 0.908, 'rank': 5.0, 'doc_org_std': 'ABB', 'topic': 'Anti-Bribery Policies',
  'summary': 'New procedures addressing both joint integrity and human rights associated with third party management',
  'relevant quote from text': "['New procedures addressing both joint integrity and human rights associated with third party management']"},
 {'kpi': 'anti-bribery policies', 'score': 0.9, 'rank': 6.0, 'doc_org_std': 'THE a2 MILK COMPANY LIMITED',
  'topic': 'Anti-corruption policies',
  'summary': 'The company aims to operate safely, ethically, and in compliance with anti-corruption laws.',
  'relevant quote from text': 'The company embeds safety, ethics, and compliance systems across the supply chain to operate safely and ethically, including in compliance with all local requirements, including anti-bribery and anti-corruption laws.'},
 {'kpi': 'anti-bribery policies', 'score': 0.9, 'rank': 6.0, 'doc_org_std': 'THE a2 MILK COMPANY LIMITED',
  'topic': 'Anti-bribery policies',
  'summary': 'The company aims to operate safely, ethically, and in compliance with anti-bribery laws.',
  'relevant quote from text': 'The company embeds safety, ethics, and compliance systems across the supply chain to operate safely and ethically, including in compliance with all local requirements, including anti-bribery and anti-corruption laws.'},
 {'kpi': 'anti-bribery policies', 'score': 0.9, 'rank': 6.0, 'doc_org_std': 'Kier Group plc', 'topic': 'Anti-Bribery',
  'summary': 'Kier Group has a no-tolerance approach to all forms of bribery and corruption. They comply with anti-bribery and corruption laws.',
  'relevant quote from text': '-Policy: Anti-Bribery and Corruption (including Gifts and Hospitality) Policy -Compliance: Require all third parties engaging with the Group to comply with the policy to ensure compliance with applicable laws'},
 {'kpi': 'anti-bribery policies', 'score': 0.9, 'rank': 6.0, 'doc_org_std': 'ABB', 'topic': 'Anti-bribery policies',
  'summary': 'Description of policies and practices for prevention of bribery and total amount of monetary losses as a result of legal proceedings associated with bribery',
  'relevant quote from text': "['a. Integrity b. and C. Please refer to the company's financial disclosures available at Q4&FY 2021: Financial information (abb.com)']"},
 {'kpi': 'anti-bribery policies', 'score': 0.9, 'rank': 6.0, 'doc_org_std': 'Ledlenser',
  'topic': 'Anti-corruption Policies',
  'summary': 'A new Groupwide department for business ethics and compliance was set up in 2019. It is tasked with ensuring all operations are conducted in compliance with relevant legislation, instructions, and internal policies.',
  'relevant quote from text': 'Ethics and compliance A new Groupwide department for business ethics and compliance was set up in 2019. It is tasked with ensuring all operations are conducted in compliance with relevant legislation, instructions and internal policies.'},
 {'kpi': 'anti-bribery policies', 'score': 0.875, 'rank': 7.0, 'doc_org_std': 'SCGG', 'topic': 'Anti-bribery Policies',
  'summary': 'The company has implemented measures such as general awareness and targeted training, a Business Code of Conduct, a secure and confidential reporting process, due diligence screening processes, periodic audits of policies, and compliance with relevant laws to manage legal, regulatory, and non-compliance risks. These measures aim to prevent violations of codes and laws, including anti-bribery and corruption risks.',
  'relevant quote from text': 'Our internal controls, operational support procedures, and employee training are focused on understanding and complying with applicable restrictions and obligations. We conduct business in countries with government corruption and have implemented a Business Code of Conduct. There are measures in place to manage legal, regulatory, and non-compliance risks, including training, secure reporting process, due diligence screening, and compliance with relevant laws and codes.'},
 {'kpi': 'anti-bribery policies', 'score': 0.8571428571428571, 'rank': 8.0, 'doc_org_std': 'Ashtead Group plc',
  'topic': 'Anti-corruption',
  'summary': 'Regular business ethics training is provided to senior employees to ensure awareness of obligations',
  'relevant quote from text': '["ETHICS TRAINING Senior employees across the Group receive regular business ethics training...with regard to...the UK Bribery Act and, enabled us to embed responsibility...Anti-corruption and bribery policies are maintained and reviewed...on a regular basis", "Our whistle-blowing arrangements allow employees, in confidence,...and web intake."]'},
 {'kpi': 'anti-bribery policies', 'score': 0.85, 'rank': 9.0, 'doc_org_std': 'WEBUILD',
  'topic': 'Anti-corruption policies',
  'summary': 'Webuild adopts a "zero tolerance" policy towards any form of corruption, such as bribery (public, private, active and passive) and the granting of improper advantages. It undertakes to always comply with anticorruption regulations in force in the countries in which it operates. It asks all of its stakeholders to always act with honesty and integrity. Behaviour aimed at improperly influencing decisions of representatives of the Public Administration or private entities is prohibited.',
  'relevant quote from text': 'Webuild adopts a "zero tolerance" policy towards any form of corruption, such as bribery and improper advantages. It complies with anticorruption regulations and expects stakeholders to act with honesty and integrity.'},
 {'kpi': 'anti-bribery policies', 'score': 0.833, 'rank': 10.0, 'doc_org_std': 'UPM', 'topic': 'Anti-bribery Policies',
  'summary': 'UPM requires due diligence before entering into or renewing contracts with third parties. It includes anti-bribery terms in agreements and conducts audits.',
  'relevant quote from text': 'Due diligence procedures, inclusion of anti-bribery contract terms, audits of third parties'},
 {'kpi': 'anti-bribery policies', 'score': 0.8, 'rank': 11.0, 'doc_org_std': 'CHINA EDUCATION GROUP HOLDINGS LIMITED',
  'topic': 'Anti-corruption policies',
  'summary': 'The company provided anti-corruption training to directors and employees during the reporting period. Training included warning film education, professional lectures, and regular training on internal audit. New recruits are required to receive anti-corruption education and training. "Honesty and integrity" is assessed in the employee annual performance appraisal.',
  'relevant quote from text': '-4.7.3 Description of anti-corruption training provided to directors and staff-Anti-corruption training provided-Warning film education, professional lecture delivered by professionals-New recruits are required to receive anti-corruption education and training-Conduct regular training on internal audit-"Honesty and integrity" is one of a compulsory assessment components in the employee annual performance appraisal'},
 {'kpi': 'anti-bribery policies', 'score': 0.8, 'rank': 11.0, 'doc_org_std': 'Air New Zealand',
  'topic': 'Anti-Bribery Policies',
  'summary': 'The company has a policy in place to prevent bribery. Employees are prohibited from making facilitation payments or kickbacks and must maintain proper records.',
  'relevant quote from text': 'Anti-bribery and Corruption, Record Keeping and Internal Controls, Training, Definitions, Roles and Responsibilities, Compliance Policies'},
 {'kpi': 'anti-bribery policies', 'score': 0.8, 'rank': 11.0, 'doc_org_std': 'Ledlenser',
  'topic': 'Anti-bribery Policies',
  'summary': 'Boliden has developed processes and procedures to make sure the necessary measures are in place, and has also trained personnel in order to increase awareness of how to behave to combat bribery and corruption.',
  'relevant quote from text': 'Anti-money laundering Bribery and corruption A new policy has been drawn up to prevent Boliden has developed processes and money laundering and the financing of procedures to make sure the necessary terrorism. to combat bribery and corruption.'}
]
"""

