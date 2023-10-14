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
## sort data by kpi, and score
df_text_ranked = df_text_ranked.sort_values(['kpi', 'score'], ascending=False).reset_index(drop=True)
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

"""
Here is the complete text data ranked by KPIs, `df_text_ranked[['kpi', 'score', 'doc_org_std', 'topic', 'summary', 'relevant quote from text', 'context', 'pagenum', 'doc_name']].to_dict('records')`
"""