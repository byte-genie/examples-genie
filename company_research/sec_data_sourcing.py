# # Source company data from SEC filings

import pandas as pd
from utils.byte_genie import ByteGenie

# ## init byte-genie
bg = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    verbose=1,
)

# ## Set company ID
company_id = {'cik': '320193'}

# ## query company info
company_info_payload = bg.create_api_payload(
    func='query_company_data',
    args={
        'company_id': company_id,
        'source': 'sec',
        'data_type': 'company-info',
    },
    task_mode='sync',
    overwrite=1,
)
company_info_resp = bg.call_api(
    payload=company_info_payload,
)
company_info = company_info_resp.get_output()
"""
Company info is dictionary/json, with following keys, `list(company_info.keys())`
[
    'addresses', 'category', 'cik', 'description', 'ein', 'entityType', 'exchanges', 'fiscalYearEnd', 'flags',
    'formerNames', 'insiderTransactionForIssuerExists', 'insiderTransactionForOwnerExists', 'investorWebsite', 'name',
    'phone', 'sic', 'sicDescription', 'stateOfIncorporation', 'stateOfIncorporationDescription', 'tickers', 'website'
]
"""

# ## query company filings
"""
<p>
When querying company filings, we can also specify any filters to retrieve only filings matching those filters.
</p>
"""

company_filings_payload = bg.create_api_payload(
    func='query_company_data',
    args={
        'company_id': company_id,
        'source': 'sec',
        'data_type': 'filings',
        'filters': {
            "form": ['10-K'],
            'isXBLR': [1]
        },
    },
    task_mode='sync',
)
company_filings_resp = bg.call_api(
    payload=company_filings_payload,
)
df_company_filings = company_filings_resp.get_output()
df_company_filings = pd.DataFrame(df_company_filings)
"""
Company filings data contain the following columns, `list(df_company_filings.columns)`
[
    'acceptance_datetime', 'accession_number', 'act', 'file_number', 'filing_date', 'film_number', 'form',
    'is_inline_xbrl', 'is_xbrl', 'primary_doc_description', 'primary_document', 'report_date', 'report_year', 'size'
]
Here is a sample of company filings data, `df_company_filings.head().to_dict('records')`
[
    {'acceptance_datetime': '2012-10-31T17:07:19.000Z', 'accession_number': '0001193125-12-444068', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2012-10-31', 'film_number': '121171452', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FORM 10-K', 'primary_document': 'd411355d10k.htm',
     'report_date': '2012-09-29', 'report_year': 2012.0, 'size': 9859755},
    {'acceptance_datetime': '2011-10-26T16:35:25.000Z', 'accession_number': '0001193125-11-282113', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2011-10-26', 'film_number': '111159350', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FORM 10-K', 'primary_document': 'd220209d10k.htm',
     'report_date': '2011-09-24', 'report_year': 2011.0, 'size': 9435428},
    {'acceptance_datetime': '2010-10-27T16:36:21.000Z', 'accession_number': '0001193125-10-238044', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2010-10-27', 'film_number': '101145250', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FOR THE FISCAL YEAR ENDED SEPTEMBER 25, 2010',
     'primary_document': 'd10k.htm', 'report_date': '2010-09-25', 'report_year': 2010.0, 'size': 13856661},
    {'acceptance_datetime': '2009-10-27T16:18:29.000Z', 'accession_number': '0001193125-09-214859', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2009-10-27', 'film_number': '091139493', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FOR THE FISCAL YEAR ENDED SEPTEMBER 26, 2009',
     'primary_document': 'd10k.htm', 'report_date': '2009-09-26', 'report_year': 2009.0, 'size': 3638340},
    {'acceptance_datetime': '2008-11-05T06:16:23.000Z', 'accession_number': '0001193125-08-224958', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2008-11-05', 'film_number': '081162315', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 0, 'primary_doc_description': 'FORM 10-K', 'primary_document': 'd10k.htm',
     'report_date': '2008-09-27', 'report_year': 2008.0, 'size': 1188704}
]
"""


# ## query company facts
"""
<p>
When querying company facts, we can also specify any filters to retrieve only filings matching those filters.
</p>
"""

company_facts_payload = bg.create_api_payload(
    func='query_company_data',
    args={
        'company_id': company_id,
        'source': 'sec',
        'data_type': 'filings',
        'filters': {
            "form": ['10-K'],
        },
    },
    task_mode='sync',
)
company_facts_resp = bg.call_api(
    payload=company_facts_payload,
)
df_company_facts = company_facts_resp.get_output()
df_company_facts = pd.DataFrame(df_company_facts)
"""
Company facts data have these columns, `list(df_company_facts.columns)`
[
    'acceptance_datetime', 'accession_number', 'act', 'file_number', 'filing_date', 'film_number', 'form',
    'is_inline_xbrl', 'is_xbrl', 'primary_doc_description', 'primary_document', 'report_date', 'report_year', 'size'
]
Here is a sample of company facts data, `df_company_facts.head().to_dict('records')`
[
    {'acceptance_datetime': '2012-10-31T17:07:19.000Z', 'accession_number': '0001193125-12-444068', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2012-10-31', 'film_number': '121171452', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FORM 10-K', 'primary_document': 'd411355d10k.htm',
     'report_date': '2012-09-29', 'report_year': 2012.0, 'size': 9859755},
    {'acceptance_datetime': '2011-10-26T16:35:25.000Z', 'accession_number': '0001193125-11-282113', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2011-10-26', 'film_number': '111159350', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FORM 10-K', 'primary_document': 'd220209d10k.htm',
     'report_date': '2011-09-24', 'report_year': 2011.0, 'size': 9435428},
    {'acceptance_datetime': '2010-10-27T16:36:21.000Z', 'accession_number': '0001193125-10-238044', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2010-10-27', 'film_number': '101145250', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FOR THE FISCAL YEAR ENDED SEPTEMBER 25, 2010',
     'primary_document': 'd10k.htm', 'report_date': '2010-09-25', 'report_year': 2010.0, 'size': 13856661},
    {'acceptance_datetime': '2009-10-27T16:18:29.000Z', 'accession_number': '0001193125-09-214859', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2009-10-27', 'film_number': '091139493', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 1, 'primary_doc_description': 'FOR THE FISCAL YEAR ENDED SEPTEMBER 26, 2009',
     'primary_document': 'd10k.htm', 'report_date': '2009-09-26', 'report_year': 2009.0, 'size': 3638340},
    {'acceptance_datetime': '2008-11-05T06:16:23.000Z', 'accession_number': '0001193125-08-224958', 'act': '34',
     'file_number': '000-10030', 'filing_date': '2008-11-05', 'film_number': '081162315', 'form': '10-K',
     'is_inline_xbrl': 0, 'is_xbrl': 0, 'primary_doc_description': 'FORM 10-K', 'primary_document': 'd10k.htm',
     'report_date': '2008-09-27', 'report_year': 2008.0, 'size': 1188704}
]
"""

# ## Next steps
"""
<p>
Once we have the relevant company data from SEC filings, we can vectorise this data, and
store this data in a PostgreSQL or graph database, to be able to use this data as part of a 
retrieval-augmented generation (RAG) solution, which we will cover in a separate example.
</p>
"""