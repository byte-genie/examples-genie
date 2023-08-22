# # Process documents to create structured data files

# ## import necessary libraries
import time
import uuid
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

# ## set inputs

# ### set file containing documents
docs_file = 'gs://db-genie/entity_type=api-tasks/entity=593a5370f106bf174d115e2fc2c2a3c9/data_type=structured/format=pickle/variable_desc=download_documents/source=api-genie/593a5370f106bf174d115e2fc2c2a3c9.pickle'

# ## read sourced documents
resp = bg_sync.read_file(docs_file)
df_reports = bg_sync.get_response_data(resp)
df_reports = pd.DataFrame(df_reports)

# ### get doc names (slugified urls)
doc_names = [
    bg_sync.get_response_data(bg_sync.slugify(href))
    for href in df_reports['href'].unique().tolist()
]

# ### add doc_names to df_reports
df_reports['doc_name'] = doc_names

# ## extract document info
resp = {}
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"{doc_num}/{len(doc_names)}: {doc_name}")
    resp[doc_name] = bg_async.extract_doc_info(
        doc_name=doc_name,
    )

# ### wait for output to be ready
time.sleep(15 * 60)

# ### get output files for doc_info
doc_info_files = [
    bg_sync.get_response_output_file(resp[doc_name])
    for doc_name in resp.keys()
]

# ### loop over all doc_info_files and read the ones that exist
missing_doc_info_files = []
df_doc_info = pd.DataFrame()
for file_num, doc_file in enumerate(doc_info_files):
    logger.info(f"{file_num}/{len(doc_info_files)}: {doc_file}")
    ## check if file exists
    doc_file_exists = bg_sync.get_response_data(
        bg_sync.check_file_exists(doc_file)
    )
    ## if file exists
    if doc_file_exists:
        ## get doc_info data
        df_doc_info_ = \
            bg_sync.get_response_data(
                bg_sync.read_file(doc_file)
            )
        ## convert to df
        df_doc_info_ = pd.DataFrame(df_doc_info_)
        ## append to df_doc_info
        df_doc_info = pd.concat([df_doc_info, df_doc_info_])
    ## if the doc_file does not yet exist
    else:
        ## add it to missing_doc_info_files
        missing_doc_info_files = missing_doc_info_files + [doc_file]

# ### check df_doc_info
list(df_doc_info.columns)

"""
list(df_doc_info.columns)
['doc_name', 'doc_org', 'doc_type', 'doc_year', 'num_pages']
"""

df_doc_info.tail().to_dict('records')
"""
df_doc_info.tail().to_dict('records')
[{'doc_name': 'httpswwwultratechcementcomcontentdamultratechcementwebsitepdfbiodiversity-assesssment-mapping-with-cdsbpdf', 'doc_org': 'Climate Disclosure Standards Board (CDSB)', 'doc_type': "['sustainability report']", 'doc_year': 2023.0, 'num_pages': 8.0}, {'doc_name': 'httpswwwultratechcementcomcontentdamultratechcementwebsitepdfsustainability-reportsalternatives_in_action-ultratech_sustainability_reportpdf', 'doc_org': 'Sustainability Report 2010', 'doc_type': "['sustainability report']", 'doc_year': 2011.0, 'num_pages': 48.0}, {'doc_name': 'httpswwwultratechcementcomcontentdamultratechcementwebsitepdffinancialsinvestor-updateslb-report-june-2023pdf', 'doc_org': nan, 'doc_type': "['sustainability report']", 'doc_year': 2023.0, 'num_pages': 10.0}, {'doc_name': 'httpswwwultratechcementcomcontentdamultratechcementwebsitepdffinancialsannual-reportsannual-report-single-viewpdf', 'doc_org': 'Mr. Aditya Vikram Birla', 'doc_type': "['annual report']", 'doc_year': 2022.0, 'num_pages': 362.0}, {'doc_name': 'httpswwwultratechcementcomcontentdamultratechcementwebsitepdfsustainability-reportsucl_sr2010-12_gricontentindexpdf', 'doc_org': 'the document was published by ultratech cement', 'doc_type': "['annual report']", 'doc_year': 2012.0, 'num_pages': 8.0}]
"""

# ## Post-process document info

# ### convert doc_year to int
df_doc_info['doc_year'] = [
    bg_sync.get_response_data(
        bg_sync.parse_numeric_string(
            text=text,
        )
    ) if str(text) != 'nan' else text
    for text in df_doc_info['doc_year']
]

#  ### check doc_year
df_doc_info['doc_year'].unique().tolist()
"""
[nan, 2022.0, 2020.0, 2023.0, 2018.0, 2021.0, 2019.0, 2013.0, 2015.0, 2009.0, 2003.0, 2010.0, 2000.0, 2001.0, 2002.0, 2008.0, 2004.0, 2016.0, 2011.0, 2012.0, 1995.0, 2017.0, 1990.0, 2014.0, 2018.5]
"""

# ### merge df_doc_info on df_reports
df_reports = pd.merge(
    left=df_reports,
    right=df_doc_info,
    on=['doc_name'],
    how='left',
)

# ## Save data with document info

# ### save df_reports
reports_file = f'gs://db-genie/entity_type=file/entity=reports_file/reports.pickle'
resp = \
    bg_sync.call_api(
        bg_sync.create_api_payload(
            func='write_to_file',
            args={
                'content': df_reports.fillna(''),
                'file': reports_file
            }
        )
    )
df_reports.to_csv(f"/tmp/reports_data_2023-08-21.csv", index=False)

# ### read reports from saved file
df_reports = pd.read_csv(f"/tmp/reports_data_2023-08-21.csv")

# ## Document selection

# ### filter documents from 2021 onward
df_reports = df_reports[df_reports['doc_year'].map(float) > 2021].reset_index(drop=True)

# ### select first 5 documents
doc_names = df_reports['doc_name'].tolist()[:5]

# ## Document processing

# ### trigger processing for a couple of documents
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"Running quant-structuring pipeline for: ({doc_num}/{len(doc_names)}) {doc_name}")
    resp = bg_async.structure_quants_pipeline(
        doc_name=doc_name
    )

# ## Next Steps
# ### The document processing for doc_names has been triggered now.
# ### We can wait for the output to be ready.
# ### Once the output is ready, we can move on to extract relevant quantiative/qualitative data.
# ### See company_research/data_extraction.py for an example of data extraction.
