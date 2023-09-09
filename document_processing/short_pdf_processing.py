# # Short-form PDF processing (e.g. invoices, bills)

import os
import time
import base64
import pandas as pd
import utils.common
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

# ## read local files to process

# ### Specify the directory containing files to process
directory = f"/Users/majid/Dropbox/startup/ESGenie/PoCs/Temus/short_pdf_processing"

# ### read file contents in a directory
df_contents = utils.common.read_file_contents(directory)

# ### upload files
resp = bg_sync.upload_data(
    contents=df_contents['content'].tolist(),
    filenames=df_contents['filename'].tolist(),
    username=bg_sync.read_username()
)

# ### get uploaded data
df_uploads = pd.DataFrame(resp.get_data())

# ### check uploaded document names
logger.info(f"uploaded document names: {df_uploads['doc_name'].unique().tolist()}")
"""
df_uploads['doc_name'].unique().tolist()
['userid_demo-genie_uploadfilename_renewal-of-hydrant-hosespdf', 'userid_demo-genie_uploadfilename_aircon-servicingpdf', 'userid_demo-genie_uploadfilename_repair-of-vehiclespdf', 'userid_demo-genie_uploadfilename_utility-billspdf', 'userid_demo-genie_uploadfilename_purchase-of-material-geocom-engineeringpdf']
Notice that all the uploaded documents will have the username attached to them. 
Hence, two users from the same organisation cannot overwrite each other's uploads, unless they are using the same account.
"""


# ## process documents

# ### get document names
doc_names = df_uploads['doc_name'].unique().tolist()

# ### trigger text and table extraction from documents
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering text translation pipeline for ({doc_num}/{len(doc_names)}): {doc_name}")
    ## trigger text translation pipeline in async (background) mode, as it is a long-running task
    resp = bg_async.translate_text_pipeline(
        doc_name=doc_name
    )
    responses = responses + [resp]

# ### wait for the output to be ready

# ## Read output

# ### list extracted original table files
table_files = []
missing_doc_names = []
for resp_num, resp in enumerate(responses):
    logger.info(f"processing response ({resp_num}/{len(responses)}")
    ## get doc_name
    doc_name = resp.response['response']['task_1']['task']['args']['doc_name']
    ## list original table files
    table_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern=f"variable_desc=orig-table/**.csv"
    ).get_data()
    if table_files_:
        table_files = table_files + table_files_
    else:
        missing_doc_names = missing_doc_names + [doc_name]

# ### documents with missing table files
logger.info(f"documents with missing table files: {missing_doc_names}")

# ### read table files
tables_dict = {}
for file_num, file in enumerate(table_files):
    logger.info(f"reading table file ({file_num}/{len(table_files)}): {file}")
    filename = file.split('/')[-1]
    table = bg_sync.read_file(file).get_data()
    tables_dict[filename] = table

# ### check tables
logger.info(f"filenames for tables: {list(tables_dict.keys())}")
"""
list(tables_dict.keys())
['renewal-of-hydrant-hosespdf_pagenum-0_table-cells_trans_orig-table_tablenum-0.csv', 'renewal-of-hydrant-hosespdf_pagenum-0_table-cells_trans_orig-table_tablenum-1.csv', 'renewal-of-hydrant-hosespdf_pagenum-0_table-cells_trans_orig-table_tablenum-2.csv', 'renewal-of-hydrant-hosespdf_pagenum-0_table-cells_trans_orig-table_tablenum-3.csv', 'renewal-of-hydrant-hosespdf_pagenum-1_table-cells_trans_orig-table_tablenum-0.csv', 'renewal-of-hydrant-hosespdf_pagenum-1_table-cells_trans_orig-table_tablenum-1.csv', 'renewal-of-hydrant-hosespdf_pagenum-1_table-cells_trans_orig-table_tablenum-2.csv', 'renewal-of-hydrant-hosespdf_pagenum-2_table-cells_trans_orig-table_tablenum-0.csv', 'renewal-of-hydrant-hosespdf_pagenum-3_table-cells_trans_orig-table_tablenum-0.csv', 'renewal-of-hydrant-hosespdf_pagenum-3_table-cells_trans_orig-table_tablenum-1.csv', 'renewal-of-hydrant-hosespdf_pagenum-4_table-cells_trans_orig-table_tablenum-0.csv', 'repair-of-vehiclespdf_pagenum-0_table-cells_trans_orig-table_tablenum-0.csv', 'repair-of-vehiclespdf_pagenum-0_table-cells_trans_orig-table_tablenum-1.csv', 'repair-of-vehiclespdf_pagenum-0_table-cells_trans_orig-table_tablenum-2.csv', 'repair-of-vehiclespdf_pagenum-0_table-cells_trans_orig-table_tablenum-3.csv', 'repair-of-vehiclespdf_pagenum-1_table-cells_trans_orig-table_tablenum-0.csv', 'repair-of-vehiclespdf_pagenum-1_table-cells_trans_orig-table_tablenum-1.csv', 'repair-of-vehiclespdf_pagenum-1_table-cells_trans_orig-table_tablenum-2.csv', 'repair-of-vehiclespdf_pagenum-1_table-cells_trans_orig-table_tablenum-3.csv', 'repair-of-vehiclespdf_pagenum-1_table-cells_trans_orig-table_tablenum-4.csv', 'repair-of-vehiclespdf_pagenum-2_table-cells_trans_orig-table_tablenum-0.csv', 'repair-of-vehiclespdf_pagenum-3_table-cells_trans_orig-table_tablenum-0.csv', 'repair-of-vehiclespdf_pagenum-3_table-cells_trans_orig-table_tablenum-1.csv', 'repair-of-vehiclespdf_pagenum-3_table-cells_trans_orig-table_tablenum-2.csv', 'repair-of-vehiclespdf_pagenum-3_table-cells_trans_orig-table_tablenum-3.csv', 'repair-of-vehiclespdf_pagenum-3_table-cells_trans_orig-table_tablenum-4.csv', 'repair-of-vehiclespdf_pagenum-4_table-cells_trans_orig-table_tablenum-0.csv', 'repair-of-vehiclespdf_pagenum-4_table-cells_trans_orig-table_tablenum-1.csv', 'utility-billspdf_pagenum-0_table-cells_trans_orig-table_tablenum-0.csv', 'utility-billspdf_pagenum-0_table-cells_trans_orig-table_tablenum-1.csv', 'utility-billspdf_pagenum-0_table-cells_trans_orig-table_tablenum-2.csv', 'utility-billspdf_pagenum-1_table-cells_trans_orig-table_tablenum-0.csv', 'utility-billspdf_pagenum-1_table-cells_trans_orig-table_tablenum-1.csv', 'utility-billspdf_pagenum-1_table-cells_trans_orig-table_tablenum-2.csv', 'utility-billspdf_pagenum-1_table-cells_trans_orig-table_tablenum-3.csv', 'utility-billspdf_pagenum-2_table-cells_trans_orig-table_tablenum-0.csv', 'purchase-of-material-geocom-engineeringpdf_pagenum-0_table-cells_trans_orig-table_tablenum-0.csv', 'purchase-of-material-geocom-engineeringpdf_pagenum-0_table-cells_trans_orig-table_tablenum-1.csv', 'purchase-of-material-geocom-engineeringpdf_pagenum-0_table-cells_trans_orig-table_tablenum-2.csv', 'purchase-of-material-geocom-engineeringpdf_pagenum-1_table-cells_trans_orig-table_tablenum-0.csv', 'purchase-of-material-geocom-engineeringpdf_pagenum-1_table-cells_trans_orig-table_tablenum-1.csv']
Notice that all extracted pages are indexed by page number and table number within the page.
tables_dict[list(tables_dict.keys())[0]]
[{'nan': 'Date :', 'nan_2': '09/12/2015'}, {'nan': 'Cheque No :', 'nan_2': '4509'}, {'nan': 'Bank Code :', 'nan_2': '7302'}, {'nan': 'Account No :', 'nan_2': '04060352312'}, {'nan': 'Voucher No :', 'nan_2': '2015/16966'}]
tables_dict[list(tables_dict.keys())[1]]
[{'A/C Category': '', 'Acct Code': '', 'Amount': 115.81, 'Description': 'Being payment of below invoice for renew of hyd hose'}, {'A/C Category': 'Total Amount :', 'Acct Code': 'Total Amount :', 'Amount': 115.81, 'Description': 'Total Amount :'}, {'A/C Category': '', 'Acct Code': 'Total Tax :', 'Amount': 8.11, 'Description': ''}, {'A/C Category': '', 'Acct Code': 'Grand Total :', 'Amount': 123.92, 'Description': ''}]
tables_dict[list(tables_dict.keys())[17]]
[{'Amount (SGD)': 405.0, 'Description': 'SD5 12V COMPRESSOR 6321', 'GST': '7% SR', 'Quantity': 1.0, 'Rate': 405.0}, {'Amount (SGD)': '', 'Description': 'RECEIVER DRIER', 'GST': '7% SR', 'Quantity': 1.0, 'Rate': ''}, {'Amount (SGD)': '', 'Description': 'TOP UP COMPRESSOR OIL', 'GST': '7% SR', 'Quantity': 1.0, 'Rate': ''}, {'Amount (SGD)': '', 'Description': 'CHARGING GAS', 'GST': '7% SR', 'Quantity': 1.0, 'Rate': ''}, {'Amount (SGD)': '', 'Description': 'LABOUR CHARGES', 'GST': '7% SR', 'Quantity': 1.0, 'Rate': ''}, {'Amount (SGD)': '', 'Description': '', 'GST': '', 'Quantity': '', 'Rate': ''}]
tables_dict[list(tables_dict.keys())[31]]
[{'Amount ($)': '1,324.58', 'SUMMARY OF CHARGES 21 Oct 2015 to 19 Nov 2015': 'Balance B/F from Previous Bill'}, {'Amount ($)': '-1,324.58', 'SUMMARY OF CHARGES 21 Oct 2015 to 19 Nov 2015': 'Payment on 06-11-2015- Thank You'}, {'Amount ($)': '0.00', 'SUMMARY OF CHARGES 21 Oct 2015 to 19 Nov 2015': 'Outstanding Balance'}, {'Amount ($)': '8,399.53', 'SUMMARY OF CHARGES 21 Oct 2015 to 19 Nov 2015': 'Total Current Charges due on 15 Dec 2015 (Tue)'}, {'Amount ($)': '$8,399.53', 'SUMMARY OF CHARGES 21 Oct 2015 to 19 Nov 2015': 'Total Amount Payable'}]
"""
