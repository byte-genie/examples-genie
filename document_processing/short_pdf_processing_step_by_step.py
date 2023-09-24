# # Short-form PDF processing (step by step approach)
"""
This example shows an example of processing pdf documents using a step by step approach, starting from running OCR, retrieving OCR results for text and tables, text segmentation, and ranking/filtering data
"""


import io
import time
import base64
import pandas as pd
from PIL import Image
import utils.async_utils
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


# ## select documents to process
doc_names = [
    'userid_demo-genie_uploadfilename_renewal-of-hydrant-hosespdf',
    'userid_demo-genie_uploadfilename_aircon-servicingpdf',
    'userid_demo-genie_uploadfilename_repair-of-vehiclespdf',
    'userid_demo-genie_uploadfilename_utility-billspdf',
    'userid_demo-genie_uploadfilename_purchase-of-material-geocom-engineeringpdf',
    'userid_demo-genie_uploadfilename_works_costpdf',
]
"""
See document_processing/upload_files.py (.ipynb) or data_management/upload_files.py (.ipynb) to see how to upload documents 
"""

# ## Extract page images

# ### extract page images from documents
tasks = [
    bg_async.async_write_pdf_img(
        doc_name=doc_name
    )
    for doc_num, doc_name in enumerate(doc_names)
]
write_img_responses = utils.async_utils.run_async_tasks(tasks)


# ### list extracted page images
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"*.png",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
img_files = utils.async_utils.run_async_tasks(tasks)
"""
image files for the last document, img_files[-1].get_data()
[
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_purchase-of-material-geocom-engineeringpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/purchase-of-material-geocom-engineeringpdf_pagenum-0.png', 
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_purchase-of-material-geocom-engineeringpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/purchase-of-material-geocom-engineeringpdf_pagenum-1.png'
]
"""

# ## Extract text from documents

# ### Run OCR on page images
ocr_start_time = time.time()
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering OCR for ({doc_num}/{len(doc_names)}: {doc_name})")
    resp = bg_async.extract_text(
        doc_name=doc_name
    )
    responses = responses + [resp]

# ### list OCR output files for text
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="variable_desc=text-blocks/**.csv"
    )
    for doc_name in doc_names
]
ocr_text_files = utils.async_utils.run_async_tasks(tasks)
ocr_text_files = [resp.get_data() for resp in ocr_text_files if resp.get_data() is not None]
"""
Number of documents with OCR text files, len(ocr_text_files): 5
Number of OCR text files for one document, len(ocr_text_files[-1]): 2
OCR text files for one document: ocr_text_files[-1]
[
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/works_costpdf_pagenum-0.png', 
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/works_costpdf_pagenum-1.png'
]
"""

# ### list OCR output files for tables
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="variable_desc=table-cells/**.csv"
    )
    for doc_name in doc_names
]
ocr_table_files = utils.async_utils.run_async_tasks(tasks)
ocr_table_files = [resp.get_data() for resp in ocr_table_files if resp.get_data() is not None]
"""
Number of documents with OCR table output files, len(ocr_table_files): 5
Number of OCR table files for one document, len(ocr_table_files[-1]): 2
ocr_table_files[-1]
[
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/works_costpdf_pagenum-0_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/works_costpdf_pagenum-1_table-cells.csv'
]
"""

# ## Format OCR table output

# ### Format OCR extracted tables
"""
OCR extracted tables return tables in a standardised (row, col, cell) format, and require an additional layer of processing to reconstruct the original table
"""

# ### trigger original table reconstruction
"""
OCR output generates table-cells files, which we will now use as an input to reconstruct original tables
"""
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering original table reconstruction for ({doc_num}/{len(doc_names)}: {doc_name})")
    resp = bg_async.reconstruct_orig_tables(
        doc_name=doc_name,
        file_pattern='variable_desc=table-cells/**.csv',
    )
    responses = responses + [resp]

# ### list original table files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=orig-table/**.csv',
    )
    for doc_name in doc_names
]
orig_table_files = utils.async_utils.run_async_tasks(tasks)
orig_table_files = [resp.get_data() for resp in orig_table_files]
"""
number of documents with original table files, len(orig_table_files): 6
original table files for one document, orig_table_files[-1]
[
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/works_costpdf_pagenum-0_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/works_costpdf_pagenum-1_table-cells_orig-table_tablenum-0.csv'
]
"""

# ## Process extracted tables

# ### set files to read
table_file = 'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_works_costpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/works_costpdf_pagenum-0_table-cells_orig-table_tablenum-0.csv'

# ### read a table file
df_table = bg_sync.read_file(table_file).get_data()
df_table = pd.DataFrame(df_table)

# ### check table data
logger.info(f"{len(df_table)} rows in df_table")
"""
list(df_table.columns)
['Description', 'Description_2', 'Item', 'Phase 1 Amount (S$)', 'Phase 2A & 2B Amount (S$)', 'Unit']
df_table.head().to_dict('records')
[
    {'Description': "Depressed Road and Lay-by (Cont'd)", 'Description_2': "Depressed Road and Lay-by (Cont'd)", 'Item': '', 'Phase 1 Amount (S$)': '', 'Phase 2A & 2B Amount (S$)': '', 'Unit': ''}, 
    {'Description': 'Painting; to parapet wall; in accordance to CAAS MOAS requirement', 'Description_2': '$ 9.40 / m2', 'Item': 'A', 'Phase 1 Amount (S$)': '100,828.00', 'Phase 2A & 2B Amount (S$)': '246,988.00', 'Unit': 'Item'}, 
    {'Description': 'Vehicular grating in hot dipped galvanised mild steel; 800 X 100mm thick; including all necessary fittings, fixings and accessories', 'Description_2': '$ 450.00 / m', 'Item': 'B', 'Phase 1 Amount (S$)': '283,500.00', 'Phase 2A & 2B Amount (S$)': '1,705,050.00', 'Unit': 'Item'}, 
    {'Description': 'Precast concrete plank; 650 X 75mm thick', 'Description_2': '$ 40.02 / m', 'Item': 'C', 'Phase 1 Amount (S$)': '11,205.60', 'Phase 2A & 2B Amount (S$)': '101,090.52', 'Unit': 'Item'}, {'Description': 'Precast concrete plank; 1150 X 75mm thick', 'Description_2': '$ 73.88 / m', 'Item': 'D', 'Phase 1 Amount (S$)': '20,686.40', 'Phase 2A & 2B Amount (S$)': '186,620.88', 'Unit': 'Item'}
]
"""

# ### re-structure table data
resp = bg_async.create_dataset(
    data=df_table.to_dict('records'),
    attrs=['product category', 'complete product description', 'key material used in the product',
           'per unit amount', 'phase 1 amount', 'phase 2 amount', 'currency']
)
if resp.check_output_file_exists():
    df_dataset = resp.read_output_data()
    df_dataset = pd.DataFrame(df_dataset)
else:
    logger.info(f"create_dataset() output is not yet ready: wait some more")

# ### pivot df_dataset
df_dataset_wide = df_dataset.pivot(
    index=['context', 'row_num'],
    columns='variable',
    values='value',
).reset_index()

# ### check df_dataset_wide
logger.info(f"shape of df_dataset_wide: {df_dataset_wide.shape}")
"""
list(df_dataset_wide.columns)
['context', 'row_num', 'complete product description', 'currency', 'key material used in the product', 'per unit amount', 'phase 1 amount', 'phase 2 amount', 'product category', 'relevant quote']
df_dataset_wide.drop(columns=['context', 'row_num', 'relevant quote']).head().to_dict('records')
[
    {'complete product description': 'Painting; to parapet wall; in accordance to CAAS MOAS requirement', 'currency': 'n/a', 'key material used in the product': 'n/a', 'per unit amount': '$ 9.40 / m2', 'phase 1 amount': '$ 100,828.00', 'phase 2 amount': '$ 246,988.00', 'product category': 'A'}, 
    {'complete product description': 'Vehicular grating in hot dipped galvanised mild steel; 800 X 100mm thick; including all necessary fittings', 'currency': 'n/a', 'key material used in the product': 'hot dipped galvanised mild steel', 'per unit amount': '$ 450.00 / m', 'phase 1 amount': '$ 283,500.00', 'phase 2 amount': '$ 1,705,050.00', 'product category': 'B'}, 
    {'complete product description': 'Precast concrete plank; 650 X 75mm thick', 'currency': 'n/a', 'key material used in the product': 'n/a', 'per unit amount': '$ 40.02 / m', 'phase 1 amount': '$ 11,205.60', 'phase 2 amount': '$ 101,090.52', 'product category': 'C'}, 
    {'complete product description': 'Precast concrete plank; 1150 X 75mm thick', 'currency': 'n/a', 'key material used in the product': 'n/a', 'per unit amount': '$ 73.88 / m', 'phase 1 amount': '$ 20,686.40', 'phase 2 amount': '$ 186,620.88', 'product category': 'D'}, 
    {'complete product description': 'Precast concrete plank; various length X 75mm thick', 'currency': 'n/a', 'key material used in the product': 'n/a', 'per unit amount': '$ 61.60 / m2', 'phase 1 amount': '$ 648.03', 'phase 2 amount': '$ 21,827.20', 'product category': 'E'}
]
"""

# ## Format OCR text output

# ### Segment OCR extracted text
"""
OCR extracted text includes text/words along with their coordinates. It needs one more layer of intelligent processing to decide which words were grouped together into a single passage, or table in the original document, to reconstruct the original text.
"""
segment_text_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering segment_text for ({doc_num}/{len(doc_names)}): {doc_name}")
    segment_text_resp = bg_async.segment_text(
        doc_name=doc_name,
    )
    segment_text_responses = segment_text_responses + [segment_text_resp]


# ### read segment_text output
segment_text_files = []
missing_segment_text_files = []
for resp_num, resp in enumerate(segment_text_responses):
    logger.info(f"processing segment_text resp: {resp_num}/{len(segment_text_responses)}")
    output_file = resp.get_output_file()
    if resp.check_output_file_exists():
        segment_text_files = segment_text_files + [resp.get_output_file()]
    else:
        missing_segment_text_files = missing_segment_text_files + [resp.get_output_file()]


