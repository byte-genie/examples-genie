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
Number of documents with OCR text files, len(ocr_text_files): 49
Number of OCR text files for one document, len(ocr_text_files[0]): 132
OCR text files for one document: ocr_text_files[0]
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
Number of documents with OCR table output files, len(ocr_table_files): 48
Number of OCR table files for one document, len(ocr_table_files[5]): 24
ocr_table_files[5]
"""


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



# ## Convert pdf documents to latex
"""
PDF to latex converter uses a specialised OCR that can convert PDF files to a latex markdown format. This converts text, tables, and equations into latex code. This is particularly useful for documents that contain equations or mathematical symbols. 
"""

# ### trigger pdf to markdown conversion
convert_to_markdown_resp = bg_async.convert_pdf_to_markdown(
    doc_names=doc_names,
)
convert_to_markdown_resp.check_output_file_exists()