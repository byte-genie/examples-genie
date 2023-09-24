# # Convert PDF files to markdown


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

# ### trigger pdf to markdown conversion
convert_to_markdown_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering pdf to markdown conversion for ({doc_num}/{doc_name}): {doc_name}")
    convert_to_markdown_resp = bg_async.convert_pdf_to_markdown(
        doc_name=doc_name,
    )
    convert_to_markdown_responses = convert_to_markdown_responses + [convert_to_markdown_resp]