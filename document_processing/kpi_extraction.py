# # Extract specific KPIs from documents

# ## import necessary libraries

import os
import time
import uuid
import pandas as pd
import utils.common
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background)
bg_async = ByteGenie(
    secrets_file='secrets_mcp.json',
    task_mode='async',
    overwrite=1,
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground)
bg_sync = ByteGenie(
    secrets_file='secrets_mcp.json',
    task_mode='sync',
    overwrite=1,
    verbose=1,
)

# ## Upload PDF files

# ### set folder containing PDF files
pdf_folder = f"/Users/majid/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/PDF"

# ### get file contents
df_contents = utils.common.read_file_contents(directory=pdf_folder)

# ### upload files
upload_responses = []
for i in range(0, len(df_contents), 1):
    logger.info(f"Uploading file: {i}/{len(df_contents)}")
    try:
        upload_resp_ = bg_async.upload_data(
            contents=[df_contents['content'].tolist()[i]],
            filenames=[df_contents['filename'].tolist()[i]],
            username=bg_sync.read_username(),
            timeout=30 * 60,
        )
        upload_responses = upload_responses + [upload_resp_]
    except Exception as e:
        logger.warning(f"Error in uploading {df_contents['filename'].tolist()[i]}")

# ### Check uploaded documents
missing_files = []
df_uploads = pd.DataFrame()
for resp_num, resp in enumerate(upload_responses):
    logger.info(f"processing upload response: {resp_num}/{len(upload_responses)}")
    try:
        if resp.check_output_file_exists():
            df_uploads_ = pd.DataFrame(resp.read_output_data())
            df_uploads = pd.concat([df_uploads, df_uploads_])
        else:
            missing_files = missing_files + [resp.get_output_file()]
    except Exception as e:
        logger.info(f"Error in processing upload response: {e}")
# reset index
df_uploads = df_uploads.reset_index(drop=True)

# ### get uploaded document names
doc_names = df_uploads['doc_name'].unique().tolist()

# ### extract page images from documents
write_img_responses = []
for doc_num, doc_name in enumerate(doc_names):
    resp_ = bg_async.write_pdf_img(
        doc_name=doc_name
    )
    write_img_responses = write_img_responses + [resp_]