# # Upload documents


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


# ## Next steps
"""
Now that documents are uploaded, we can run any processing on them.
See short_pdf_processing.py (.ipynb) to see an example of processing these documents
"""