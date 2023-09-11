# # Upload local files

import os
import base64
import pandas as pd
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


"""
'async' mode is suitable for long-running tasks, so that api calls can be run in the background, 
while the rest of the code can continue doing other things.

'sync' mode is suitable for short-lived tasks, where we need some output, before we can move on to anything else.
"""


# ## Specify the directory containing files to upload
directory = r'/tmp/sample-files'

# ## Get all file names in the directory
file_names = os.listdir(directory)

# Generate the fileContents and names arrays
contents = []
filenames = []

for file_name in file_names:
    file_path = os.path.join(directory, file_name)
    with open(file_path, 'rb') as file:
        content = file.read()
        data = base64.b64encode(content)
        data_with_prefix = "data:;base64," + data.decode('utf-8')
        contents.append(data_with_prefix)
        filenames.append(file_name)

# ## upload files
resp = bg_sync.upload_data(
    contents=contents,
    filenames=filenames,
    username=bg_sync.read_username()
)

# ## check resp
resp.get_status()

# ## check your uploads

# ### make api call
resp = bg_sync.show_uploads(
    username=bg_sync.read_username()
)

# ### get response data
df_uploads = pd.DataFrame(resp.get_data())

"""
df_uploads.to_dict('records')
[
    {'doc_name': 'userid_demo-genie_uploadfilename_demo-portfoliocsv', 'file_type': '.csv', 'filename': 'demo-portfoliocsv', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_demo_taxonomycsv', 'file_type': '.csv', 'filename': 'demo_taxonomycsv', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_deploying-a-react-app-using-aws-s3pdf', 'file_type': '.pdf', 'filename': 'deploying-a-react-app-using-aws-s3pdf', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_mandarin-oriental-sustainability-report-2020-2pdf', 'file_type': '.pdf', 'filename': 'mandarin-oriental-sustainability-report-2020-2pdf', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_oil-and-gas_taxonomycsv', 'file_type': '.csv', 'filename': 'oil-and-gas_taxonomycsv', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_real-estate_taxonomycsv', 'file_type': '.csv', 'filename': 'real-estate_taxonomycsv', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_shell-tax-contribution-report-2020pdf', 'file_type': '.pdf', 'filename': 'shell-tax-contribution-report-2020pdf', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_vodafone-tax-report-19-20pdf', 'file_type': '.pdf', 'filename': 'vodafone-tax-report-19-20pdf', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_with-highlights-comments-barclays-country-snapshot-2021pdf', 'file_type': '.pdf', 'filename': 'with-highlights-comments-barclays-country-snapshot-2021pdf', 'username': 'demo-genie'}, 
    {'doc_name': 'userid_demo-genie_uploadfilename_capitaland-recent-documents', 'file_type': '.csv', 'filename': 'capitaland-recent-documents', 'username': 'demo-genie'}
]
"""

# ## Next steps
"""
* Now that the files are uploaded, we can move on to processing these documents;
* See document_processing/short_pdf_processing.py (.ipynb) and company_research/document_processing.py (.ipynb) for examples of document processing.
"""