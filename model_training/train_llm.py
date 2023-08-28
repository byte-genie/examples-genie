# # Train LLMs

import time
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

"""
'async' mode is suitable for long-running tasks, so that api calls can be run in the background, 
while the rest of the code can continue doing other things.

'sync' mode is suitable for short-lived tasks, where we need some output, before we can move on to anything else.
"""

# ## set inputs

# ### Set documents to train an LLM on
doc_names = [
    'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf',
    'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf',
]

# ### set username
usernme = 'demo-genie'

# ### set model name
model_name = 'model-101'

"""
username and model_name inputs are used to set the unique model_id for the trained model 
"""

# ## run model training
resp = bg_async.train_llm(
    username=usernme,
    model_name=model_name,
    doc_names=doc_names,
    training_formats=['masked-table-cells', 'generative-qa'],
)

# ## check model output exists or not
resp.check_output_file_exists()

# ## Next steps