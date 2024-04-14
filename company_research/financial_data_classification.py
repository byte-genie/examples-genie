# # Extract financial data from financial statements

import time
import pandas as pd
import utils.common
import utils.async_utils
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie
bg = ByteGenie(
    secrets_file='secrets.json',
    verbose=1,
)

