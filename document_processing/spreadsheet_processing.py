# <h1>Spreadsheet processing</h1>
# <p>This example covers
#     <ul>
#         <li>Extracting data from spreadsheets;</li>
#         <li>Re-structuring data in a desired format;</li>
#         <li>Saving re-structured data into a relational database.</li>
#     </ul>
# </p>

# <h2> import necessary libraries </h2>

import os
import json
import time
import uuid
import numpy as np
import pandas as pd
import utils.common
import utils.async_utils
from utils.logging import logger
from utils.byte_genie import ByteGenie

# <h2> init byte-genie </h2>

# <h3> init byte-genie in async mode (tasks will run in the background) </h3>
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    overwrite=0,
    verbose=1,
)

# <h3> init byte-genie in sync mode (tasks will run in the foreground) </h3>
bg_sync = ByteGenie(
    secrets_file='secrets.json',
    task_mode='sync',
    overwrite=0,
    verbose=1,
)

# <h2> Read local files to process </h2>

# <h3> Specify the directory containing files to upload </h3>
directory = f"/tmp/sample-spreadsheets"

# <h3> read file contents in a directory </h3>
df_contents = utils.common.read_file_contents(directory)

# <h3> upload files </h3>
resp = bg_sync.upload_data(
    contents=df_contents['content'].tolist(),
    filenames=df_contents['filename'].tolist(),
    username=bg_sync.read_username()
)

# <h3> Get uploaded data </h3>
df_uploads = pd.DataFrame(resp.get_output())

# <h3> Get uploaded document names </h3>
doc_names = df_uploads['doc_name'].unique().tolist()

# <h2> Extract text & tables from spreadsheets </h2>

# <h3> Run text extraction pipeline </h3>

# <p> Text extraction pipeline will extract both all the text
# from each page (sheet) in the original spreadsheet
# </p>

tasks = [
    bg_async.async_extract_text_pipeline(
        doc_name=doc_name,
    )
    for doc_name in doc_names
]
text_extraction_responses = utils.async_utils.run_async_tasks(tasks)
# text_extraction_output = [resp.get_output() for resp in text_extraction_responses]

# <h2> Check original table extracted </h2>

# <h3> List original table files </h3>
tasks = [
    bg_async.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=orig-table/**.csv',
    )
    for doc_name in doc_names
]
table_files_responses = utils.async_utils.run_async_tasks(tasks=tasks)
table_files = [resp.get_output() for resp in table_files_responses]
# <p> Flatten table files <p>
table_files = [file for files in table_files for file in files]
"""
Number of table files, `len(table_files)`: 3
Table files, `table_files`
[
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_energy-consumptionxlsx/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/energy-consumptionxlsx_pagenum-0_table-cells_orig-table_tablenum-0.csv',
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_energy-consumptionxlsx/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/energy-consumptionxlsx_pagenum-1_table-cells_orig-table_tablenum-0.csv',
    'gs://db-genie/entity_type=url/entity=userid_demo-genie_uploadfilename_energy-consumptionxlsx/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/energy-consumptionxlsx_pagenum-2_table-cells_orig-table_tablenum-0.csv'
]
"""


# <h3> Read table files <h3>
tasks = [
    bg_sync.async_read_file(file=file)
    for file in table_files
]
read_table_responses = utils.async_utils.run_async_tasks(tasks)
list_tables = [resp.get_output() for resp in read_table_responses]
list_tables = [pd.DataFrame(df) for df in list_tables]
"""
Number of tables, `len(list_tables)`: 3
Sample rows for the first table, `list_tables[0].head().values.tolist()`
[
    ['2023-01-01 00:00:00', 'SiteName', 'Gas Cost', '2023-09-01 00:00:00', '2023-10-01 00:00:00', '2023-11-01 00:00:00',
     '2023-12-01 00:00:00', 'Consumption', '2023-01-01 00:00:00', '2023-02-01 00:00:00', '2023-03-01 00:00:00',
     '2023-04-01 00:00:00', '2023-05-01 00:00:00', '2023-06-01 00:00:00', '2023-07-01 00:00:00', '2023-08-01 00:00:00',
     '2023-09-01 00:00:00', '2023-10-01 00:00:00', '2023-11-01 00:00:00', '2023-12-01 00:00:00', '2023-02-01 00:00:00',
     '2023-03-01 00:00:00', '2023-04-01 00:00:00', '2023-05-01 00:00:00', '2023-06-01 00:00:00', '2023-07-01 00:00:00',
     '2023-08-01 00:00:00'],
    ['5565.71', 'M&C Copthorne Hotel Aberdeen', '£', '', '', '', '', 'kWh', '29311', '27515', '26625', '29659', '28255',
     '25995', '23283', '', '', '', '', '', '4346.54', '3503.87', '3856.25', '2245.22', '2237.58', '1819.49', ''],
    ['21730.26', 'M&C Copthorne Hotel Cardiff', '£', '', '', '', '', 'kWh', '205804', '192057', '176106', '192208',
     '162342', '86441', '45364', '', '', '', '', '', '15981.46', '11000.33', '11832.19', '9514.76', '5726.41',
     '2868.53',
     ''],
    ['21256.03', 'M&C Copthorne Hotel Effingham Park', '£', '', '', '', '', 'kWh', '200627', '128827', '135245',
     '166010',
     '137450', '117816', '189719', '', '', '', '', '', '11510.45', '8951.13', '10546.71', '8165.23', '7537.26',
     '9726.37',
     ''],
    ['43273.17', 'M&C Copthorne Hotel Gatwick', '£', '', '', '', '', 'kWh', '427612', '596687', '375782', '457760',
     '379215', '323177', '367716', '', '', '', '', '', '45616.74', '21938.51', '25885.95', '21716', '19848.51',
     '18625.89', '']
]
"""

# <h2> Re-structure tables </h2>
tasks = [
    bg_async.async_create_dataset(
        file=file,
        attrs=['site_name', 'quantity_type', 'value_type', 'value', 'unit_of_measurement', 'value_date'],
        attrs_metadata=[{'site_name': 'Name of site or hotel or company'}],
    )
    for file in table_files
]
