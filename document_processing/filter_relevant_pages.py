# # Filter pages from company disclosures most relevant to KPIs of interest
"""
In this example, we will identify the most relevant text and table files extracted from a few company disclosures,
by:
* Extracting text and table files from all the pages;
* Ranking extracted text and table files by relevant KPIs;
* Extracting document meta-data for each document;
* Standardising company names across documents;
* Keeping most relevant pages across documents for each company.
"""

# ## import necessary libraries

import os
import time
import uuid
import numpy as np
import pandas as pd
import utils.common
import utils.async_utils
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    overwrite=0,
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground)
bg_sync = ByteGenie(
    secrets_file='secrets_mcp.json',
    task_mode='sync',
    overwrite=0,
    verbose=1,
)

# ## Upload PDF files

# ### set folder containing PDF files
pdf_folder = f"/tmp/PDF"

# ### get file contents
df_contents = utils.common.read_file_contents(directory=pdf_folder)

# ### upload files
start_time = time.time()
tasks = [
    bg_async.async_upload_data(
        contents=[df_contents['content'].tolist()[i]],
        filenames=[df_contents['filename'].tolist()[i]],
        username=bg_sync.read_username(),
    )
    for i in range(0, len(df_contents), 1)
]
upload_responses = utils.async_utils.run_async_tasks(tasks)
end_time = time.time()
logger.info(
    f"Time taken to upload {len(upload_responses)} documents: "
    f"{(end_time - start_time) / 60} min"
)
"""
Time taken to upload 55 documents: 8.278589681784313 min
"""

# ## check uploaded data

# ### define async tasks to read output data
start_time = time.time()
tasks = [
    resp.async_read_output_data()
    for resp_num, resp in enumerate(upload_responses)
    if resp is not None
]

# ### run tasks
df_uploads = utils.async_utils.run_async_tasks(tasks)

# ### convert output to dataframes
df_uploads = [pd.DataFrame(df) for df in df_uploads]

# ### concat dataframes
df_uploads = pd.concat(df_uploads)
# reset index
df_uploads = df_uploads.reset_index(drop=True)
end_time = time.time()
logger.info(
    f"Time taken to read {len(df_uploads)} upload responses: "
    f"{(end_time - start_time) / 60} min"
)
"""
Time taken to read 53 upload responses: 10.077525063355763 min
list(df_uploads.columns)
['doc_name', 'file_type', 'filename', 'href', 'username']
df_uploads.head().to_dict('records')
[
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'file_type': '.pdf', 'filename': 'jason_08_gpgpdf', 'href': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=pdf/variable_desc=uploaded-document/source=stuartcullinan/jason_08_gpgpdf.pdf', 'username': 'stuartcullinan'}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'file_type': '.pdf', 'filename': 'jeon_20_billerudkorsnas_annual-report_2021pdf', 'href': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=unstructured/format=pdf/variable_desc=uploaded-document/source=stuartcullinan/jeon_20_billerudkorsnas_annual-report_2021pdf.pdf', 'username': 'stuartcullinan'}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'file_type': '.pdf', 'filename': 'karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'href': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf/data_type=unstructured/format=pdf/variable_desc=uploaded-document/source=stuartcullinan/karishma-13-2021-air-new-zealand-gender-pay-reportpdf.pdf', 'username': 'stuartcullinan'}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf', 'file_type': '.pdf', 'filename': 'jeon_25_upm_annual-report_2021pdf', 'href': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf/data_type=unstructured/format=pdf/variable_desc=uploaded-document/source=stuartcullinan/jeon_25_upm_annual-report_2021pdf.pdf', 'username': 'stuartcullinan'}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'file_type': '.pdf', 'filename': 'karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'href': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf/data_type=unstructured/format=pdf/variable_desc=uploaded-document/source=stuartcullinan/karishma-13-anti-bribery-and-corruption-policy-august-2021pdf.pdf', 'username': 'stuartcullinan'}
]
"""

# ## set documents

# ### get uploaded document names
doc_names = df_uploads['doc_name'].unique().tolist()
"""
input documents: `doc_names`
['userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'userid_stuartcullinan_uploadfilename_jason_09_srpdf', 'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf', 'userid_stuartcullinan_uploadfilename_anastasia_19_china_east_education_ltd_20211228164502_62371643_enpdf', 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf', 'userid_stuartcullinan_uploadfilename_28_kim_cartapdf', 'userid_stuartcullinan_uploadfilename_karishma-03-lse_rav_2020pdf', 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf', 'userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf', 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf', 'userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf', 'userid_stuartcullinan_uploadfilename_jeon_22_boliden_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf', 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'userid_stuartcullinan_uploadfilename_jaime_allianz-group_sustainability-reportpdf', 'userid_stuartcullinan_uploadfilename_jason_14_srpdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-annual-financial-resultspdf', 'userid_stuartcullinan_uploadfilename_jeon_27_ecolab_corporate-responsibility-report_2021pdf', 'userid_stuartcullinan_uploadfilename_16_samsung_sdspdf', 'userid_stuartcullinan_uploadfilename_jeon_26_bayer_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_al_9_webuild_ethics_code_1pdf', 'userid_stuartcullinan_uploadfilename_anastasia_4_-2020-aggreko-annual-reportpdf', 'userid_stuartcullinan_uploadfilename_12_ashteadgroup_mrpdf', 'userid_stuartcullinan_uploadfilename_al_6_kier-2021-ara-finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf', 'userid_stuartcullinan_uploadfilename_4_kim_cartapdfpdf', 'userid_stuartcullinan_uploadfilename_3_cgcpdf', 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_1_adesso_sepdfpdf', 'userid_stuartcullinan_uploadfilename_jason_08_srpdf', 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf', 'userid_stuartcullinan_uploadfilename_12_argo_blockchainpdfpdf', 'userid_stuartcullinan_uploadfilename_13_capita_mrpdf', 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf', 'userid_stuartcullinan_uploadfilename_5_compass-group_mrpdf', 'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_uk-pay-gap-reportpdf', 'userid_stuartcullinan_uploadfilename_karishma-04-sustainability-highlights-report-2021-19-finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-01-des-annualreport-2021-e-spdf', 'userid_stuartcullinan_uploadfilename_al_9_relazione-governance-2021-final_eng-con-tabellepdf', 'userid_stuartcullinan_uploadfilename_jeon_07_a2-milk-company_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jason_14_gpgpdf', 'userid_stuartcullinan_uploadfilename_karishma-04-savills-plc-ar21pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-greenhouse-gas-inventory-report_finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-new-zealand-sustainability-report-2020pdf']
"""

# ## Extract page images

# ### extract page images from documents
img_extraction_start_time = time.time()  # 1695212673.5361311
tasks = [
    bg_async.async_write_pdf_img(
        doc_name=doc_name
    )
    for doc_num, doc_name in enumerate(doc_names)
]
write_img_responses = utils.async_utils.run_async_tasks(tasks)
"""
ocr_start_time
1695410621.57586
"""

# ### list extracted page images
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"*.png",
        timeout=15 * 60,
    )
    for doc_num, doc_name in enumerate(doc_names)
]
img_files = utils.async_utils.run_async_tasks(tasks)

# ### check img files
logger.info(f"{len(img_files[0].get_data())} img files found for {doc_names[0]}")
"""
img_files[0].get_data()
['gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-2.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-3.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-4.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-5.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-6.png', 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-7.png']
"""

# ## Extract text and tables from documents

# ### Run OCR on page images
ocr_start_time = time.time()
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering OCR for ({doc_num}/{len(doc_names)}): {doc_name}")
    resp = bg_async.extract_text(
        doc_name=doc_name
    )
    responses = responses + [resp]

# ### list OCR output files for text
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="data_type=semi-structured/**/variable_desc=text-blocks/**.csv"
    )
    for doc_name in doc_names
]
ocr_text_files = utils.async_utils.run_async_tasks(tasks)
ocr_text_files = [resp.get_output() for resp in ocr_text_files if resp.get_output() is not None]
"""
Number of documents with OCR text files, `len(ocr_text_files)`: 49
Number of OCR text files for one document, `len(ocr_text_files[0])`: 8
First 5 OCR text files for one document: ocr_text_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jason_08_gpgpdf_pagenum-0_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jason_08_gpgpdf_pagenum-1_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jason_08_gpgpdf_pagenum-2_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jason_08_gpgpdf_pagenum-3_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jason_08_gpgpdf_pagenum-4_text-blocks.csv'
]
Extracted text files contain page number from which the text was extracted, so tables belonging to a specific page can be filtered, if needed.
"""

# ### list OCR output files for tables
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="data_type=semi-structured/**variable_desc=table-cells/**.csv"
    )
    for doc_name in doc_names
]
ocr_table_files = utils.async_utils.run_async_tasks(tasks)
ocr_table_files = [resp.get_output() for resp in ocr_table_files if resp.get_output() is not None]
"""
Number of documents with OCR table output files, len(ocr_table_files): 48
Number of OCR table files for one document, len(ocr_table_files[5]): 24
ocr_table_files[5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-1_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-21_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-22_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-25_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-27_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-29_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-34_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-36_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-42_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-44_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-46_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-49_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-52_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-53_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-54_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-59_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-64_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-65_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-68_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-73_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-77_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-78_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-7_table-cells.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=semi-structured/format=csv/variable_desc=table-cells/source=esgnie.com/1_accor_mrpdf_pagenum-8_table-cells.csv'
]
Extracted table files contain page number from which a table was extracted, so tables belonging to a specific page can be filtered, if needed.
"""

# ## Reconstruct original tables
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering original table reconstruction for ({doc_num}/{len(doc_names)}: {doc_name})")
    resp = bg_async.reconstruct_orig_tables(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=table-cells/**.csv',
    )
    responses = responses + [resp]

# ## Segment OCR extracted text
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

# ### list segment_text output
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="data_type=semi-structured/**/variable_desc=text-segments/**.csv"
    )
    for doc_name in doc_names
]
text_segment_files = utils.async_utils.run_async_tasks(tasks)
text_segment_files = [resp.get_data() for resp in text_segment_files if resp.get_data() is not None]
"""
Number of documents with text segment files available, len(text_segment_files): 45
First 5 text segment files for first document, text_segment_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_srpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_srpdf_pagenum-10_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_srpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_srpdf_pagenum-11_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_srpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_srpdf_pagenum-12_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_srpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_srpdf_pagenum-15_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_srpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_srpdf_pagenum-16_table-cells_orig-table_tablenum-0.csv'
]
"""

# ### flatten text_segment_files
text_segment_files = [file for doc_files in text_segment_files for file in doc_files]
text_segment_files = list(set(text_segment_files))
logger.info(f"total number of text segment files across all documents: {len(text_segment_files)}")
"""
len(text_segment_files)
6085
"""

# ### read a few text segments files
tasks = [
    bg_sync.async_read_file(file)
    for file in text_segment_files[:5]
]
df_text_segments = utils.async_utils.run_async_tasks(tasks)
df_text_segments = [pd.DataFrame(resp.get_data()) for resp in df_text_segments]
df_text_segments = pd.concat(df_text_segments)
"""
list(df_text_segments.columns)
['pagenum', 'text', 'xy_group']
df_text_segments['text'].tolist()[:5]
[
    'Assets held for sale are recognised as such when the following events take place:', 
    'signing of a binding sales agreement;', 
    'approval and communication of a formal sales plan by directors.', 
    'In order to be correctly measured, the assets shall be:', 
    'available for immediate sale in their present condition,'
]
"""

# ## Rank text and table files by relevance to KPIs

# ### embed table files
tasks = [
    bg_async.async_embed_doc_data(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=orig-table/**.csv',
        cols_to_use=None,
    )
    for doc_name in doc_names
]
## run tasks in batches of 10 documents at a time to avoid rate limit errors
batch_size = 10
wait_time = 2 * 60
doc_emb_responses = []
for task_num, task in enumerate(tasks):
    logger.info(f"running task: {task_num}/{len(tasks)}")
    doc_emb_response_ = utils.async_utils.run_async_tasks([task])
    doc_emb_responses.append(doc_emb_response_)
    if (task_num % batch_size == 0) and (task_num > 0):
        time.sleep(wait_time)

# ### embed text segment files
tasks = [
    bg_async.async_embed_doc_data(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=text-segments/**.csv',
        cols_to_use=['text'],
    )
    for doc_name in doc_names
]
## run tasks in batches of 10 documents at a time to avoid rate limit errors
batch_size = 10
wait_time = 2 * 60
doc_emb_responses = []
for task_num, task in enumerate(tasks):
    logger.info(f"running task: {task_num}/{len(tasks)}")
    doc_emb_response_ = utils.async_utils.run_async_tasks([task])
    doc_emb_responses.append(doc_emb_response_)
    if (task_num % batch_size == 0) and (task_num > 0):
        time.sleep(wait_time)

# ## Rank tables by relevance to keyphrases
"""
Once we have the tables extracted, we can rank them by relevance to the KPIs to filter out the most relevant data.
"""

# ### set attributes to extract

## KPIs for which we want quantitative data
kpis = [
    '% of female representation on the board',
    'hazardous waste',
    'gender pay gap',
    'GHG Scope 1 emissions',
    'GHG Scope 2 emissions',
    'GHG Scope 3 emissions',
    'Non-renewable energy consumption',
    'Emissions to water',
    'Percentage of non-renewable energy production',
    'anti-corruption policies',
    'anti-bribery policies',
]

# ### score table similarity
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"running similarity scoring for ({doc_num}/{len(doc_names)}): {doc_name}")
    try:
        tasks = [
            bg_async.async_score_doc_text_similarity(
                doc_name=doc_name,
                file_pattern='data_type=embeddings/**/variable_desc=orig-table/**.csv',
                query=query,
            )
            for query in kpis
        ]
        score_table_sim_responses = utils.async_utils.run_async_tasks(tasks)
    except Exception as e:
        logger.warning(f"Error running similarity scoring for: {doc_name}")

# ## Rank text by relevance to KPIs
"""
Once we have the text segments extracted from documents, we can rank them by relevance to the KPIs to filter out the most relevant data.
"""

# ### score text data by similarity
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"running similarity scoring for ({doc_num}/{len(doc_names)}): {doc_name}")
    try:
        tasks = [
            bg_async.async_score_doc_text_similarity(
                doc_name=doc_name,
                file_pattern='data_type=embeddings/**/variable_desc=text-segments/**.csv',
                query=query,
            )
            for query in kpis
        ]
        score_text_sim_responses = utils.async_utils.run_async_tasks(tasks)
    except Exception as e:
        logger.warning(f"Error running similarity scoring for: {doc_name}")

# ## filter most relevant table files
"""
Once, the tables are scored by similarity to relevant KPIs, we can filter out the most relevant table files
"""

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=orig-table/**.csv',
        filter_what='files',
        groupby_cols=['query'],
        max_rows_to_keep=5,
        filename_sfx='filtered-tables',
    )
    for doc_name in doc_names
]
## run tasks
filtered_table_responses = utils.async_utils.run_async_tasks(tasks)

# ### get filtered table files
filtered_table_sim_files = [resp.get_output() for resp in filtered_table_responses]
filtered_table_sim_files = [file for file in filtered_table_sim_files if file is not None]
"""
Number of filtered table files, `len(filtered_table_sim_files)`: 48
First 5 filtered table files, `filtered_table_sim_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_descorig-tablecsv/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_filtered-tables.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_descorig-tablecsv/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_filtered-tables.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_descorig-tablecsv/userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf_filtered-tables.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_descorig-tablecsv/userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf_filtered-tables.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_srpdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_descorig-tablecsv/userid_stuartcullinan_uploadfilename_jason_09_srpdf_filtered-tables.csv'
] 
"""

# ## Get most similar original-table files
"""
Once, we have scored extracted table files by similarity to our KPIs, 
we can retrieve the most similar table files for each KPI from each document. 
"""

# ### Read filtered table similarity files
tasks = [
    bg_sync.async_read_file(
        file=file
    )
    for file in filtered_table_sim_files
]
df_filtered_table_sim_files = utils.async_utils.run_async_tasks(tasks)
df_filtered_table_sim_files = [resp.get_output() for resp in df_filtered_table_sim_files]
df_filtered_table_sim_files = [pd.DataFrame(df) for df in df_filtered_table_sim_files]
df_filtered_table_sim_files = pd.concat(df_filtered_table_sim_files)
## add doc_name to df
df_filtered_table_sim_files['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_filtered_table_sim_files['file']
]
## add page number
df_filtered_table_sim_files['pagenum'] = [
    os.path.splitext(file)[0].split('pagenum-')[-1].split('_')[0]
    for file in df_filtered_table_sim_files['file']
]
## check filtered table similarity files for 1st document, for a specific KPI
mask = (df_filtered_table_sim_files['doc_name'] == doc_names[0]) & \
       (df_filtered_table_sim_files['query'] == kpis[0])
logger.info(
    f"Filtered table similarity files for 1st document, and first KPI: "
    f"{df_filtered_table_sim_files[mask]['file'].unique().tolist()}"
)
"""
Filtered table similarity files for 1st document, and first KPI
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-2_embeddings_similarity_query-of-female-representation-on-the-board.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0_embeddings_similarity_query-of-female-representation-on-the-board.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-1_embeddings_similarity_query-of-female-representation-on-the-board.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-1_embeddings_similarity_query-of-female-representation-on-the-board.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-0_embeddings_similarity_query-of-female-representation-on-the-board.csv'
]
As we kept max of 5 table files in `/filter_similarity_scored_data` api call, we will have 5 top ranked files from each document for each KPI
"""
# ### Get underlying orig-table files
"""
Once, we have filtered the similarity-scored files, we need to get the underlying original-table that contain the tables data. 
We can retrieve these files using `/list_corresponding_files` endpoint.
"""
tasks = [
    bg_sync.async_list_corresponding_files(
        files=df_filtered_table_sim_files['file'].unique().tolist(),
        data_type='semi-structured',
        variable_desc='orig-table',
        file_format='csv',
    )
]
filtered_orig_table_files = utils.async_utils.run_async_tasks(tasks)
filtered_orig_table_files = [resp.get_output() for resp in filtered_orig_table_files]
## flatten filtered_orig_table_files
filtered_orig_table_files = [file for files in filtered_orig_table_files for file in files]
"""
Number of original-table files after filtering over relevant KPIs: `len(filtered_orig_table_files)`: 2384
Fist 5 original table files after filtering over relevant KPIs: `filtered_orig_table_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-5_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-0_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-1.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-6_table-cells_orig-table_tablenum-0.csv'
]
"""
# ### add orig-table files to df_filtered_table_sim_files
df_filtered_table_sim_files['orig_table_file'] = filtered_orig_table_files
"""
First 5 rows of df_filtered_table_sim_files, `df_filtered_table_sim_files[['query', 'score', 'doc_name', 'orig_table_file']].head(5).to_dict('records')`
[
    {'query': 'hazardous waste', 'score': 0.6973772931528301, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0.csv'}, 
    {'query': 'hazardous waste', 'score': 0.690958398363218, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-5_table-cells_orig-table_tablenum-0.csv'}, 
    {'query': 'hazardous waste', 'score': 0.6870429295359026, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-0_table-cells_orig-table_tablenum-0.csv'}, 
    {'query': 'hazardous waste', 'score': 0.6820895998128814, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-1.csv'}, 
    {'query': 'hazardous waste', 'score': 0.6817806352540019, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-6_table-cells_orig-table_tablenum-0.csv'}
]
`df_filtered_table_sim_files` now contains the most relevant table files for each KPI from each document. 
So we will use this dataframe to access the most relevant files, and do further processing on them. 
"""
# ### save df_filtered_table_sim_files locally
df_filtered_table_sim_files.to_csv(f"/tmp/df_filtered_table_sim_files.csv", index=False)


# ## filter most relevant text files
"""
Once, the text segments are scored by similarity to relevant KPIs, we can filter out the most relevant text files
"""

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=text-segments/**.csv',
        filter_what='files',
        groupby_cols=['query'],
        max_rows_to_keep=5,
        filename_sfx='filtered-text',
    )
    for doc_name in doc_names
]
## run tasks
filtered_text_responses = utils.async_utils.run_async_tasks(tasks)

# ### get filtered text files
filtered_text_sim_files = [resp.get_output() for resp in filtered_text_responses]
filtered_text_sim_files = [file for file in filtered_text_sim_files if file is not None]
"""
Number of documents with filtered text similarity files, `len(filtered_text_sim_files)`: 49
First 5 filtered text similarity files, `filtered_text_sim_files[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf_filtered-text.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf/data_type=similarity/format=csv/variable_desc=filtered-files/source=data_typesimilarityvariable_desctext-segmentscsv/userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf_filtered-text.csv'
]
We have one filtered text similarity file for each document that contains the paths to all the most relevant similarity files for that document.
"""

# ## Read most similar text files
"""
Once, we read the filtered text files, it will give us the file paths for text similarity files 
for each of our KPIs.
"""

# ### Read filtered text similarity files
tasks = [
    bg_sync.async_read_file(
        file=file
    )
    for file in filtered_text_sim_files
]
df_filtered_text_sim_files = utils.async_utils.run_async_tasks(tasks)
df_filtered_text_sim_files = [resp.get_output() for resp in df_filtered_text_sim_files]
df_filtered_text_sim_files = [pd.DataFrame(df) for df in df_filtered_text_sim_files]
df_filtered_text_sim_files = pd.concat(df_filtered_text_sim_files)
## add doc_name to df
df_filtered_text_sim_files['doc_name'] = [
    file.split('entity=')[-1].split('/')[0]
    for file in df_filtered_text_sim_files['file']
]
## add pagenum to df
df_filtered_text_sim_files['pagenum'] = [
    os.path.splitext(file)[0].split('pagenum-')[-1].split('_')[0]
    for file in df_filtered_text_sim_files['file']
]
"""
Number of documents in filtered text similarity dataframe, `len(df_filtered_text_sim_files['doc_name'].unique())`: 49
First 5 similarity files, `df_filtered_text_sim_files['file'].tolist()[:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-3_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-7_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-1_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-4_text-blocks_text-segments_embeddings_similarity_query-hazardous-waste.csv'
]
As we can see, these files contain the page numbers that are the most relevant for each KPI. 
"""

# ## Find most relevant pages
"""
Now that we have both relevnt table and text files, we can extract the page numbers to identify pages that are most relevant to our KPIs.
"""

# ### append df_filtered_table_sim_files and df_filtered_text_sim_files
df_filtered_sim_files = pd.concat([df_filtered_table_sim_files, df_filtered_text_sim_files])

## save df_filtered_sim_files locally
df_filtered_sim_files.to_csv(f"/tmp/df_filtered_sim_files.csv", index=False)


# ## Add document info (meta-data)
"""
Since, we may have multiple documents for each company, and we only need the most relevant pages across all company documents, 
we will now merge the document meta-data onto filtered files, to be able to rank pages by company.
"""

# ### trigger doc info extraction
tasks = [
    bg_async.async_extract_doc_info(
        doc_name=doc_name,
    )
    for doc_name in doc_names
]
df_doc_info = utils.async_utils.run_async_tasks(tasks)

# ### read extracted doc info
df_doc_info = [resp.get_output() for resp in df_doc_info]
# convert to dataframe
df_doc_info = [pd.DataFrame(df) for df in df_doc_info]
df_doc_info = pd.concat(df_doc_info)
logger.info(f"length of df_doc_info: {len(df_doc_info)}")
"""
Number of documents in df_doc_info: `len(df_doc_info['doc_name'].unique())`: 53
df_doc_info.head().to_dict('records')
[
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'doc_org': 'American Express', 'doc_type': "['annual report']", 'doc_year': 2021, 'num_pages': 8}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'doc_org': 'BillerudKorsnäs', 'doc_type': "['annual report']", 'doc_year': 2021, 'num_pages': 132}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'doc_org': 'Air New Zealand', 'doc_type': "['sustainability report']", 'doc_year': 2021, 'num_pages': 1}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf', 'doc_org': 'UPM', 'doc_type': "['annual report']", 'doc_year': 2021, 'num_pages': 119}, 
    {'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'doc_org': 'Air New Zealand', 'doc_type': "['anti-corruption policy']", 'doc_year': 2019, 'num_pages': 4}
]
"""

# ### Merge document info onto filtered tabular files
df_filtered_sim_files = pd.merge(
    left=df_filtered_sim_files,
    right=df_doc_info,
    on=['doc_name'],
    how='left'
)
"""
Check `doc_org`, i.e. organisation name that published the documents 
df_filtered_sim_files['doc_org'].unique().tolist()
['American Express', 'BillerudKorsnäs', 'UPM', 'Air New Zealand', 'American International Group, Inc.', 'Aviva plc', 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'AIG', 'RAVEN PROPERTY GROUP LIMITED', 'ACCOR', 'Admiral Group plc', 'DEG Deutsche EuroShop', 'Ledlenser', 'Albioma', 'AKER CARBON CAPTURE', 'ABB', '3M', 'Webuild S.p.A.', 'VINCI', 'Allianz Group', 'Arch Capital Group Ltd.', 'Air New Zealand Limited', 'ECOLAB', 'Samsung SDS', 'Bayer', 'WEBUILD', 'Aggreko plc', 'Ashtead Group plc', 'Kier Group plc', 'Adani Ports and Special Economic Zone Limited', 'KIN +CARTA', 'SCGG', 'Lenzing', 'adesso SE', 'Mondi Group', 'ARKEMA', 'Responsible Business Report', 'COMPASS GROUP', 'Aviva', 'WEBUILD S.p.A.', 'THE a2 MILK COMPANY LIMITED', 'Arch Insurance Group Inc.', 'Savills plc']
Since different documents may state the same company's name somewhat differently, we see small variations in doc_org values, 
e.g. ('Air New Zealand', 'Air New Zealand Limited'). Such as variations can be standardized to make downstream processing easier.
"""

# ## Standardise doc_org

# ### Trigger standardisation
name_std_resp = bg_async.standardise_names(
    data=df_filtered_sim_files[['doc_org']].drop_duplicates().to_dict('records'),
    text_col='doc_org',
    name_keyword='company name',
)
## get output
df_std_doc_org = name_std_resp.get_output()
df_std_doc_org = pd.DataFrame(df_std_doc_org)
"""
Standardised company names, `df_std_doc_org[['orig_name', 'std_name']].to_dict('records')`
[
    {'orig_name': 'American Express', 'std_name': 'American Express'}, 
    {'orig_name': 'BillerudKorsnäs', 'std_name': 'BillerudKorsnäs'}, 
    {'orig_name': 'UPM', 'std_name': 'UPM'}, 
    {'orig_name': 'Air New Zealand', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'American International Group, Inc.', 'std_name': 'American International Group, Inc.'}, 
    {'orig_name': 'Aviva plc', 'std_name': 'Aviva plc'}, 
    {'orig_name': 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'std_name': 'CHINA EDUCATION GROUP HOLDINGS LIMITED'}, 
    {'orig_name': 'AIG', 'std_name': 'AIG'}, 
    {'orig_name': 'RAVEN PROPERTY GROUP LIMITED', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'ACCOR', 'std_name': 'ACCOR'}, 
    {'orig_name': 'Admiral Group plc', 'std_name': 'Admiral Group plc'}, 
    {'orig_name': 'DEG Deutsche EuroShop', 'std_name': 'DEG Deutsche EuroShop'}, 
    {'orig_name': 'Ledlenser', 'std_name': 'Ledlenser'}, 
    {'orig_name': 'Albioma', 'std_name': 'Albioma'}, 
    {'orig_name': 'AKER CARBON CAPTURE', 'std_name': 'AKER CARBON CAPTURE'}, 
    {'orig_name': 'ABB', 'std_name': 'ABB'}, 
    {'orig_name': '3M', 'std_name': '3M'}, 
    {'orig_name': 'Webuild S.p.A.', 'std_name': 'WEBUILD'}, 
    {'orig_name': 'VINCI', 'std_name': 'VINCI'}, 
    {'orig_name': 'Allianz Group', 'std_name': 'Allianz Group'}, 
    {'orig_name': 'Arch Capital Group Ltd.', 'std_name': 'Arch Capital Group Ltd.'}, 
    {'orig_name': 'Air New Zealand Limited', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'ECOLAB', 'std_name': 'ECOLAB'},
    {'orig_name': 'Samsung SDS', 'std_name': 'Samsung SDS'}, 
    {'orig_name': 'Bayer', 'std_name': 'Bayer'}, 
    {'orig_name': 'WEBUILD', 'std_name': 'WEBUILD'}, 
    {'orig_name': 'Aggreko plc', 'std_name': 'Aggreko plc'}, 
    {'orig_name': 'Ashtead Group plc', 'std_name': 'Ashtead Group plc'},
    {'orig_name': 'Kier Group plc', 'std_name': 'Kier Group plc'}, 
    {'orig_name': 'Adani Ports and Special Economic Zone Limited', 'std_name': 'Adani Ports and Special Economic Zone Limited'}, 
    {'orig_name': 'KIN + CARTA', 'std_name': 'KIN + CARTA'}, 
    {'orig_name': 'SCGG', 'std_name': 'SCGG'}, 
    {'orig_name': 'Lenzing', 'std_name': 'Lenzing'}, 
    {'orig_name': 'adesso SE', 'std_name': 'adesso SE'}, 
    {'orig_name': 'Mondi Group', 'std_name': 'Mondi Group'}, 
    {'orig_name': 'ARKEMA', 'std_name': 'ARKEMA'}, 
    {'orig_name': 'Responsible Business Report', 'std_name': 'Responsible Business Report'}, 
    {'orig_name': 'COMPASS GROUP', 'std_name': 'COMPASS GROUP'}, 
    {'orig_name': 'Aviva', 'std_name': 'Aviva'}, 
    {'orig_name': 'WEBUILD S.p.A.', 'std_name': 'WEBUILD'}, 
    {'orig_name': 'THE a2 MILK COMPANY LIMITED', 'std_name': 'THE a2 MILK COMPANY LIMITED'}, 
    {'orig_name': 'Arch Insurance Group Inc.', 'std_name': 'Arch Insurance Group Inc.'}, 
    {'orig_name': 'Savills plc', 'std_name': 'Savills plc'}
]
Number of unique standardised company names: `len(df_std_doc_org['orig_name'].unique())`: 43
Number of unique standardised company names: `len(df_std_doc_org['std_name'].unique())`: 40
As we can see, number of company names have gone down from 43 in the original names to 40 in the standardised names, so we have removed a few duplicates.
"""
## fill NA std_name values with orig_name
mask = df_std_doc_org['std_name'].isnull()
df_std_doc_org.loc[mask, 'std_name'] = df_std_doc_org.loc[mask, 'orig_name']

# ### merge standardise doc_org into df_filtered_sim_files
df_filtered_sim_files = pd.merge(
    left=df_filtered_sim_files,
    right=df_std_doc_org.rename(
        columns={
            'orig_name': 'doc_org',
            'std_name': 'doc_org_std',
        }
    ),
    on=['doc_org'],
    how='left'
)
## fill NA std_name values with orig_name
mask = df_filtered_sim_files['doc_org_std'].isnull()
df_filtered_sim_files.loc[mask, 'doc_org_std'] = df_filtered_sim_files.loc[mask, 'doc_org']
"""
Sample of standardised document organisation names in `df_filtered_sim_files`
df_filtered_sim_files[['query', 'score', 'doc_name', 'orig_table_file', 'doc_org_std']].head().to_dict('records')
[
    {'query': 'hazardous waste', 'score': 0.6973772931528301, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.690958398363218, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-5_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.6870429295359026, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-0_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.6820895998128814, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-1.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.6817806352540019, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-6_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}
]
"""
# ### save df_filtered_table_sim_files locally
df_filtered_sim_files.to_csv(f"/tmp/df_filtered_sim_files.csv", index=False)


# ### calc max score by (query, doc_name) for each query
df_filtered_sim_files['page_rank'] = \
    df_filtered_sim_files.groupby(
        by=['doc_org_std', 'query']
    )['score'].rank('dense', ascending=False)

# ### sort df_filtered_sim_files by company name, query, page_rank
df_filtered_sim_files = df_filtered_sim_files.sort_values(
    by=['query', 'page_rank', 'doc_org_std', 'doc_name']
).reset_index(drop=True)
"""
Top ranked pages, 
df_filtered_sim_files[df_filtered_sim_files['page_rank'] <=2][['query', 'score', 'page_rank', 'doc_org_std', 'doc_name', 'pagenum', 'file']].head().to_dict('records')
[
    {'query': '% of female representation on the board', 'score': 0.885834557694716, 'page_rank': 1.0, 'doc_org_std': '3M', 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf', 'pagenum': '0', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jeon_01_3m-company_sustainability-report_2021pdf_pagenum-0_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.8688483983019661, 'page_rank': 1.0, 'doc_org_std': 'ABB', 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf', 'pagenum': '94', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jeon_08_abb_sustainability-report_2021pdf_pagenum-94_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.8520371452852017, 'page_rank': 1.0, 'doc_org_std': 'ACCOR', 'doc_name': 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf', 'pagenum': '73', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_1_accor_mrpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/1_accor_mrpdf_pagenum-73_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.831945273415326, 'page_rank': 1.0, 'doc_org_std': 'AIG', 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf', 'pagenum': '2', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_gpgpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_09_gpgpdf_pagenum-2_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv'}, 
    {'query': '% of female representation on the board', 'score': 0.8651149368646915, 'page_rank': 1.0, 'doc_org_std': 'AKER CARBON CAPTURE', 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'pagenum': '107', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jeon_21_aker-carbon-capture_annual-report_2021pdf_pagenum-107_text-blocks_text-segments_embeddings_similarity_query-of-female-representation-on-the-board.csv'}
]
Now that we have the most relevant pages for each query and company, we will read the top 2 pages, and extract the relevant KPIs
"""

# ### save df_filtered_table_sim_files locally
df_filtered_sim_files.to_csv(f"/tmp/df_filtered_sim_files.csv", index=False)


# ## Get page data for most relevant pages
"""
To get the full page content content that contains tables as well as page text in a structured format, 
we will use `/read_page_data` endpoint.
"""

# ### convert pagenum to int
df_filtered_sim_files['pagenum'] = [
    int(p) for p in df_filtered_sim_files['pagenum']
]

# ### define page reading tasks
tasks = [
    bg_async.async_read_page_data(
        doc_name=doc_name,
        page_numbers=df_filtered_sim_files[
            (df_filtered_sim_files['doc_name'] == doc_name) &
            (df_filtered_sim_files['page_rank'] <= 2)
            ]['pagenum'].unique().tolist()
    )
    for doc_name in doc_names
]
# ### run tasks
page_data_responses = utils.async_utils.run_async_tasks(tasks)
page_data_files = [resp.get_output() for resp in page_data_responses]
page_data_files = [files for files in page_data_files if files is not None]
## flatten page_data_files
page_data_files = [file for files in page_data_files for file in files]
"""
Number of page data files, `len(page_data_files)`: 1443
First 5 page data files, `page_data_files[:5]` 
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=pickle/variable_desc=page-data/source=read_page_data/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_page-data.pickle', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=pickle/variable_desc=page-data/source=read_page_data/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_page-data.pickle', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=pickle/variable_desc=page-data/source=read_page_data/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_page-data.pickle', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=pickle/variable_desc=page-data/source=read_page_data/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_page-data.pickle', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=pickle/variable_desc=page-data/source=read_page_data/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-4_page-data.pickle'
]
"""

# ### save filtered page files to the cloud

## remove NA with empty strings
df_filtered_sim_files = df_filtered_sim_files.fillna('')
## drop unnecessary columns
df_filtered_sim_files = df_filtered_sim_files.drop(columns=['context'])
## write the data locally first
os.makedirs("/tmp/filtered_files", exist_ok=True)
df_filtered_sim_files.to_csv(f"/tmp/filtered_files/df_filtered_sim_files.csv", index=False)
## read data in bytes
df_filtered_sim_files_bytes = utils.common.read_file_contents("/tmp/filtered_files")
## upload data
upload_resp = bg_sync.upload_data(
    contents=df_filtered_sim_files_bytes['content'].tolist(),
    filenames=df_filtered_sim_files_bytes['filename'].tolist()
)
logger.info(f"Uploaded data: {upload_resp.get_output()}")
"""
Uploaded data
[
    {'doc_name': 'userid_stuartcullinan_uploadfilename_df_filtered_sim_filescsv', 'file_type': '.csv', 'filename': 'df_filtered_sim_filescsv', 'href': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_df_filtered_sim_filescsv/data_type=unstructured/format=csv/variable_desc=uploaded-document/source=stuartcullinan/df_filtered_sim_filescsv.csv', 'username': 'stuartcullinan'}
]
"""

# ## Next Steps
"""
Now that we have the most relevant pages filtered and saved, we can move on to extracting and structuring quantitative info 
from these pages. See `document_processing/extract_and_filter_quants_from_pages.py` for such an example. 
"""