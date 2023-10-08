# # Extract specific KPIs from documents

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
    secrets_file='secrets_mcp.json',
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
pdf_folder = f"/Users/majid/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/PDF"

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
Once we have the tables extracted and structured, we can rank them by relevance to the KPIs to filter out the most relevant data.
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
First 5 rows of df_filtered_table_sim_files, df_filtered_table_sim_files[['query', 'score', 'doc_name', 'orig_table_file']].head(5).to_dict('records')
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
# df_filtered_table_sim_files.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_filtered_table_sim_files.csv", index=False)

# ## Add document info (meta-data)

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
df_filtered_table_sim_files = pd.merge(
    left=df_filtered_table_sim_files,
    right=df_doc_info,
    on=['doc_name'],
    how='left'
)
"""
Check `doc_org`, i.e. organisation name that published the documents 
df_filtered_table_sim_files['doc_org'].unique().tolist()
['American Express', 'BillerudKorsnäs', 'UPM', 'Air New Zealand', 'American International Group, Inc.', 'Aviva plc', 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'AIG', 'RAVEN PROPERTY GROUP LIMITED', 'ACCOR', 'Admiral Group plc', 'DEG Deutsche EuroShop', 'Ledlenser', 'Albioma', 'AKER CARBON CAPTURE', 'ABB', '3M', 'Webuild S.p.A.', 'VINCI', 'Allianz Group', 'Arch Capital Group Ltd.', 'Air New Zealand Limited', 'ECOLAB', 'Samsung SDS', 'Bayer', 'WEBUILD', 'Aggreko plc', 'Ashtead Group plc', 'Kier Group plc', 'Adani Ports and Special Economic Zone Limited', 'KIN +CARTA', 'SCGG', 'Lenzing', 'adesso SE', 'Mondi Group', 'ARKEMA', 'Responsible Business Report', 'COMPASS GROUP', 'Aviva', 'WEBUILD S.p.A.', 'THE a2 MILK COMPANY LIMITED', 'Arch Insurance Group Inc.', 'Savills plc']
Since different documents may state the same company's name somewhat differently, we see small variations in doc_org values, 
e.g. ('Air New Zealand', 'Air New Zealand Limited'). Such as variations can be standardized to make downstream processing easier.
"""

# ## Standardise doc_org

# ### Trigger standardisation
name_std_resp = bg_async.standardise_names(
    data=df_filtered_table_sim_files[['doc_org']].drop_duplicates().to_dict('records'),
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

# ### merge standardise doc_org into df_filtered_table_sim_files
df_filtered_table_sim_files = pd.merge(
    left=df_filtered_table_sim_files,
    right=df_std_doc_org.rename(
        columns={
            'orig_name': 'doc_org',
            'std_name': 'doc_org_std',
        }
    ),
    on=['doc_org'],
    how='left'
)
"""
Sample of standardised document organisation names in `df_filtered_table_sim_files`
df_filtered_table_sim_files[['query', 'score', 'doc_name', 'orig_table_file', 'doc_org_std']].head().to_dict('records')
[
    {'query': 'hazardous waste', 'score': 0.6973772931528301, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-3_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.690958398363218, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-5_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.6870429295359026, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-0_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.6820895998128814, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-1.csv', 'doc_org_std': 'American Express'}, 
    {'query': 'hazardous waste', 'score': 0.6817806352540019, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'orig_table_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=orig-table/source=api-genie/jason_08_gpgpdf_pagenum-6_table-cells_orig-table_tablenum-0.csv', 'doc_org_std': 'American Express'}
]
"""
# ### save df_filtered_table_sim_files locally
df_filtered_table_sim_files.to_csv(f"/tmp/df_filtered_table_sim_files.csv", index=False)
# df_filtered_table_sim_files.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_filtered_table_sim_files.csv", index=False)

# ## Create datasets for each KPI
"""
Now, we will use the filtered tabular files to estimate the value for each one of our KPIs from each document, 
based on the data contained in the filtered file.
"""

# ### Set attributes to extract for each KPI
kpi_attrs = {
    '% of female representation on the board': [
        'company name', 'date', '% of female representation on the board',
        'any details about the female representation on the board'
    ],
    'hazardous waste': [
        'company name', 'date', 'hazardous waste amount', 'unit of measurement',
        'any details of hazardous waste'
    ],
    'gender pay gap': [
        'company name', 'date', 'gender pay gap', 'any description of gender pay gap'
    ],
    'GHG Scope 1 emissions': [
        'company name', 'date', 'amount of emissions', 'scope of emissions',
        'unit of measurement', 'any details of emissions'
    ],
    'GHG Scope 2 emissions': [
        'company name', 'date', 'amount of emissions', 'scope of emissions',
        'unit of measurement', 'any details of emissions'
    ],
    'GHG Scope 3 emissions': [
        'company name', 'date', 'amount of emissions', 'scope of emissions',
        'unit of measurement', 'any details of emissions'
    ],
    'Non-renewable energy consumption': [
        'company name', 'date', 'amount of energy consumption',
        'renewable or non-renewable flag', 'unit of measurement',
        'any details of energy consumption'
    ],
    'Emissions to water': [
        'company name', 'date', 'amount of emissions to water', 'unit of measurement',
        'any details of emissions to water'
    ],
    'Percentage of non-renewable energy production': [
        'company name', 'date', 'amount of energy production',
        'renewable or non-renewable flag', 'unit of measurement',
        'any details of energy production'
    ],
    'anti-corruption policies': [
        'company name', 'complete description of anti-corruption policies',
        'summary of anti-corruption policies'
    ],
    'anti-bribery policies': [
        'company name', 'complete description of anti-bribery policies',
        'summary of anti-bribery policies'
    ],
}

# ### for each doc_org_std, identify the top 2 orig_table_file with highest score
df_filtered_table_sim_files['file_rank'] = df_filtered_table_sim_files.groupby(
    by=['doc_org_std', 'query'],
    group_keys=False,
)['score'].rank('dense', ascending=False)

# ### save df_filtered_table_sim_files locally
df_filtered_table_sim_files.to_csv(f"/tmp/df_filtered_table_sim_files.csv", index=False)

# ### process top 2 ranked files first
files_to_process = \
    df_filtered_table_sim_files[
        df_filtered_table_sim_files['file_rank'] <= 2
        ]['orig_table_file'].unique().tolist()
logger.info(f"Number of files to process: {len(files_to_process)}")
"""
Number of files to process: `len(files_to_process)`: 380
"""
for file_num, file in enumerate(files_to_process):
    logger.info(f"processing ({file_num}/{len(files_to_process)}): {file}")
    ## get queries matching this file
    queries = \
        df_filtered_table_sim_files[df_filtered_table_sim_files['orig_table_file'] == file]['query'].unique().tolist()
    ## get corresponding attributes to extract for each query
    attrs_to_extract = [
        kpi_attrs[kpi] for kpi in queries
    ]
    attrs_to_extract = pd.DataFrame(attrs_to_extract)
    ## extract the same set of attrs only once
    attrs_to_extract = attrs_to_extract.drop_duplicates().reset_index(drop=True)
    attrs_to_extract = attrs_to_extract.values.tolist()
    ## remove None attributes
    attrs_to_extract = [[value for value in sublist if value is not None] for sublist in attrs_to_extract]
    ## create tasks to extract all possible attributes from this file
    tasks = [
        bg_async.async_create_dataset(
            file=file,
            attrs=attrs_to_extract_,
        )
        for attrs_to_extract_ in attrs_to_extract
    ]
    ## run tasks
    attr_extraction_responses = utils.async_utils.run_async_tasks(tasks)
    ## wait for 15 sec to avoid rate limits
    time.sleep(30)
"""
`/create_dataset` will write dataset file with extracted attributes in files with path `.../data_type=dataset/...csv`
"""

# ## Verify extracted datasets
"""
Extracted dataset files may have some errors, so we can run one layer of verification to remove such errors. 
For verification, we will use `/verify_data` endpoint, which allows to verify (variable, value) pair, given a context. 
We will focus on verifying the most important attributes in each dataset, such as 'hazardous waste amount', 'amount of emissions', etc. 
"""

# ### define tasks
tasks = [
    bg_async.async_verify_data(
        doc_name=doc_name,
        file_pattern='data_type=dataset/**/variable_desc=orig-table/**.csv',
        var_col='variable',
        val_col='value',
        context_col='context',
        verification_method='lm-verification',
        verification_type='variable-value',
        output_data_type='verification',
        vars_to_verify=[
            'gender pay gap',
            '% of female representation on the board',
            'hazardous waste amount',
            'amount of emissions to water',
            'amount of emissions',
            'amount of energy production',
        ],
    )
    for doc_name in doc_names[1:2]
]
verify_dataset_responses = utils.async_utils.run_async_tasks(tasks)

# ### list dataset files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=dataset/**/variable_desc=orig-table/**.csv"
    )
    for doc_name in doc_names
]
tabular_dataset_files = utils.async_utils.run_async_tasks(tasks)
tabular_dataset_files = [resp.get_output() for resp in tabular_dataset_files if resp.get_output() is not None]
"""
Number of documents for which dataset files are available, `len(tabular_dataset_files)`: 48
Dataset files for the first document, `tabular_dataset_files[0]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=dataset/format=csv/variable_desc=orig-table/source=0ce43925c073c45c71d798c13d00d781/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=dataset/format=csv/variable_desc=orig-table/source=2bbdd7d5532b826b7542438b805b1b7a/jason_08_gpgpdf_pagenum-7_table-cells_orig-table_tablenum-0.csv'
]
"""
# ### Flatten tabular_dataset_files
tabular_dataset_files = [file for files in tabular_dataset_files for file in files]
tabular_dataset_files = list(set(tabular_dataset_files))
tabular_dataset_docnames = [file.split('entity=')[-1].split('/')[0] for file in tabular_dataset_files]
df_tabular_dataset_files = pd.DataFrame()
df_tabular_dataset_files['file'] = tabular_dataset_files
df_tabular_dataset_files['doc_name'] = tabular_dataset_docnames

# ## Read structured datasets extracted from table files
tasks = [
    bg_async.async_read_files(
        files=df_tabular_dataset_files[df_tabular_dataset_files['doc_name'] == doc_name]['file'].unique().tolist(),
        # tabular_dataset_files,
        add_file_path=1,  ## this will add file path in the returned dataframe
    )
    for doc_name in doc_names
]
df_tabular_datasets = utils.async_utils.run_async_tasks(tasks)
df_tabular_datasets = [resp.get_output() for resp in df_tabular_datasets]
df_tabular_datasets = [pd.DataFrame(df) for df in df_tabular_datasets]
df_tabular_datasets = pd.concat(df_tabular_datasets)
## add doc_name to df_tabular_datasets
df_tabular_datasets['doc_name'] = [file.split('entity=')[-1].split('/')[0] for file in df_tabular_datasets['file']]
"""
Number of documents for which tabular datasets are available, `len(df_tabular_datasets['doc_name'].unique())`: 48
df_tabular_datasets columns, `list(df_tabular_datasets.columns)`
['context', 'file', 'row_num', 'value', 'variable', 'doc_name']  
"""
## save df_tabular_datasets locally
df_tabular_datasets.to_csv(f"/tmp/df_tabular_datasets.csv", index=False)
# df_tabular_datasets.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_tabular_datasets.csv", index=False)

# ### Merge file scores and document info with df_tabular_datasets
## add pagenum
df_tabular_datasets['pagenum'] = [
    file.split('/')[-1].split('pagenum-')[-1].split('_')[0]
    for file in df_tabular_datasets['file']
]
df_filtered_table_sim_files['pagenum'] = [
    file.split('/')[-1].split('pagenum-')[-1].split('_')[0]
    for file in df_filtered_table_sim_files['file']
]
## add pagenum
df_tabular_datasets['tablenum'] = [
    file.split('/')[-1].split('tablenum-')[-1].split('.csv')[0] for file in df_tabular_datasets['file']
]
df_filtered_table_sim_files['tablenum'] = [
    file.split('/')[-1].split('tablenum-')[-1].split('.csv')[0]
    for file in df_filtered_table_sim_files['orig_table_file']
]
## cols to add from df_filtered_table_sim_files
cols_to_add = [
    'doc_name', 'pagenum', 'tablenum', 'orig_table_file',
    'doc_org', 'doc_org_std', 'doc_year', 'num_pages',
]
## merge dataframes
df_tabular_datasets = pd.merge(
    left=df_tabular_datasets,
    right=df_filtered_table_sim_files[cols_to_add].drop_duplicates(),
    on=['doc_name', 'pagenum', 'tablenum'],
    how='left'
)
## save df_tabular_datasets locally
df_tabular_datasets.to_csv(f"/tmp/df_tabular_datasets.csv", index=False)
# df_tabular_datasets.to_csv(f"~/Dropbox/startup/ESGenie/PoCs/MainStreetPartners/data/df_tabular_datasets.csv", index=False)


# ### Get datasets for each KPI
dataset_dict = {}
for kpi_num, kpi in enumerate(kpi_attrs.keys()):
    logger.info(f"Filtering datasets for ({kpi_num}/{len(kpi_attrs.keys())}): {kpi}")
    df_ = df_tabular_datasets[df_tabular_datasets['variable'].isin(kpi_attrs[kpi])]
    df_ = df_[df_['variable'].notnull() & df_['value'].notnull() & (df_['value'] != '')].reset_index(drop=True)
    dataset_kpi = df_.pivot(
        index=[
            'doc_org', 'doc_org_std', 'doc_year', 'num_pages', 'doc_name', 'pagenum', 'tablenum',
            'file', 'orig_table_file', 'context', 'row_num',
        ],
        columns=['variable'],
        values='value'
    ).reset_index()
    dataset_dict[kpi] = dataset_kpi

# ### define non-null columns for each KPI to remove irrelevant rows
"""
We can now define a dictionary of non-null columns for each KPI, so that that the rows where the column is empty, 
can be dropped. For example, for 'hazardous waste', we are only interested in rows that provide a 'hazardous waste amount', 
and if this column is null, the rest of the details are largely irrelevant, so we will drop such rows. 
"""
kpi_attrs_nonnull = {
    '% of female representation on the board': ['% of female representation on the board'],
    'hazardous waste': ['hazardous waste amount'],
    'gender pay gap': ['gender pay gap'],
    'GHG Scope 1 emissions': ['amount of emissions'],
    'GHG Scope 2 emissions': ['amount of emissions'],
    'GHG Scope 3 emissions': ['amount of emissions'],
    'Non-renewable energy consumption': ['amount of energy consumption'],
    'Emissions to water': ['amount of emissions to water'],
    'Percentage of non-renewable energy production': ['amount of energy production'],
    'anti-corruption policies': ['summary of anti-corruption policies'],
    'anti-bribery policies': ['summary of anti-bribery policies'],
}
for kpi_num, kpi in enumerate(kpi_attrs.keys()):
    logger.info(f"Filtering datasets for ({kpi_num}/{len(kpi_attrs.keys())}): {kpi}")
    for col in kpi_attrs_nonnull[kpi]:
        df_ = dataset_dict[kpi].copy()
        df_ = df_[(df_[col].notnull()) & (df_[col] != '') & (df_[col] != 'nan')]
        df_ = df_.fillna('')
        dataset_dict[kpi] = df_

# ### check dataset for a few KPIs
"""
Available KPIs, `list(dataset_dict.keys())`
[
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
    'anti-bribery policies'
]
Dataset columns for 'hazardous waste', `list(dataset_dict['hazardous waste'].columns)`
['doc_org', 'doc_org_std', 'doc_year', 'num_pages', 'doc_name', 'pagenum', 'tablenum', 'file', 'orig_table_file', 'context', 'row_num', 'any details of hazardous waste', 'company name', 'date', 'hazardous waste amount', 'unit of measurement']
dataset_dict['hazardous waste'][['doc_org_std', 'company name', 'date', 'hazardous waste amount', 'unit of measurement', 'any details of hazardous waste', 'context']].head().to_dict('records')
[
    {'company name': nan, 'date': nan, 'hazardous waste amount': '30,099', 'unit of measurement': 'tCO2e/year*', 'any details of hazardous waste': nan, 'context': '[["2022; UK", "2022; Total", "2021; UK", "2021; Total", "nan", "nan_2"], ["Scope 1", "tCO2e/year*", "30,099", "302,843", "30,610", "288,438"], ["Scope 2", "tCO2e/year*", "357", "26,977", "2,409", "30,532"], ["Total", "tCO2e/year*", "30,456", "329,820", "33,019", "318,970"], [NaN, NaN, NaN, NaN, NaN, NaN], ["Energy consumption used to calculate emissions", "mWh", "131,148", "1,317,129", "139,912", "1,266,179"]]'}, 
    {'company name': nan, 'date': nan, 'hazardous waste amount': '357', 'unit of measurement': 'tCO2e/year*', 'any details of hazardous waste': nan, 'context': '[["2022; UK", "2022; Total", "2021; UK", "2021; Total", "nan", "nan_2"], ["Scope 1", "tCO2e/year*", "30,099", "302,843", "30,610", "288,438"], ["Scope 2", "tCO2e/year*", "357", "26,977", "2,409", "30,532"], ["Total", "tCO2e/year*", "30,456", "329,820", "33,019", "318,970"], [NaN, NaN, NaN, NaN, NaN, NaN], ["Energy consumption used to calculate emissions", "mWh", "131,148", "1,317,129", "139,912", "1,266,179"]]'}, 
    {'company name': 'US', 'date': 'Recordable accidents', 'hazardous waste amount': '190.0', 'unit of measurement': nan, 'any details of hazardous waste': nan, 'context': '[["2022; OSHA", "2022; RIDDOR", "2021; OSHA", "2021; RIDDOR", "nan"], ["US Recordable accidents", 190.0, 74.0, 194.0, 114.0], ["Incident rate", 0.9, 0.17, 1.07, 0.31], ["Canada Recordable accidents", 25.0, 5.0, 29.0, 8.0], ["Incident rate", 1.49, 0.15, 2.12, 0.29], ["UK Recordable accidents", NaN, 18.0, NaN, 21.0], ["Incident rate", NaN, 0.22, NaN, 0.27], [NaN, NaN, NaN, NaN, NaN]]'}, 
    {'company name': 'US', 'date': 'Incident rate', 'hazardous waste amount': '0.9', 'unit of measurement': nan, 'any details of hazardous waste': nan, 'context': '[["2022; OSHA", "2022; RIDDOR", "2021; OSHA", "2021; RIDDOR", "nan"], ["US Recordable accidents", 190.0, 74.0, 194.0, 114.0], ["Incident rate", 0.9, 0.17, 1.07, 0.31], ["Canada Recordable accidents", 25.0, 5.0, 29.0, 8.0], ["Incident rate", 1.49, 0.15, 2.12, 0.29], ["UK Recordable accidents", NaN, 18.0, NaN, 21.0], ["Incident rate", NaN, 0.22, NaN, 0.27], [NaN, NaN, NaN, NaN, NaN]]'}, 
    {'company name': 'Canada', 'date': 'Recordable accidents', 'hazardous waste amount': '25.0', 'unit of measurement': nan, 'any details of hazardous waste': nan, 'context': '[["2022; OSHA", "2022; RIDDOR", "2021; OSHA", "2021; RIDDOR", "nan"], ["US Recordable accidents", 190.0, 74.0, 194.0, 114.0], ["Incident rate", 0.9, 0.17, 1.07, 0.31], ["Canada Recordable accidents", 25.0, 5.0, 29.0, 8.0], ["Incident rate", 1.49, 0.15, 2.12, 0.29], ["UK Recordable accidents", NaN, 18.0, NaN, 21.0], ["Incident rate", NaN, 0.22, NaN, 0.27], [NaN, NaN, NaN, NaN, NaN]]'}
]
"""
