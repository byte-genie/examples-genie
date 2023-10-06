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
ocr_text_files = [resp.get_data() for resp in ocr_text_files if resp.get_data() is not None]
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
ocr_table_files = [resp.get_data() for resp in ocr_table_files if resp.get_data() is not None]
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

# ## Convert pdf documents to latex
"""
PDF to latex converter uses a specialised OCR that can convert PDF files to a latex markdown format. 
This converts text, tables, and equations into latex code. This is particularly useful for documents that contain equations or mathematical symbols. 
"""

# ### trigger pdf to markdown conversion
doc_names.reverse()
convert_to_markdown_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering pdf to markdown conversion for ({doc_num}/{len(doc_names)}): {doc_name}")
    convert_to_markdown_resp = bg_async.convert_pdf_to_markdown(
        doc_name=doc_name,
        cluster_args={
            'accelerators': 'T4:1',
            'use_spot': False,
        }
    )
    convert_to_markdown_responses = convert_to_markdown_responses + [convert_to_markdown_resp]

# ### list markdown files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=markdown/**.mmd'
    )
    for doc_name in doc_names
]
markdown_files = utils.async_utils.run_async_tasks(tasks)
markdown_files = [resp.get_data() for resp in markdown_files if resp.get_data() is not None]
"""
Number of documents with markdown files, len(markdown_files): 4
markdown files for first two documents, markdown_files[0] + markdown_files[1]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=mmd/variable_desc=markdown/source=app.esgnie.org/jason_08_gpgpdf.mmd', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=mmd/variable_desc=markdown/source=app.esgnie.org/jeon_20_billerudkorsnas_annual-report_2021pdf.mmd'
]
"""

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
missing_text_segment_files = [
    resp.response['payload']['task_1']['task']['args']['doc_name']
    for resp in text_segment_files if resp.get_data() is None
]
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

# ## Extract quant metrics
"""
ByteGenie API has dedicated endpoints for extracting quants in a structured form, from text passages and tables. 
The endpoints allow user to specify any specific attributes to extract in the quantitative dataset. 
By default, these attributes are set to be generic attributes needed to understand quantitative values, i.e. 
(company name, variable description, category, variable, value, unit, date, pagenum, doc_name).  
"""

# ### quant extraction start time
quant_extraction_start_time = time.time()
"""
`quant_extraction_start_time: 1695704723.746251`
"""

# ### Extract quant metrics from passages
tasks = [
    bg_async.async_structure_passage_quants(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=text-segments/**.csv',
        text_col='text',
    )
    for doc_name in doc_names
]
passage_quant_extraction_responses = utils.async_utils.run_async_tasks(tasks)

# ### extract quant metrics from tables
tasks = [
    bg_async.async_structure_tabular_quants(
        doc_name=doc_name,
    )
    for doc_name in doc_names
]
tabular_quant_extraction_responses = utils.async_utils.run_async_tasks(tasks)

# ### check extracted passage quant files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=structured/**/source=passage-quants/**.csv"
    )
    for doc_name in doc_names
]
passage_quant_files = utils.async_utils.run_async_tasks(tasks)
passage_quant_files = [resp.get_data() for resp in passage_quant_files if resp.get_data() is not None]
"""
Documents with passage quant files: `len(passage_quant_files)`: 49
First 5 **passage_quant_files for the first document**
passage_quant_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_contextnum-0_passage-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-4_contextnum-0_passage-quants_structured-quant-summary.csv'
]
"""

# ### check extracted tabular quant files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern=f"data_type=structured/**/source=tabular-quants/**.csv"
    )
    for doc_name in doc_names
]
tabular_quant_files = utils.async_utils.run_async_tasks(tasks)
tabular_quant_files = [resp.get_data() for resp in tabular_quant_files if resp.get_data() is not None]
"""
Number of documents with tabular quant files, `len(tabular_quant_files)`: 48
First 5 **tabular_quant_files for the first document**
tabular_quant_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-1_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-2_contextnum-0_tabular-quants_structured-quant-summary.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-5_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary.csv'
]
"""

# ### read a passage quant file
df_passage_quants_sample = bg_sync.read_file(
    file=passage_quant_files[0][1]
).get_data()
df_passage_quants_sample = pd.DataFrame(df_passage_quants_sample)
df_passage_quants_sample = df_passage_quants_sample[df_passage_quants_sample['value'] != '']
"""
df_passage_quants_sample columns: `list(df_passage_quants_sample.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'pagenum', 'relevant quote', 'unit', 'value', 'variable', 'variable description']
check a **short sample of df_passage_quants_sample**
`df_passage_quants_sample[['company name', 'category', 'variable description', 'variable', 'unit', 'value', 'date', 'pagenum', 'doc_name']].head().to_dict('records')`
[
    {'company name': 'American Express', 'category': 'Number of women in', 'variable description': 'The number of women in first level manager roles has increased by one-third in the last five years.', 'variable': 'First level manager', 'unit': '', 'value': '32%', 'date': '', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': 'American Express', 'category': 'Gender pay gap', 'variable description': 'There has been almost a 2 percentage point improvement year-on-year in the gender pay gap.', 'variable': 'Improvement', 'unit': '', 'value': '2 percentage', 'date': 'year-on-year', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': 'American Express', 'category': 'Gender diversity', 'variable description': 'The number of women in senior management positions now stands at 47%.', 'variable': 'Senior management', 'unit': '', 'value': '47%', 'date': '', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""

# ### read a tabular quant file
df_tabular_quants_sample = bg_sync.read_file(
    file=tabular_quant_files[0][1]
).get_data()
df_tabular_quants_sample = pd.DataFrame(df_tabular_quants_sample)
df_tabular_quants_sample = df_tabular_quants_sample[df_tabular_quants_sample['value'] != '']
"""
df_tabular_quants_sample columns: `list(df_tabular_quants_sample.columns)`
['category', 'company name', 'context', 'date', 'doc_name', 'pagenum', 'relevant quote from text', 'unit', 'value', 'variable', 'variable description']
check a **short sample of df_tabular_quants_sample**
`df_tabular_quants_sample[['company name', 'category', 'variable description', 'variable', 'unit', 'value', 'date', 'pagenum', 'doc_name', 'context']].head().to_dict('records')`
[
    {'company name': '', 'category': 'GENDER', 'variable description': '', 'variable': 'MEAN', 'unit': '', 'value': '14.7%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': 'GENDER', 'variable description': '', 'variable': 'MEDIAN', 'unit': '', 'value': '16.7%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': '% W/M', 'variable description': '', 'variable': 'WOMEN', 'unit': '', 'value': '55%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'company name': '', 'category': '% W/M', 'variable description': '', 'variable': 'MEN', 'unit': '', 'value': '45%', 'date': '', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""
#
# # ## Verify extracted quants info
# """
# After the quant extraction, we can run a data verification layer to remove any values that may be incorrectly extracted.
# """
#
# # ### start time for quant value verification
# verify_value_start_time = time.time()
# """
# verify_value_start_time
# 1695790862.0567858
# """
#
# # ### verify extracted quant values
# tasks = [
#     bg_async.async_verify_data(
#         doc_name=doc_name,
#         file_pattern='data_type=structured/**/variable_desc=structured-quant-summary/**.csv',
#     )
#     for doc_name in doc_names
# ]
# verify_value_responses = utils.async_utils.run_async_tasks(tasks)
#
# # ### list verified quant value files
# tasks = [
#     bg_sync.async_list_doc_files(
#         doc_name=doc_name,
#         file_pattern='data_type=structured/**/variable_desc=verified-quants/**.csv',
#     )
#     for doc_name in doc_names
# ]
# verify_value_files = utils.async_utils.run_async_tasks(tasks)
# verify_value_files = [resp.get_data() for resp in verify_value_files if resp.get_data() is not None]
# """
# Number of documents with verified quant value files: `len(verify_value_files)`: 48
# **verified value files** for the first document: `verify_value_files[0]`
# [
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary_verified.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary_verified.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-1_contextnum-0_tabular-quants_structured-quant-summary_verified.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-2_contextnum-0_tabular-quants_structured-quant-summary_verified.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-6_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary_verified.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary_verified.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_tablenum-1_contextnum-0_tabular-quants_structured-quant-summary_verified.csv'
# ]
# """
#
# # ### start time for verifying company names
# verify_company_start_time = time.time()
# """
# verify_company_start_time
# 1695795411.6313999
# """
#
# # ### verify extracted company names
# tasks = [
#     bg_async.async_verify_quants_company_info(
#         doc_name=doc_name,
#         file_pattern='data_type=structured/**/variable_desc=verified-quants/**.csv',
#     )
#     for doc_name in doc_names
# ]
# verify_company_responses = utils.async_utils.run_async_tasks(tasks)
#
# # ### list verified company name and quant value files
# tasks = [
#     bg_sync.async_list_doc_files(
#         doc_name=doc_name,
#         file_pattern='data_type=structured/**/variable_desc=verified-company-quants/**.csv',
#     )
#     for doc_name in doc_names
# ]
# verify_company_quant_files = utils.async_utils.run_async_tasks(tasks)
# verify_company_quant_files = [resp.get_data() for resp in verify_company_quant_files if resp.get_data() is not None]
# """
# Number of documents with verified company and quant files, `len(verify_company_quant_files)`: 46
# First 5 verified company name and quant value files for the first document: verify_company_quant_files[0][:5]
# [
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-company-quants/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary_verified_verified-company-names.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-company-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary_verified_verified-company-names.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-company-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_tablenum-1_contextnum-0_tabular-quants_structured-quant-summary_verified_verified-company-names.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-company-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-6_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary_verified_verified-company-names.csv',
#     'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=verified-company-quants/source=tabular-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-7_tablenum-0_contextnum-0_tabular-quants_structured-quant-summary_verified_verified-company-names.csv'
# ]
# """
#
# # ## Find missing files
# """
# At any step of the processing, we can check for any documents that are missing in the current output.
# To do so, we can get document names for the available output files, and compare them with our initial list of document names.
# """
#
# # ### flatten verify_value_files
# verify_value_files = [file for doc_files in verify_value_files for file in doc_files]
#
# # ### get doc_name for verify_value_files
# verify_value_doc_names = [utils.common.get_doc_name(file) for file in verify_value_files]
# verify_value_doc_names = list(set(verify_value_doc_names))
#
# # ### find missing doc_names
# missing_doc_names_from_verify_value = [
#     doc_name for doc_name in doc_names
#     if doc_name not in verify_value_doc_names
# ]
# """
# document names missing from verified quant value files are: `missing_doc_names_from_verify_value`
# [
#     'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf',
#     'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf',
#     'userid_stuartcullinan_uploadfilename_28_kim_cartapdf',
#     'userid_stuartcullinan_uploadfilename_1_accor_mrpdf',
#     'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf',
#     'userid_stuartcullinan_uploadfilename_jason_08_srpdf',
#     'userid_stuartcullinan_uploadfilename_12_argo_blockchainpdfpdf',
#     'userid_stuartcullinan_uploadfilename_karishma-04-sustainability-highlights-report-2021-19-finalpdf',
#     'userid_stuartcullinan_uploadfilename_karishma-01-des-annualreport-2021-e-spdf'
# ]
# """

#
# # ## Synthesize quant data with document meta-data
# """
# Now we can combine the document-level info with the specific quantitative info extracted from the document, as the document info
# can help determine the dates or company names for quant values when not provided directly in passages or tables.
# For this, we will use `/synthesize_quant_data` endpoint, which will synthesize quant data with document meta-data,
# and also create embeddings for each row of the data, which can later be used to run semantic searches.
# """
#
# # ### quant synthesis start time
# quant_synthesis_start_time = time.time()
# """
# quant_synthesis_start_time
#
# """
#
# # ### define asynchronous tasks for synthesizing quant data
# tasks = [
#     bg_async.async_synthesize_quant_data(
#         doc_name=doc_name,
#     )
#     for doc_name in doc_names
# ]
# quant_synthesis_responses = utils.async_utils.run_async_tasks(tasks)

# ## Vectorise quant data for semantic searching

# ### set columns to embed
cols_to_embed = ['category', 'company name', 'date', 'unit', 'value', 'variable', 'variable description']

# ### embed quant files
bg_async.overwrite = 1
bg_async.overwrite_base_output = 1
tasks = [
    bg_async.async_embed_doc_data(
        doc_name=doc_name,
        file_pattern='data_type=structured/**/variable_desc=structured-quant-summary/**.csv',
        cols_to_use=cols_to_embed,
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

# ### list embedding files for quants
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=embeddings/**/variable_desc=structured-quant-summary/**.csv',
    )
    for doc_name in doc_names
]
embed_doc_files = utils.async_utils.run_async_tasks(tasks)
embed_doc_files = [resp.get_data() for resp in embed_doc_files if resp.get_data() is not None]
"""
Number of documents with embedding files, len(embed_doc_files): 49
First 5 embedding files for the first document: embed_doc_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_similarity_query-emissions-by-scope_embeddings.csv'
]
"""

# ## Vectorise text segments for semantic searching

# ### set columns to embed
cols_to_embed = ['text']

# ### embed text files
tasks = [
    bg_async.async_embed_doc_data(
        doc_name=doc_name,
        file_pattern='data_type=semi-structured/**/variable_desc=text-segments/**.csv',
        cols_to_use=cols_to_embed,
    )
    for doc_name in doc_names
]
text_emb_responses = utils.async_utils.run_async_tasks(tasks)

# ### list embedding files for text segments
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=embeddings/**/variable_desc=text-segments/**.csv',
    )
    for doc_name in doc_names
]
embed_text_files = utils.async_utils.run_async_tasks(tasks)
embed_text_files = [resp.get_data() for resp in embed_text_files if resp.get_data() is not None]
"""
Number of documents with embedding files: len(embed_text_files): 49
First 5 embedding files for the first documnet: embed_text_files[0][:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-1_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-2_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-3_text-blocks_text-segments_embeddings.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=embeddings/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-4_text-blocks_text-segments_embeddings.csv'
]
"""

# ## Rank quants by relevance to keyphrases
"""
Once we have the quant metrics extracted and structured, we can rank them by relevance to the KPIs to filter out the most relevant data.
"""

# ### set attributes to extract

## KPIs for which we want quantitative data
quant_kpis = [
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

# ### score quant data similarity
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"running similarity scoring for ({doc_num}/{len(doc_names)}): {doc_name}")
    try:
        tasks = [
            bg_async.async_score_doc_text_similarity(
                doc_name=doc_name,
                file_pattern='data_type=embeddings/**/variable_desc=structured-quant-summary/**.csv',
                query=query,
            )
            for query in quant_kpis
        ]
        score_quant_sim_responses = utils.async_utils.run_async_tasks(tasks)
    except Exception as e:
        logger.warning(f"Error running similarity scoring for: {doc_name}")
    ## wait for 15 sec before starting next document to avoid rate limit errors
    time.sleep(15)

# ## Rank text by relevance to keyphrases
"""
Once we have the text segments extracted from documents, we can rank them by relevance to the KPIs to filter out the most relevant data.
"""

# ### set attributes to extract

## KPIs for which we want qualitative data
qual_kpis = [
    'anti-corruption policies',
    'anti-bribery policies',
]

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
            for query in qual_kpis
        ]
        score_quanl_sim_responses = utils.async_utils.run_async_tasks(tasks)
    except Exception as e:
        logger.warning(f"Error running similarity scoring for: {doc_name}")
    ## wait for 15 sec before starting next document to avoid rate limit errors
    time.sleep(15)

# ## Filter out quant data most relevant to KPIs

# ### read similarity scored files
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=structured-quant-summary/**.csv',
    )
    for doc_name in doc_names
]
sim_score_files = utils.async_utils.run_async_tasks(tasks)
sim_score_files = [resp.get_data() for resp in sim_score_files if resp.get_data() is not None]
"""
Number of documents with sim_score_files, `len(sim_score_files)`: 49
First 5 sim_score_files for the first document, `sim_score_files[0][:5]`
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-anti-bribery-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-anti-corruption-policies.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-emissions-to-water.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-gender-pay-gap.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_embeddings_embeddings_embeddings_similarity_query-ghg-scope-1-emissions.csv'
]
"""
## flatten sim_score_files
sim_score_files = [file for doc_files in sim_score_files for file in doc_files]
"""
Total number of sim_score_files across all documents, `len(sim_score_files)`: 119732
"""

# ### create a dataframe of files by KPI
df_sim_files = pd.DataFrame()
df_sim_files['file'] = sim_score_files
df_sim_files['query'] = [os.path.splitext(file.split('/')[-1].split('query-')[-1])[0] for file in sim_score_files]
"""
Unique queries/KPIs for which we have similarity scored files, `list(df_sim_files['query'].unique())`
[
    'anti-bribery-policies', 
    'anti-corruption-policies', 
    'emissions-to-water', 
    'gender-pay-gap', 
    'ghg-scope-1-emissions', 
    'ghg-scope-2-emissions', 
    'ghg-scope-3-emissions', 
    'hazardous-waste', 
    'non-renewable-energy-consumption', 
    'of-female-representation-on-the-board', 
    'percentage-of-non-renewable-energy-production', 
    'emissions-by-scope'
]
First 5 files for 'ghg-scope-1-emissions', df_sim_files[df_sim_files['query'] == 'ghg-scope-1-emissions']['file'].unique().tolist()[:5]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-0_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-2_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-3_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-4_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-ghg-scope-1-emissions.csv'
]
"""

# ### save files locally
df_sim_files.to_csv(f"/tmp/df_sim_files.csv", index=False)

# ### read from local file
df_sim_files = pd.read_csv(f"/tmp/df_sim_files.csv")

# ### filter similarity scored quant files to keep the most relevant rows

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=structured-quant-summary/**.csv',
        non_null_cols=['value'],
        groupby_cols=['query'],
        max_rows_to_keep=20,
        max_frac_rows_to_keep=0.1,
        filename_sfx='quant-kpi-01',
    )
    for doc_name in doc_names
]
## run tasks
filtered_quant_responses = utils.async_utils.run_async_tasks(tasks)
## read filtered quants
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=similarity/**/variable_desc=filtered-data/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quants_filtered = utils.async_utils.run_async_tasks(tasks)
df_quants_filtered = [resp.get_output() for resp in df_quants_filtered]
df_quants_filtered = [pd.DataFrame(df) for df in df_quants_filtered]
df_quants_filtered = pd.concat(df_quants_filtered)
## drop unwanted columns
if 'context' in df_quants_filtered.columns:
    df_quants_filtered = df_quants_filtered.drop(columns=['context'])
## sort by (query, score)
df_quants_filtered = \
    df_quants_filtered.sort_values(['query', 'score'], ascending=False).reset_index(drop=True)
## re-arrange columns
df_quants_filtered = df_quants_filtered[[
    'query', 'score', 'category', 'company name',
    'variable description', 'variable', 'value', 'date', 'unit',
    'pagenum', 'doc_name', 'file',
]]
## save filtered_text_data_dict locally
df_quants_filtered.to_csv(f"/tmp/df_quants_filtered.csv", index=False)
"""
Number of rows in df_quants_filtered, len(df_quants_filtered): 61518
df_quants_filtered.head().to_dict('records')
[
    {'query': 'hazardous waste', 'score': 0.8145144484413344, 'category': 'Waste', 'company name': nan, 'variable description': 'Hazardous waste generated from the manufacturing process in tonnes', 'variable': 'Hazardous waste', 'value': '27.0', 'date': '2021', 'unit': 'tonnes', 'pagenum': 112, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-112_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.8074640482800609, 'category': 'Waste Recycling', 'company name': 'VINCI Energies', 'variable description': '"VINCI Energies divisions that were part of the reporting scope in 2021 achieved recycling rates of 69% for hazardous waste."', 'variable': 'Hazardous Waste Rate', 'value': '69%', 'date': '2021.0', 'unit': nan, 'pagenum': 120, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf_pagenum-120_contextnum-1_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.798367934167774, 'category': 'Waste', 'company name': nan, 'variable description': 'Non-hazardous waste generated from the manufacturing process in tonnes', 'variable': 'Process waste', 'value': '135.0', 'date': '2021', 'unit': 'tonnes', 'pagenum': 112, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-112_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.7958005310728269, 'category': 'Waste Recovery', 'company name': 'VINCI Construction Central Europe', 'variable description': '"At VINCI Construction, only the Central Europe division is included in the scope for waste recovered, with recovery rates of 31% for hazardous waste."', 'variable': 'Hazardous Waste Rate', 'value': '31%', 'date': nan, 'unit': nan, 'pagenum': 120, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf_pagenum-120_contextnum-1_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}, 
    {'query': 'hazardous waste', 'score': 0.7920064789552598, 'category': 'Environment', 'company name': 'Aker Carbon', 'variable description': 'Total amount of hazardous waste generated in 2021', 'variable': 'Hazardous waste generated', 'value': '0.002', 'date': '2021.0', 'unit': 'Tons', 'pagenum': 104, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf/data_type=similarity/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf_pagenum-104_contextnum-0_passage-quants_structured-quant-summary_embeddings_similarity_query-hazardous-waste.csv'}
]
"""

# ### filter similarity-scored text files to keep the most relevant rows

## create tasks
tasks = [
    bg_async.async_filter_similarity_scored_data(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=text-segments/**.csv',
        non_null_cols=['text'],
        groupby_cols=['query'],
        max_rows_to_keep=20,
        max_frac_rows_to_keep=0.1,
        filename_sfx='qual-kpi-01',
    )
    for doc_name in doc_names
]
## run tasks
filtered_text_responses = utils.async_utils.run_async_tasks(tasks)
## read filtered text
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=similarity/**/variable_desc=filtered-data/**qual-kpi-01.csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_text_filtered = utils.async_utils.run_async_tasks(tasks)
df_text_filtered = [resp.get_output() for resp in df_text_filtered]
df_text_filtered = [pd.DataFrame(df) for df in df_text_filtered]
df_text_filtered = pd.concat(df_text_filtered)
## drop unwanted columns
if 'context' in df_text_filtered.columns:
    df_text_filtered = df_text_filtered.drop(columns=['context'])
## filter over relevant queries
df_text_filtered = df_text_filtered[df_text_filtered['query'].isin(qual_kpis)]
## sort by score
df_text_filtered = df_text_filtered.sort_values(['query', 'score'], ascending=False).reset_index(drop=True)
## drop unwanted columns
if 'context' in df_text_filtered.columns:
    df_text_filtered = df_text_filtered.drop(columns=['context'])
## re-arrange columns
df_text_filtered = df_text_filtered[['query', 'score', 'text', 'pagenum', 'doc_name', 'file']]
## save data locally
df_text_filtered.to_csv(f"/tmp/df_text_filtered.csv", index=False)
"""
Number of rows in df_quants_filtered, len(df_text_filtered): 30206
df_text_filtered.head().to_dict('records')
[
    {'query': 'anti-corruption policies', 'score': 0.871208445921234, 'text': 'Anti- corruption Policy', 'pagenum': 112, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_9_2021-annual-report_compressedpdf_pagenum-112_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.8698307926193798, 'text': 'Anti-corruption: the anti-corruption principles to be adhered to by employees, based on the fundamental tenet of "zero tolerance".', 'pagenum': 113, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_9_2021-annual-report_compressedpdf_pagenum-113_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.863275482062681, 'text': 'Anti-Corruption', 'pagenum': 106, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_09_srpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_09_srpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/jason_09_srpdf_pagenum-106_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.8528518716714095, 'text': "VINCI's anti-corruption arrangements", 'pagenum': 110, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_8_vinci-2021-universal-registration-documentpdf_pagenum-110_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}, 
    {'query': 'anti-corruption policies', 'score': 0.8526271061198671, 'text': 'Anti-corruption Code of Conduct', 'pagenum': 110, 'doc_name': 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf/data_type=similarity/format=csv/variable_desc=text-segments/source=layout-genie/al_8_vinci-2021-universal-registration-documentpdf_pagenum-110_text-blocks_text-segments_embeddings_similarity_query-anti-corruption-policies.csv'}
]
"""

# ## Retrieve evidence for all filtered values
"""
We can use `/trace_evidence` endpoint to trace evidence for any extracted or derived data. `/trace_evidence` takes a document name and file pattern as inputs,  
and determines where these files lie in the processing pipeline, and which previous output is relevant for contextualising the data in these files. 
For example, for similarity-scored data, it will fetch the base structured data (before any vectorisation and similarity scoring), 
and original page image, from which all the data in the similarity-score files are derived. 
`/trace_evidence` call will write new files with `data_type=evidence`. These files will contain the relevant evidence for each extracted row of the data.  
"""

# ### trace evidence for quant files

## define tasks
tasks = [
    bg_async.async_trace_evidence(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=filtered-data/**quant-kpi-01*csv',
    )
    for doc_name in doc_names
]
## run tasks
quant_evidence_responses = utils.async_utils.run_async_tasks(tasks)

# ### read quant evidence files
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=evidence/**/variable_desc=filtered-data/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quant_evidence = utils.async_utils.run_async_tasks(tasks)
df_quant_evidence = [resp.get_output() for resp in df_quant_evidence]
df_quant_evidence = [pd.DataFrame(df) for df in df_quant_evidence]
df_quant_evidence = pd.concat(df_quant_evidence)
"""
Number of documents for which quant evidence files are available, `len(df_quant_evidence['doc_name'].unique())`: 49
df_quant_evidence columns, `list(df_quant_evidence.columns)`
[
    'query', 'score', 'company name',  'category',  
    'variable description', 'variable', 'date', 'unit', 'value',   
    'context', 'context_file', 'img_file', 'pagenum', 'doc_name', 
]
AS we can see, /trace_evidence has added `context`, `context_file`, and `img_file` columns to the data. 
* `context` contains the text extracted from the page (before structuring); 
* `context_file` contains the file path for the file containing the extracted text;
* `img_file` is the path to the page image. 
This will allow us to see the full details of the context from the relevant was extracted, and use these additional details to correct any mistakes in the extraction.
"""
## re-arrange columns
df_quant_evidence = df_quant_evidence[[
    'query', 'score', 'category', 'date',
    'unit', 'value', 'variable', 'variable description',
    'context', 'context_file', 'img_file', 'pagenum', 'doc_name',
]]
"""
Snapshot of quants data merged with evidence from where it was extracted, `df_quant_evidence.head().to_dict('records')`
[
    {'query': 'anti-bribery policies', 'score': 0.7446372728941727, 'category': 'Inclusion and Recognition', 'date': nan, 'unit': nan, 'value': '', 'variable': 'Awards', 'variable description': 'Recognition by Working Families as a Top 10 Family Friendly Employer, for the tenth year running', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7374088264758053, 'category': 'Inclusion and Recognition', 'date': nan, 'unit': nan, 'value': '', 'variable': 'Awards', 'variable description': 'Recognition by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7311989710999165, 'category': 'Gender Pay Gap', 'date': nan, 'unit': nan, 'value': '', 'variable': 'Gender Pay Gap', 'variable description': "Company's commitment to achieving a greater gender balance and the primary reason for the gender pay gap", 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7240244921672689, 'category': 'Gender Pay Gap Improvement', 'date': nan, 'unit': nan, 'value': '2% improvement', 'variable': 'Gender Pay Gap', 'variable description': 'Year-on-year improvement in the gender pay gap', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7239204026679752, 'category': 'Gender Diversity', 'date': nan, 'unit': nan, 'value': '47%', 'variable': 'Women in Manager', 'variable description': 'Number of women in senior management positions', 'context': '"Reporting Our Progress \\n\\n At American Express we\'re committed to creating an inclusive, equitable and diverse working environment for all. It\'s been another testing year operating through the uncertainties of the global pandemic, with the majority of our colleagues worldwide working from home. \\n\\n Over this time, it\'s testament to the strength of Team Amex that we\'ve been able to enhance our \\n\\n Number of women in first level manager roles \\n\\n 32% \\n\\n 1 \\n\\n 5 \\n\\n TERR 2 \\n\\n 3 \\n\\n 4 \\n\\n culture of inclusion, and strong sense of colleague community. We\'ve continued to invest in hiring, engaging and developing the careers of our women colleagues, as well as provided benefits and flexibility that support inclusion and allyship. As we\'ve done this, we\'re proud to have been externally recognised by Linkedln Top Companies List 2021 as one of the best 25 workplaces in the UK for career growth, and by Working Families as a Top 10 Family Friendly Employer, for the tenth year running. \\n\\n In terms of our gender pay gap, there has been almost a 2 percentage point improvement year-on-year and we remain committed to achieving a greater gender balance at all levels within the company. The composition of our workforce remains the primary reason for our gender pay gap, as we have more women in non-senior positions and more men in senior positions. We have made continued efforts to advance into leadership more women which, in the last five years, has meant the number of women in first level manager roles has increased by one-third and the number of women in senior management positions now stands at 47%. \\n\\n 2 \\n\\n Charlotte Duerden \\n\\n EXECUTIVE VICE PRESIDENT, ICS CHIEF CUSTOMER OFFICER \\n\\n UK COUNTRY MANAGER (MAY 2018 - FEBRUARY 2022) \\n\\n COLLEAGUE SINCE 2000 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jason_08_gpgpdf_pagenum-1_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-1.png', 'pagenum': 1, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""
## write data locally
df_quant_evidence.to_csv(f"/tmp/df_quant_evidence.csv", index=False)

# ### evidence tracing for text files

## tasks
tasks = [
    bg_async.async_trace_evidence(
        doc_name=doc_name,
        file_pattern='data_type=similarity/**/variable_desc=filtered-data/**qual-kpi-01*csv',
    )
    for doc_name in doc_names
]
## run tasks
text_evidence_responses = utils.async_utils.run_async_tasks(tasks)

# ### read text evidence files
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=evidence/**/variable_desc=filtered-data/**qual-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_text_evidence = utils.async_utils.run_async_tasks(tasks)
df_text_evidence = [resp.get_output() for resp in df_text_evidence]
df_text_evidence = [pd.DataFrame(df) for df in df_text_evidence]
df_text_evidence = pd.concat(df_text_evidence)
## re-arrange columns
df_text_evidence = df_text_evidence[
    ['query', 'score', 'text', 'context_file', 'img_file', 'pagenum', 'doc_name']
]
"""
Number of documents for which text evidence files are available, `len(df_text_evidence['doc_name'].unique())`: 49
df_text_evidence columns, `list(df_text_evidence.columns)`
['query', 'score', 'text', 'context_file', 'img_file', 'pagenum', 'doc_name']
df_text_evidence.head().to_dict('records')
[
    {'query': 'anti-bribery policies', 'score': 0.7144075502491216, 'text': 'AMERICAN', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7003961798659838, 'text': 'EXPRESS', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-bribery policies', 'score': 0.7341672165364279, 'text': 'UK Gender Pay Gap REPORT 2021', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-corruption policies', 'score': 0.7057159443441187, 'text': 'AMERICAN', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}, 
    {'query': 'anti-corruption policies', 'score': 0.6886003988493039, 'text': 'EXPRESS', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=semi-structured/format=csv/variable_desc=text-segments/source=layout-genie/jason_08_gpgpdf_pagenum-0_text-blocks_text-segments.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jason_08_gpgpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jason_08_gpgpdf_pagenum-0.png', 'pagenum': 0, 'doc_name': 'userid_stuartcullinan_uploadfilename_jason_08_gpgpdf'}
]
"""
## write data locally
df_text_evidence.to_csv(f"/tmp/df_text_evidence.csv", index=False)

# ## Verify filtered data
"""
To verify any extracted data, we can use `/verify_data` endpoint, which takes variable, value and context column names as inputs, allows us to verify whether the extracted (variable, value) pair is consistent with what's in the context column. 
That is the context column is essentially the original source data from which (variable, value) pair was extracted, but due to extraction errors, (variable, value) pair may have been extracted incorrectly. 
`/verify_data` is meant to flag such errors, so we can handle these errors separately.
"""

# ### Verify extracted quant values
tasks = [
    bg_async.async_verify_data(
        doc_name=doc_name,
        file_pattern='data_type=evidence/**/variable_desc=filtered-data/**quant-kpi-01*csv',
        var_col='variable',
        val_col='value',
        verification_type='variable-value',
        context_col='context',
    )
    for doc_name in doc_names
]
verify_quants_responses = utils.async_utils.run_async_tasks(tasks)

# ### read verified quant files
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=verification/**/variable_desc=verified-variable-values/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quant_verified = utils.async_utils.run_async_tasks(tasks)
df_quant_verified = [resp.get_output() for resp in df_quant_verified]
df_quant_verified = [pd.DataFrame(df) for df in df_quant_verified]
df_quant_verified = pd.concat(df_quant_verified)
## sort data by score
df_quant_verified = df_quant_verified.sort_values(['query', 'score'], ascending=False).reset_index(drop=True)
## drop unwanted columns
if 'row_id' in df_quant_verified.columns:
    df_quant_verified = df_quant_verified.drop(columns=['row_id'])
## re-arrange columns
df_quant_verified = df_quant_verified[[
    'query', 'score', 'company name', 'category', 'variable description', 'variable', 'value', 'date', 'unit',
    'fuzzy_verification_flag', 'lm_verification_flag',
    'context', 'context_file', 'img_file', 'pagenum', 'doc_name'
]]
"""
Number of documents for which verified quant files are available, `len(df_quant_verified['doc_name'].unique())`: 48
Number of rows in df_quant_verified, `len(df_quant_verified)`: 10600
Number of documents for each KPI 
`df_quant_verified.groupby(['query']).apply(lambda x: len(x['doc_name'].unique())).reset_index().values.tolist()`
[
    ['% of female representation on the board', 40], 
    ['Emissions to water', 41], 
    ['GHG Scope 1 emissions', 38], 
    ['GHG Scope 2 emissions', 37], 
    ['GHG Scope 3 emissions', 40], 
    ['Non-renewable energy consumption', 38],
    ['Percentage of non-renewable energy production', 39], 
    ['anti-bribery policies', 47], 
    ['anti-corruption policies', 45], 
    ['emissions by scope', 1], 
    ['gender pay gap', 41], 
    ['hazardous waste', 38]
]
Verified quants for a specific KPI, df_quant_verified[df_quant_verified['query'] == 'Percentage of non-renewable energy production'].head().to_dict('records')
[
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8317343036066032, 'company name': '', 'category': 'Renewable Energy', 'variable description': 'At the Noginsk warehouse, at least 25% of electricity consumption comes from renewable sources, resulting in a significant positive impact on net carbon emissions.', 'variable': 'Carbon Emissions', 'value': 'At least 25%', 'date': '', 'unit': '', 'fuzzy_verification_flag': False, 'lm_verification_flag': True, 'context': '"On site renewable energy case studies \\n\\n The Rostov solar farm and hydro electric power case studies outlined in Case Study 1 and 2 below highlight some of the positive steps the Group is taking to explore the adoption of renewable energy sources in its portfolio. \\n\\n 44 \\n\\n 1. ROSTOV SOLAR FARM \\n\\n Meteorological conditions in Russia are such that, in the majority of locations, the number of sunlight hours and levels of snowfall mean that solar generated power is not practical or financially viable. Our Rostov property is geographically far enough south to have limited snowfall and sufficient sun light hours to make solar energy feasible and we have designed a pilot scheme which will generate \\n\\n 1,257MW PER ANNUM, OR 12% OF OUR TENANTS\' POWER REQUIREMENT, EQUIVALENT TO POWERING 550 AVERAGE FAMILY HOMES IN THE LOCAL ROSTOV AREA. \\n\\n There have been numerous challenges in developing a framework to implement the project, but we hope to be on site in the next month and have commenced power generation by early summer. We anticipate this investment will pay back within ten years. \\n\\n If the project is successful we will explore further opportunities across the portfolio, to roll out similar or smaller systems to supplement existing power consumption. \\n\\n 2. HYDRO ELECTRICITY - NOGINSK, MOSCOW \\n\\n We have transferred the electrical supply to our Noginsk warehouse to RusHydro, meaning that \\n\\n AT LEAST 25% OF THE ELECTRICITY WE CONSUME AT THE PROJECT WILL COME FROM RENEWABLE SOURCES. \\n\\n 60% of the Noginsk warehouse is used for chilled and cold storage meaning its energy requirements per square metre is one of the highest in our portfolio. Changing supplier on this site to a renewable provider has had a significant positive impact on net carbon emissions. \\n\\n Changing power supply in Russia is a complex and time consuming task particularly as renewable power for our properties is only available via the wholesale electricity market. Moving to the wholesale market can take up to eight months with requisite notice periods and the necessity for the new supplier to perform a detailed audit and inspection of the electrical distribution and metering system which in turn is then subject to validation by the wholesale market trading administrator. \\n\\n ENVIRONMENTAL AND SOCIAL REPORT \\n\\n Rostov Solar Farm \\n\\n Rostov Solar Farm \\n\\n Noginsk fit out works \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf_pagenum-3_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf_pagenum-3.png', 'pagenum': 3, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8249588034888888, 'company name': '', 'category': 'Renewable Energy', 'variable description': 'Percentage of electricity purchased from renewable sources for the reporting period.', 'variable': 'Electricity Source', 'value': '64%', 'date': 'FY2021', 'unit': '%', 'fuzzy_verification_flag': False, 'lm_verification_flag': True, 'context': '"Streamlined Energy and Carbon Reporting \\n\\n This statement has been prepared in accordance with our regulatory obligation to report greenhouse gas (GHG) emissions pursuant to the Companies (Directors\' Report) and Limited Liability Partnerships (Energy and Carbon Report) Regulations 2018 which implement the government\'s policy on Streamlined Energy and Carbon Reporting. \\n\\n During the reporting period January 2021 to December 2021, our measured Scope 1 and 2 emissions (location-based) totalled 4,516 tCO2 \\n\\n 114 \\n\\n FY2020 Scope UK Rest of world Total Scope 1 1,121 Scope 2 - location-based 2,074 1,712 3,786 Scope 2 - market-based 0 1,798 1,798 Total Scope 1 & 2 (Location-Based) 2,074 1,712 4,907 Total Scope 1 & 2 (Market-Based) 0 1,798 2,919 Scope & 2 intensity per FTE (Location-Based) * * 0.4 Scope 3 * * 535 \\n\\n Overall, our Scope 1 and 2 emissions have decreased by 8% against last year. This was due to improved control in our Building Management Systems in our largest locations. We purchase 64% of our electricity from renewable sources, meaning our Scope 1 and 2 market- based emissions were 2,834 tCO2e, a decrease of 3% from last year. \\n\\n Complete energy data for the whole New emissions reporting - tracking progress group so that verified data isn\'t towards target. based on assumptions: Identify carbon heavy and/or identifying - Fugitives assets and their life cycle for replacement. - Water - Commuting Engage Arup to assist with mapping the route to net zero carbon. 2021 2022 \\n\\n Environmental Policy put in place to better measure, record and reduce the company Greenhouse Gas emissions. Responsible Investment Policy integrated in place to put the achieve more sustainable long-term returns. Admiral Baseline Emission verified by Carbon Trust. To be re-visited part of the Science Based Target initiative. as \\n\\n 2019 \\n\\n Action areas 2019 \\n\\n 2020 \\n\\n Energy \\n\\n Admiral Group plc Annual Report and Accounts 2021 \\n\\n FY2021 UK Rest of world Total 1,285 192 1,477 1,768 1,272 3,039 25 1,332 1,357 3,053 1,463 4,516 1,310 1,523 2,834 0.4 0.4 0.4 435 517 952 \\n\\n The impact of Covid has resulted in The building management within the working from home being adopted UK sites Newport, Cardiff and Swansea as the norm, with the offices being are controlled by Building Management kept within statutory and regulatory System (BMS) which are actively compliance requirements. This has monitored for performance optimisation naturally resulted in a reduction of utility and time schedule efficiency, and with usage and driven a floor space reduction the requirement to introduce greater which has further increased the energy amounts of fresh air into the buildings or utility savings. \\n\\n 2022-23 Continue to engage with Carbon Trust in verifying the data and expand into Scope 3. 2023 \\n\\n A Sustainability Working Group was established in 2020 to provide additional governance support around matters related to ESG. \\n\\n 2025 \\n\\n Continuous and transparent emissions reporting tracking progress towards the targets. \\n\\n Travel \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf_pagenum-115_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/jaime_admiral-group_annual-reportpdf_pagenum-115.png', 'pagenum': 115, 'doc_name': 'userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8185137500421, 'company name': 'Adani Ports and Special Economic Zone Ltd', 'category': 'Grid Energy (in %)', 'variable description': 'Percentage of non-renewable energy consumption in grid energy', 'variable': 'Non-Renewable Energy', 'value': '52', 'date': 'FY 16', 'unit': '%', 'fuzzy_verification_flag': False, 'lm_verification_flag': False, 'context': '"Adani Ports and Special Economic Zone Limited \\n\\n 32 \\n\\n Types of fuel and coolant consumed during FY 19 \\n\\n 1,055 kg 30kg 2.5 kg R22 R410 R134 \\n\\n Energy \\n\\n FY 19 \\n\\n FY 17 \\n\\n FY 16 \\n\\n FY \\n\\n 27.8 ML \\n\\n Diesel \\n\\n 18 \\n\\n 52.8 kL \\n\\n Petrol \\n\\n Mix \\n\\n 4.3KL \\n\\n Furnace Oil \\n\\n There is also a significant impact that we face in terms of cost of acquisition of power 8 fuel. Over the years, the per capita cost has increased further putting pressure on our profitability. So, switching to a cleaner fuel may be of strategic importance for us in the longer term. \\n\\n 82 kg 2,317 kg R407C Acetylene \\n\\n Fuel \\n\\n 28,272 kg LPG \\n\\n Standalone (in %) \\n\\n 87% \\n\\n 39% \\n\\n 57% \\n\\n 58% \\n\\n 42% \\n\\n 59% \\n\\n 43% \\n\\n 5% \\n\\n 14,139 SCM PNG \\n\\n FY 16 \\n\\n Grid \\n\\n Energy \\n\\n FY 19 \\n\\n FY 18 \\n\\n FY \\n\\n Mix \\n\\n 17 \\n\\n Renewable \\n\\n Consolidated \\n\\n 52% \\n\\n 65% \\n\\n 67% \\n\\n 72% \\n\\n (in %) \\n\\n 45% \\n\\n 33% \\n\\n 33% \\n\\n 28% \\n\\n Sustainability Report FY 19 \\n\\n At our ports, energy is primarily used for crane operations, transportation of cargo within campus through trucks 8 conveyor belts, automatic cargo handling system and tug boat. During the FY 19, our total energy consumption was 19,03,253 GJ and total GHG emissions were 2,70,170 tCOe. Through a series of interventions, including technological and process changes we have been able to reduce our energy intensity year-on-year since our base year. \\n\\n Energy Standalone \\n\\n Energy consumption per MMT of cargo handled 47% from previous year KY 67% from the base year \\n\\n Non- Renewable Energy (GJ) \\n\\n 9,906 \\n\\n -7,82,276 1,743 -6.81.000 \\n\\n FY 16 FY 17 \\n\\n 6,246 7,043 \\n\\n FY 18 \\n\\n -9,071 \\n\\n FY 19 \\n\\n -15,806 3,296 3,05,201 \\n\\n Consolidated \\n\\n 9.738 \\n\\n Energy consumption per MMT of cargo handled 32% from previous year 48% from the base year \\n\\n Renewable Energy (GJ) Intensity (GJ/MMT) \\n\\n o 25,65,281 1,748 44,610 -23.42.188 -22,95,625 -18,851 15,503 -14,356 \\n\\n FY 16 FY 17 \\n\\n 52,851 18,50,402 \\n\\n FY 18 FY 19 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17.png', 'pagenum': 17, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8145957411786507, 'company name': 'Adani Ports and Special Economic Zone Ltd', 'category': 'Energy Standalone', 'variable description': 'Non-renewable energy consumption in standalone operations during FY 16', 'variable': 'Non-Renewable Energy', 'value': '9,906', 'date': 'FY 16', 'unit': 'GJ', 'fuzzy_verification_flag': False, 'lm_verification_flag': True, 'context': '"Adani Ports and Special Economic Zone Limited \\n\\n 32 \\n\\n Types of fuel and coolant consumed during FY 19 \\n\\n 1,055 kg 30kg 2.5 kg R22 R410 R134 \\n\\n Energy \\n\\n FY 19 \\n\\n FY 17 \\n\\n FY 16 \\n\\n FY \\n\\n 27.8 ML \\n\\n Diesel \\n\\n 18 \\n\\n 52.8 kL \\n\\n Petrol \\n\\n Mix \\n\\n 4.3KL \\n\\n Furnace Oil \\n\\n There is also a significant impact that we face in terms of cost of acquisition of power 8 fuel. Over the years, the per capita cost has increased further putting pressure on our profitability. So, switching to a cleaner fuel may be of strategic importance for us in the longer term. \\n\\n 82 kg 2,317 kg R407C Acetylene \\n\\n Fuel \\n\\n 28,272 kg LPG \\n\\n Standalone (in %) \\n\\n 87% \\n\\n 39% \\n\\n 57% \\n\\n 58% \\n\\n 42% \\n\\n 59% \\n\\n 43% \\n\\n 5% \\n\\n 14,139 SCM PNG \\n\\n FY 16 \\n\\n Grid \\n\\n Energy \\n\\n FY 19 \\n\\n FY 18 \\n\\n FY \\n\\n Mix \\n\\n 17 \\n\\n Renewable \\n\\n Consolidated \\n\\n 52% \\n\\n 65% \\n\\n 67% \\n\\n 72% \\n\\n (in %) \\n\\n 45% \\n\\n 33% \\n\\n 33% \\n\\n 28% \\n\\n Sustainability Report FY 19 \\n\\n At our ports, energy is primarily used for crane operations, transportation of cargo within campus through trucks 8 conveyor belts, automatic cargo handling system and tug boat. During the FY 19, our total energy consumption was 19,03,253 GJ and total GHG emissions were 2,70,170 tCOe. Through a series of interventions, including technological and process changes we have been able to reduce our energy intensity year-on-year since our base year. \\n\\n Energy Standalone \\n\\n Energy consumption per MMT of cargo handled 47% from previous year KY 67% from the base year \\n\\n Non- Renewable Energy (GJ) \\n\\n 9,906 \\n\\n -7,82,276 1,743 -6.81.000 \\n\\n FY 16 FY 17 \\n\\n 6,246 7,043 \\n\\n FY 18 \\n\\n -9,071 \\n\\n FY 19 \\n\\n -15,806 3,296 3,05,201 \\n\\n Consolidated \\n\\n 9.738 \\n\\n Energy consumption per MMT of cargo handled 32% from previous year 48% from the base year \\n\\n Renewable Energy (GJ) Intensity (GJ/MMT) \\n\\n o 25,65,281 1,748 44,610 -23.42.188 -22,95,625 -18,851 15,503 -14,356 \\n\\n FY 16 FY 17 \\n\\n 52,851 18,50,402 \\n\\n FY 18 FY 19 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17.png', 'pagenum': 17, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf'}, 
    {'query': 'Percentage of non-renewable energy production', 'score': 0.8128389164472071, 'company name': 'Adani Ports and Special Economic Zone Ltd', 'category': 'Standalone (in %)', 'variable description': 'Percentage of non-renewable energy consumption in standalone operations', 'variable': 'Non-Renewable Energy', 'value': '87', 'date': '', 'unit': '%', 'fuzzy_verification_flag': False, 'lm_verification_flag': False, 'context': '"Adani Ports and Special Economic Zone Limited \\n\\n 32 \\n\\n Types of fuel and coolant consumed during FY 19 \\n\\n 1,055 kg 30kg 2.5 kg R22 R410 R134 \\n\\n Energy \\n\\n FY 19 \\n\\n FY 17 \\n\\n FY 16 \\n\\n FY \\n\\n 27.8 ML \\n\\n Diesel \\n\\n 18 \\n\\n 52.8 kL \\n\\n Petrol \\n\\n Mix \\n\\n 4.3KL \\n\\n Furnace Oil \\n\\n There is also a significant impact that we face in terms of cost of acquisition of power 8 fuel. Over the years, the per capita cost has increased further putting pressure on our profitability. So, switching to a cleaner fuel may be of strategic importance for us in the longer term. \\n\\n 82 kg 2,317 kg R407C Acetylene \\n\\n Fuel \\n\\n 28,272 kg LPG \\n\\n Standalone (in %) \\n\\n 87% \\n\\n 39% \\n\\n 57% \\n\\n 58% \\n\\n 42% \\n\\n 59% \\n\\n 43% \\n\\n 5% \\n\\n 14,139 SCM PNG \\n\\n FY 16 \\n\\n Grid \\n\\n Energy \\n\\n FY 19 \\n\\n FY 18 \\n\\n FY \\n\\n Mix \\n\\n 17 \\n\\n Renewable \\n\\n Consolidated \\n\\n 52% \\n\\n 65% \\n\\n 67% \\n\\n 72% \\n\\n (in %) \\n\\n 45% \\n\\n 33% \\n\\n 33% \\n\\n 28% \\n\\n Sustainability Report FY 19 \\n\\n At our ports, energy is primarily used for crane operations, transportation of cargo within campus through trucks 8 conveyor belts, automatic cargo handling system and tug boat. During the FY 19, our total energy consumption was 19,03,253 GJ and total GHG emissions were 2,70,170 tCOe. Through a series of interventions, including technological and process changes we have been able to reduce our energy intensity year-on-year since our base year. \\n\\n Energy Standalone \\n\\n Energy consumption per MMT of cargo handled 47% from previous year KY 67% from the base year \\n\\n Non- Renewable Energy (GJ) \\n\\n 9,906 \\n\\n -7,82,276 1,743 -6.81.000 \\n\\n FY 16 FY 17 \\n\\n 6,246 7,043 \\n\\n FY 18 \\n\\n -9,071 \\n\\n FY 19 \\n\\n -15,806 3,296 3,05,201 \\n\\n Consolidated \\n\\n 9.738 \\n\\n Energy consumption per MMT of cargo handled 32% from previous year 48% from the base year \\n\\n Renewable Energy (GJ) Intensity (GJ/MMT) \\n\\n o 25,65,281 1,748 44,610 -23.42.188 -22,95,625 -18,851 15,503 -14,356 \\n\\n FY 16 FY 17 \\n\\n 52,851 18,50,402 \\n\\n FY 18 FY 19 \\n\\n "', 'context_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=structured/format=csv/variable_desc=structured-quant-summary/source=passage-quants/userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17_contextnum-0_passage-quants_structured-quant-summary.csv', 'img_file': 'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf/data_type=unstructured/format=img/variable_desc=page-img/source=pdf-genie/karishma-12-apsez-sustainability-report-fy19pdf_pagenum-17.png', 'pagenum': 17, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf'}
]
"""
# ### save df_quant_verified locally
df_quant_verified.to_csv(f'/tmp/df_quant_verified.csv', index=False)

# ## Verify extracted company names
"""
Similar to verifying extracted quant values, we can also run one layer of verification for company names, to minimize any errors for values getting attributed to incorrect companies. 
"""

# ### Verify company names in verified quant files
tasks = [
    bg_async.async_verify_data(
        doc_name=doc_name,
        file_pattern='data_type=verification/**/variable_desc=verified-variable-values/**/source=var-variable_val-value/**.csv',
        val_col='company name',
        context_col='context',
        verification_type='values',
    )
    for doc_name in doc_names
]
verify_company_responses = utils.async_utils.run_async_tasks(tasks)

# ### read quant files with verified values and company names
tasks = [
    bg_sync.async_read_files(
        doc_name=doc_name,
        file_pattern=f"data_type=verification/**/variable_desc=verified-values/**/source=company-name/**quant-kpi-01*csv",
        timeout=15 * 60,
    )
    for doc_name in doc_names
]
df_quant_verified = utils.async_utils.run_async_tasks(tasks)
df_quant_verified = [resp.get_output() for resp in df_quant_verified]
df_quant_verified = [pd.DataFrame(df) for df in df_quant_verified]
df_quant_verified = pd.concat(df_quant_verified)
"""
Number of documents with verified values and company name files, `len(df_quant_verified['doc_name'].unique())`: 48
df_quant_verified columns, `list(df_quant_verified.columns)`
['category', 'company name', 'company name_verification_flag', 'context', 'context_file', 'doc_name', 'fuzzy_verification_flag', 'img_file', 'lm_verification_flag', 'pagenum', 'query', 'row_id', 'score', 'value', 'variable', 'variable description', 'date', 'unit']
As we can see, this data contains an additional column, `company name_verification_flag`, which comes from the call to `/verify_data` with `company name` as the value column. 
"""

# ## Extract document info
"""
In order to better contextualise the information extracted from within the documents, we will now use `/extract_doc_info` 
to extract some key document-level information, e.g. document organisation, document year, document type, etc. 
By default `/extract_doc_info' uses a pre-defined list of document types, which includes documents related to corporate disclosures, 
e.g. annual reports, sustainability reports, press releases, etc. However, you can also pass your own list of choices for document types 
you want to classify documents into. For this example, we will stick to the default choices. 
"""

# ### start time
doc_info_extraction_start_time = time.time()
"""
doc_info_extraction_start_time
1695727051.7271008
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

# ### merge doc_info with verified quant data
df_quant_verified = pd.merge(
    left=df_quant_verified,
    right=df_doc_info,
    on=['doc_name'],
    how='left'
)

# ## Organise KPIs by company name
"""
Once we have the document meta-data merged with extracted values, we can group extracted values 
by company names, so synthesize values across documents for the same company
"""

# ### clean company name
"""
By default, we will have company name extracted both from document meta-data and specific content within the document. 
The company names extracted from within the content are useful if a company is reporting some value for a subsidiary, or a specific business unit, or it's parent entity, etc. 
However, for many of the values disclosed in the contents of the document, no specific company name may be mentioned, as it is understood that these values belong to the same 
company, who published the document. In such cases, where no company name is explicitly provided in the contents of the document, we can use the company name from the meta-data of the document.
"""
## fill missing values with na
df_quant_verified = df_quant_verified.fillna('')
## convert company name and doc_org columns to str
for col in ['company name', 'doc_org']:
    df_quant_verified[col] = df_quant_verified[col].map(str)
## mask for rows where company name is none, but doc_org (extracted from document meta-data) is not None
empty_vals = ['', 'nan']
mask = (df_quant_verified['company name'].isin(empty_vals)) & \
       (~df_quant_verified['doc_org'].isin(empty_vals))
## set doc_org as company name for masked rows
df_quant_verified.loc[mask, 'company name'] = df_quant_verified.loc[mask, 'doc_org']
logger.info(f"unique company names after cleaning: {list(df_quant_verified['company name'].unique())}")
"""
Unique company names, `list(df_quant_verified['company name'].unique())`
[
    'eROI', 
    'Adani Ports and Special Economic Zone Limited', 
    '3M', 
    'Compass Group', 
    'ABB', 
    'Ecolab', 
    'American Express', 
    'RAVEN PROPERTY GROUP LIMITED', 
    'WEBUILD', 
    'Webuild Group', 
    'Arch Capital Group Ltd.', 
    'Aggreko plc', 
    'Webuild', 
    'Adani Ports and Special Economic Zone Ltd', 
    'VINCI', 
    'SCGG', 
    'Ashtead Group', 
    'AIG', 
    'Raven Property Group Limited', 
    'Samsung SDS', 
    'Aker Carbon', 
    'Admiral Group PLC', 
    'KIN + CARTA', 
    'Air New Zealand', 
    'AIR NEW ZEALAND', 
    'Ashtead Group plc', 
    'Accor', 
    'Raven Property Group', 
    'Webuild S.p.A.', 
    'Deutsche EuroShop', 
    'Sunbelt Rentals', 
    'Weapons Down Gloves Up', 
    'Capita', 
    'UPM', 
    'Admiral Group', 
    'Boliden', 
    'adesso Group', 
    'THE a2 MILK COMPANY LIMITED', 
    'APSEZ', 
    'Massey University / Spark', 
    'Embassy Village', 
    'Bank of America', 
    'BillerudKorsnäs', 
    'China Education Group Holdings Limited', 
    'Lenzing', 
    'DEG Deutsche EuroShop', 
    'Admiral Group plc', 
    'Compass Group PLC', 
    'Fresh by Eurest initiatives', 
    'CGG', 
    'webuild', 
    'a2 Milk Company', 
    'Arch', 
    'Company Name', 
    'CHINA EDUCATION GROUP HOLDINGS LIMITED', 
    'Savills', 
    'Savills plc', 
    'Aviva', 
    'Aviva Investors', 
    'China Education Group Holdings', 
    'Jiangxi Provincial', 
    'Albioma', 
    'Lenzing Group', 
    'Sunbelt', 
    'adesso SE', 
    'CGG SA', 
    'WEBUILD S.p.A.', 
    'RANUTS', 
    'Aviva plc', 
    'Arch Insurance', 
    'Fonds Stratégique de', 
    'Bayer', 
    'Mondi', 
    'ARKEMA', 
    'Participations (FSP)', 
    'Isabelle Boccon-Gibod', 
    'Allianz MD', 
    'Kier'
]
"""
## set company names that failed company name verification to doc_org
mask = (df_quant_verified['company name_verification_flag'] != True)
df_quant_verified.loc[mask, 'company name'] = df_quant_verified.loc[mask, 'doc_org']
"""
Unique company names, `list(df_quant_verified['company name'].unique())`
[
    'American Express', 
    'BillerudKorsnäs', 
    'Air New Zealand', 
    'UPM', 
    'American International Group, Inc.', 
    'AIG', 
    'Aviva plc', 
    'China Education Group Holdings Limited', 
    'CHINA EDUCATION GROUP HOLDINGS LIMITED', 
    'Jiangxi Provincial', 
    'China Education Group Holdings', 
    'RAVEN PROPERTY GROUP LIMITED', 
    'Accor', 
    'Admiral Group plc', 
    'Admiral Group', 
    'DEG Deutsche EuroShop', 
    'Deutsche EuroShop', 
    'Raven Property Group Limited', 
    'Raven Property Group', 
    'Boliden', 
    'Albioma', 
    'Aker Carbon', 
    'ABB', 
    '3M', 
    'Webuild S.p.A.', 
    'Webuild Group', 
    'Webuild', 
    'VINCI', 
    'Allianz MD', 
    'Arch', 
    'Arch Capital Group Ltd.', 
    'Air New Zealand Limited', 
    'Ecolab', 
    'ECOLAB', 
    'Samsung SDS', 
    'Bayer', 
    'WEBUILD', 
    'webuild', 
    'Aggreko plc', 
    'Ashtead Group plc', 
    'Sunbelt Rentals', 
    'Weapons Down Gloves Up', 
    'Embassy Village', 
    'Ashtead Group', 
    'Sunbelt', 
    'Kier Group plc', 
    'Adani Ports and Special Economic Zone Limited', 
    'Adani Ports and Special Economic Zone Ltd', 
    'KIN +CARTA', 
    'KIN + CARTA', 
    'SCGG',
    'CGG SA', 
    'CGG', 
    'Lenzing Group', 
    'Lenzing', 
    'adesso SE', 
    'adesso Group', 
    'Mondi', 
    'ARKEMA', 
    'Isabelle Boccon-Gibod', 
    'Participations (FSP)', 
    'Responsible Business Report', 
    'Capita', 
    'APSEZ', 
    'Compass Group', 
    'Bank of America', 
    'COMPASS GROUP', 
    'Fresh by Eurest initiatives',
    'Compass Group PLC', 
    'Aviva Investors', 
    'Aviva', 
    'WEBUILD S.p.A.', 
    'THE a2 MILK COMPANY LIMITED', 
    'a2 Milk Company', 
    'Arch Insurance', 
    'Savills', 
    'Savills plc', 
    'AIR NEW ZEALAND', 
    'Massey University / Spark'
]
As we can see, replacing company name with doc_org for rows where company name verification failed, fixes a couple of incorrect company names we had previously, ['Company Name', 'Fonds Stratégique de', 'eROI']. 
It is worth noting here that we could also flag specific rows in the data where `company name` does not match `doc_org`, and 
then run a more verification only for these rows, as these are the rows where company name needs more attention, as it is being extracted as something different from the document-level company name, 
and may in some cases just be the name of a product or business unit, rather a company per se. However, we will skip such deeper verification in this example, and use the `company name` as extracted, whenever 
it passes our previous verification check, and use `doc_org` otherwise.
"""
## save df_quant_verified locally
df_quant_verified.to_csv(f'/tmp/df_quant_verified.csv', index=False)

# ## Standardise company names
"""
Since the company names are extracted from different pages and sections of a document, these names may take slightly different form, 
e.g. we have pairs ('American International Group, Inc.', 'AIG') and ('China Education Group Holdings Limited', 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'China Education Group Holdings'), 
which correspond to the same company name written with small variations. We can standardise these names to make our downstream analysis easier, and to be able to use `company name` as a useful grouping column.
"""
# ### trigger company name standardisation
name_std_resp = bg_async.standardise_names(
    data=df_quant_verified[['company name']].drop_duplicates().to_dict('records'),
    text_col='company name',
    name_keyword='company name',
)
## get output
df_std_company_names = name_std_resp.get_output()
df_std_company_names = pd.DataFrame(df_std_company_names)
"""
Standardised company names, `df_std_company_names[['orig_name', 'std_name']].to_dict('records')`
[
    {'orig_name': 'American Express', 'std_name': 'American Express'}, 
    {'orig_name': 'BillerudKorsnäs', 'std_name': 'BillerudKorsnäs'}, 
    {'orig_name': 'Air New Zealand', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'UPM', 'std_name': 'UPM'}, 
    {'orig_name': 'American International Group, Inc.', 'std_name': 'American International Group, Inc.'}, 
    {'orig_name': 'AIG', 'std_name': 'American International Group, Inc.'}, 
    {'orig_name': 'Aviva plc', 'std_name': 'Aviva plc'}, 
    {'orig_name': 'China Education Group Holdings Limited', 'std_name': 'China Education Group Holdings Limited'}, 
    {'orig_name': 'CHINA EDUCATION GROUP HOLDINGS LIMITED', 'std_name': 'China Education Group Holdings Limited'}, 
    {'orig_name': 'Jiangxi Provincial', 'std_name': 'Jiangxi Provincial'}, 
    {'orig_name': 'China Education Group Holdings', 'std_name': 'China Education Group Holdings Limited'}, 
    {'orig_name': 'RAVEN PROPERTY GROUP LIMITED', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'Accor', 'std_name': 'Accor'}, 
    {'orig_name': 'Admiral Group plc', 'std_name': 'Admiral Group plc'}, 
    {'orig_name': 'Admiral Group', 'std_name': 'Admiral Group plc'}, 
    {'orig_name': 'DEG Deutsche EuroShop', 'std_name': 'DEG Deutsche EuroShop'}, 
    {'orig_name': 'Deutsche EuroShop', 'std_name': 'DEG Deutsche EuroShop'}, 
    {'orig_name': 'Raven Property Group Limited', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'Raven Property Group', 'std_name': 'RAVEN PROPERTY GROUP LIMITED'}, 
    {'orig_name': 'Boliden', 'std_name': 'Boliden'}, 
    {'orig_name': 'Albioma', 'std_name': 'Albioma'}, 
    {'orig_name': 'Aker Carbon', 'std_name': 'Aker Carbon'}, 
    {'orig_name': 'ABB', 'std_name': 'ABB'}, 
    {'orig_name': '3M', 'std_name': '3M'}, 
    {'orig_name': 'Webuild S.p.A.', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'Webuild Group', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'Webuild', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'VINCI', 'std_name': 'VINCI'}, 
    {'orig_name': 'Allianz MD', 'std_name': 'Allianz MD'}, 
    {'orig_name': 'Arch', 'std_name': 'Arch'}, 
    {'orig_name': 'Arch Capital Group Ltd.', 'std_name': 'Arch'}, 
    {'orig_name': 'Air New Zealand Limited', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'Ecolab', 'std_name': 'Ecolab'}, 
    {'orig_name': 'ECOLAB', 'std_name': 'Ecolab'}, 
    {'orig_name': 'Samsung SDS', 'std_name': 'Samsung SDS'}, 
    {'orig_name': 'Bayer', 'std_name': 'Bayer'}, 
    {'orig_name': 'WEBUILD', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'webuild', 'std_name': 'WEBUILD S.p.A.'}, 
    {'orig_name': 'Aggreko plc', 'std_name': 'Aggreko plc'}, 
    {'orig_name': 'Ashtead Group plc', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Sunbelt Rentals', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Weapons Down Gloves Up', 'std_name': 'Weapons Down Gloves Up'}, 
    {'orig_name': 'Embassy Village', 'std_name': 'Embassy Village'}, 
    {'orig_name': 'Ashtead Group', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Sunbelt', 'std_name': 'Ashtead Group plc'}, 
    {'orig_name': 'Kier Group plc', 'std_name': 'Kier Group plc'}, 
    {'orig_name': 'Adani Ports and Special Economic Zone Limited', 'std_name': 'Adani Ports and Special Economic Zone Limited'}, 
    {'orig_name': 'Adani Ports and Special Economic Zone Ltd', 'std_name': 'Adani Ports and Special Economic Zone Limited'}, 
    {'orig_name': 'KIN +CARTA', 'std_name': 'KIN + CARTA'}, 
    {'orig_name': 'KIN + CARTA', 'std_name': 'KIN + CARTA'}, 
    {'orig_name': 'SCGG', 'std_name': 'SCGG'}, 
    {'orig_name': 'CGG SA', 'std_name': 'SCGG'}, 
    {'orig_name': 'CGG', 'std_name': 'SCGG'}, 
    {'orig_name': 'Lenzing Group', 'std_name': 'Lenzing Group'}, 
    {'orig_name': 'Lenzing', 'std_name': 'Lenzing Group'}, 
    {'orig_name': 'adesso SE', 'std_name': 'adesso SE'}, 
    {'orig_name': 'adesso Group', 'std_name': 'adesso SE'}, 
    {'orig_name': 'Mondi', 'std_name': 'Mondi'}, 
    {'orig_name': 'ARKEMA', 'std_name': 'ARKEMA'}, 
    {'orig_name': 'Isabelle Boccon-Gibod', 'std_name': 'Isabelle Boccon-Gibod'}, 
    {'orig_name': 'Participations (FSP)', 'std_name': 'Isabelle Boccon-Gibod'},
    {'orig_name': 'Responsible Business Report', 'std_name': 'Isabelle Boccon-Gibod'}, 
    {'orig_name': 'Capita', 'std_name': 'Capita'}, 
    {'orig_name': 'APSEZ', 'std_name': 'APSEZ'}, 
    {'orig_name': 'Compass Group', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Bank of America', 'std_name': 'Bank of America'}, 
    {'orig_name': 'COMPASS GROUP', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Fresh by Eurest initiatives', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Compass Group PLC', 'std_name': 'Compass Group'}, 
    {'orig_name': 'Aviva Investors', 'std_name': 'Aviva Investors'}, 
    {'orig_name': 'Aviva', 'std_name': 'Aviva Investors'}, 
    {'orig_name': 'THE a2 MILK COMPANY LIMITED', 'std_name': 'THE a2 MILK COMPANY LIMITED'}, 
    {'orig_name': 'a2 Milk Company', 'std_name': 'THE a2 MILK COMPANY LIMITED'}, 
    {'orig_name': 'Arch Insurance', 'std_name': 'Arch Insurance'}, 
    {'orig_name': 'Savills', 'std_name': 'Savills'}, 
    {'orig_name': 'Savills plc', 'std_name': 'Savills'}, 
    {'orig_name': 'AIR NEW ZEALAND', 'std_name': 'Air New Zealand'}, 
    {'orig_name': 'Massey University / Spark', 'std_name': 'Massey University / Spark'}
]
Number of unique standardised company names: `len(df_std_company_names['orig_name'].unique())`: 78
Number of unique standardised company names: `len(df_std_company_names['std_name'].unique())`: 46
As we can see, company names have now been standardised quite well, 
and number of company names have gone down from 78 in the original names to 46 in the standardised names. 
"""

# ### merge standardised names onto df_quant_verified
df_quant_verified = pd.merge(
    left=df_quant_verified,
    right=df_std_company_names[['orig_name', 'std_name']].rename(
        columns={'orig_name': 'company name',
                 'std_name': 'company name_std'},
    ),
    on=['company name'],
    how='left',
)
"""
Check standardised names in df_quant_verified, `df_quant_verified[['company name', 'company name_std']].drop_duplicates().tail().to_dict('records')`
[
    {'company name': 'Arch Insurance', 'company name_std': 'Arch Insurance'}, 
    {'company name': 'Savills', 'company name_std': 'Savills'}, 
    {'company name': 'Savills plc', 'company name_std': 'Savills'}, 
    {'company name': 'AIR NEW ZEALAND', 'company name_std': 'Air New Zealand'}, 
    {'company name': 'Massey University / Spark', 'company name_std': 'Massey University / Spark'}
]
"""
## save df_quant_verified locally
df_quant_verified.to_csv(f'/tmp/df_quant_verified.csv', index=False)

# ## Filter most relevant data
"""
With company names standardised, we can filter data by company name, and keep most relevant rows for each KPI for each company.
"""

# ### sort data by company name, KPI
df_quant_verified = df_quant_verified.sort_values(
    by=['query', 'score', 'company name_std', ],
    ascending=False
).reset_index(drop=True)
"""
A sample of sorted data
df_quant_verified[[ 
    'query', 'score', 
    'company name_std', 'variable description', 'variable', 'value', 'unit', 'date',
    'doc_year', 'pagenum', 'doc_name'
]].head().to_dict('records')
[
    {'query': 'hazardous waste', 'score': 0.774275504945014, 'company name_std': 'Ecolab', 'variable description': 'Avoid more than 84 MILLION pounds of waste', 'variable': 'Pounds of waste', 'value': '84 million', 'unit': nan, 'date': '2021', 'doc_year': '2021', 'pagenum': 13, 'doc_name': 'userid_stuartcullinan_uploadfilename_jeon_27_ecolab_corporate-responsibility-report_2021pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7736468217838872, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Achievement of 75% reduction in waste intensity', 'variable': 'Waste intensity reduction', 'value': '75%', 'unit': nan, 'date': 'FY 2021-22', 'doc_year': '2021', 'pagenum': 108, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7712695153236897, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Quantity of wastewater recycled and reused', 'variable': '-', 'value': '650', 'unit': 'ML', 'date': nan, 'doc_year': '2021', 'pagenum': 100, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7703765424786837, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Target for achieving 20% reduction in waste intensity', 'variable': 'Waste intensity reduction', 'value': '20%', 'unit': nan, 'date': 'FY 2024-25', 'doc_year': '2021', 'pagenum': 108, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}, 
    {'query': 'hazardous waste', 'score': 0.7693046430369277, 'company name_std': 'Adani Ports and Special Economic Zone Limited', 'variable description': 'Quantity of waste managed', 'variable': '-', 'value': '29,359', 'unit': 'MT', 'date': nan, 'doc_year': '2021', 'pagenum': 100, 'doc_name': 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf'}
]
As we can see, the top 5 rows of the sorted data are highly relevant to the query ('hazardous waste' in this case). 
"""

# ### keep top 5 values for each company and KPI
df_quant_verified_filtered = df_quant_verified.groupby(
    by=['company name_std', 'query'],
    group_keys=False,
).apply(
    lambda x: x.sort_values(
        by=['company name_std', 'query', 'score'],
        ascending=False
    ).head(10)
).reset_index()
## relevant columns for printing data
data_cols = ['company name_std', 'query', 'score', 'variable description', 'variable', 'value', 'unit', 'date']
## check values for one company and kpi
mask = (df_quant_verified_filtered['company name_std'] == '3M') & \
       (df_quant_verified_filtered['query'] == 'GHG Scope 1 emissions')
logger.info(f"df_quant_verified_filtered[mask][data_cols].to_dict('records'): "
            f"{df_quant_verified_filtered[mask][data_cols].to_dict('records')}")
"""
df_quant_verified_filtered[mask][data_cols].to_dict('records')
[
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8206044541364184, 'variable description': 'Total Scope 1 and 2 location-based emissions', 'variable': 'Emissions', 'value': '73.3', 'unit': 'metric tons CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8203032984225203, 'variable description': 'Scope 1 emissions', 'variable': 'Emissions', 'value': '20,600', 'unit': 'metric tons COe', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.817225411941106, 'variable description': 'Scope 2 location-based emissions', 'variable': 'Emissions', 'value': '14,600', 'unit': 'metric tons CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8102522382382019, 'variable description': 'Scope 2 market-based emissions', 'variable': 'Emissions', 'value': '1.16', 'unit': 'metric tons COe', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8093313918397605, 'variable description': 'Total Scope 1 and 2 market-based emissions', 'variable': 'Emissions', 'value': '87.9', 'unit': 'metric tons COe', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8057149633162554, 'variable description': '% reduction Scope 1 and Scope 2 location-based emissions', 'variable': 'Emissions reduction', 'value': '73.5', 'unit': '% CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.8022968369319777, 'variable description': '% reduction Scope 1 and Scope 2 market-based emissions', 'variable': 'Emissions reduction', 'value': '66.3', 'unit': '% CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.7822868484425515, 'variable description': 'Total customer avoided metric tons CO2e emissions, cumulative since 2015', 'variable': 'Total customer avoided emissions', 'value': '25', 'unit': 'metric tons CO2e', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.7601040228162862, 'variable description': '% improved energy efficiency, indexed to net sales', 'variable': 'Energy efficiency', 'value': '34.7', 'unit': '%', 'date': '2022.0'}, 
    {'company name_std': '3M', 'query': 'GHG Scope 1 emissions', 'score': 0.7555038645494616, 'variable description': '% renewable energy to total electricity use', 'variable': 'Renewable energy', 'value': '29.1', 'unit': '%', 'date': '2022.0'}
]
"""

# ### Keep only verified values

# ## Process filtered data

# ### estimate values for each KPI for each document
