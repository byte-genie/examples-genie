# # Extract specific KPIs from documents

# ## import necessary libraries

import os
import time
import uuid
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
doc_names
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

# ## Extract text from documents

# ### Run OCR on page images
ocr_start_time = time.time()
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering OCR for ({doc_num}/{len(doc_names)}: {doc_name})")
    resp = bg_async.extract_text(
        doc_name=doc_name
    )
    responses = responses + [resp]

# ### list OCR output files for text
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="variable_desc=text-blocks/**.csv"
    )
    for doc_name in doc_names
]
ocr_text_files = utils.async_utils.run_async_tasks(tasks)
ocr_text_files = [resp.get_data() for resp in ocr_text_files if resp.get_data() is not None]
"""
Number of documents with OCR text files, len(ocr_text_files): 49
Number of OCR text files for one document, len(ocr_text_files[0]): 132
OCR text files for one document: ocr_text_files[0]
[
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-0_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-100_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-101_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-102_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-103_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-104_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-105_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-106_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-107_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-108_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-109_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-10_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-110_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-111_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-112_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-113_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-114_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-115_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-116_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-117_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-118_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-119_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-11_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-120_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-121_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-122_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-123_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-124_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-125_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-126_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-127_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-128_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-129_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-12_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-130_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-131_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-13_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-14_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-15_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-16_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-17_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-18_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-19_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-1_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-20_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-21_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-22_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-23_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-24_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-25_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-26_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-27_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-28_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-29_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-2_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-30_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-31_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-32_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-33_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-34_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-35_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-36_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-37_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-38_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-39_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-3_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-40_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-41_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-42_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-43_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-44_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-45_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-46_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-47_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-48_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-49_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-4_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-50_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-51_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-52_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-53_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-54_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-55_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-56_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-57_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-58_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-59_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-5_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-60_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-61_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-62_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-63_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-64_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-65_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-66_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-67_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-68_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-69_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-6_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-70_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-71_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-72_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-73_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-74_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-75_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-76_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-77_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-78_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-79_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-7_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-80_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-81_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-82_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-83_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-84_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-85_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-86_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-87_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-88_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-89_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-8_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-90_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-91_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-92_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-93_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-94_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-95_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-96_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-97_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-98_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-99_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/jeon_20_billerudkorsnas_annual-report_2021pdf_pagenum-9_text-blocks.csv'
]
Extracted text files contain page number from which the text was extracted, so tables belonging to a specific page can be filtered, if needed.
"""

# ### list OCR output files for tables
tasks = [
    bg_sync.async_list_doc_files(
        doc_name=doc_name,
        file_pattern="variable_desc=table-cells/**.csv"
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

# ### Segment OCR extracted text
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

# ### read segment_text output
segment_text_files = []
missing_segment_text_files = []
for resp_num, resp in enumerate(segment_text_responses):
    logger.info(f"processing segment_text resp: {resp_num}/{len(segment_text_responses)}")
    output_file = resp.get_output_file()
    if resp.check_output_file_exists():
        segment_text_files = segment_text_files + [resp.get_output_file()]
    else:
        missing_segment_text_files = missing_segment_text_files + [resp.get_output_file()]


# ## Convert pdf documents to latex
"""
PDF to latex converter uses a specialised OCR that can convert PDF files to a latex markdown format. This converts text, tables, and equations into latex code. This is particularly useful for documents that contain equations or mathematical symbols. 
"""

# ### trigger pdf to markdown conversion
convert_to_markdown_responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"triggering pdf to markdown conversion for ({doc_num}/{doc_name}): {doc_name}")
    convert_to_markdown_resp = bg_async.convert_pdf_to_markdown(
        doc_name=doc_name,
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