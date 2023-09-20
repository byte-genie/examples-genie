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

# ### get uploaded document names
doc_names = df_uploads['doc_name'].unique().tolist()
"""
doc_names
['userid_stuartcullinan_uploadfilename_jason_08_gpgpdf', 'userid_stuartcullinan_uploadfilename_jeon_20_billerudkorsnas_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-2021-air-new-zealand-gender-pay-reportpdf', 'userid_stuartcullinan_uploadfilename_jeon_25_upm_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-anti-bribery-and-corruption-policy-august-2021pdf', 'userid_stuartcullinan_uploadfilename_jason_09_srpdf', 'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_annual-reportpdf', 'userid_stuartcullinan_uploadfilename_anastasia_19_china_east_education_ltd_20211228164502_62371643_enpdf', 'userid_stuartcullinan_uploadfilename_jason_09_gpgpdf', 'userid_stuartcullinan_uploadfilename_28_kim_cartapdf', 'userid_stuartcullinan_uploadfilename_karishma-03-lse_rav_2020pdf', 'userid_stuartcullinan_uploadfilename_1_accor_mrpdf', 'userid_stuartcullinan_uploadfilename_jaime_admiral-group_annual-reportpdf', 'userid_stuartcullinan_uploadfilename_karishma-01-des-esg-2021-e-spdf', 'userid_stuartcullinan_uploadfilename_karishma-03-environmental-and-social-report-extracted-from-2020-annual-reportpdf', 'userid_stuartcullinan_uploadfilename_jeon_22_boliden_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_anastasia_5_albioma_urd_20201231_vdef_engpdf', 'userid_stuartcullinan_uploadfilename_jeon_21_aker-carbon-capture_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_08_abb_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_01_3m-company_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_al_9_2021-annual-report_compressedpdf', 'userid_stuartcullinan_uploadfilename_al_8_vinci-2021-universal-registration-documentpdf', 'userid_stuartcullinan_uploadfilename_jaime_allianz-group_sustainability-reportpdf', 'userid_stuartcullinan_uploadfilename_jason_14_srpdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-annual-financial-resultspdf', 'userid_stuartcullinan_uploadfilename_jeon_27_ecolab_corporate-responsibility-report_2021pdf', 'userid_stuartcullinan_uploadfilename_16_samsung_sdspdf', 'userid_stuartcullinan_uploadfilename_jeon_26_bayer_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_al_9_webuild_ethics_code_1pdf', 'userid_stuartcullinan_uploadfilename_anastasia_4_-2020-aggreko-annual-reportpdf', 'userid_stuartcullinan_uploadfilename_12_ashteadgroup_mrpdf', 'userid_stuartcullinan_uploadfilename_al_6_kier-2021-ara-finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-12-apsez-sustainability-report-fy19pdf', 'userid_stuartcullinan_uploadfilename_4_kim_cartapdfpdf', 'userid_stuartcullinan_uploadfilename_3_cgcpdf', 'userid_stuartcullinan_uploadfilename_jeon_23_lenzing_sustainability-report_2021pdf', 'userid_stuartcullinan_uploadfilename_1_adesso_sepdfpdf', 'userid_stuartcullinan_uploadfilename_jason_08_srpdf', 'userid_stuartcullinan_uploadfilename_jeon_24_mondi_integrated-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jeon_19_arkema_universal-registration-document_2021pdf', 'userid_stuartcullinan_uploadfilename_12_argo_blockchainpdfpdf', 'userid_stuartcullinan_uploadfilename_13_capita_mrpdf', 'userid_stuartcullinan_uploadfilename_karishma-12-adani-port-special-economic-zone-ir21pdf', 'userid_stuartcullinan_uploadfilename_5_compass-group_mrpdf', 'userid_stuartcullinan_uploadfilename_jaime_aviva-plc_uk-pay-gap-reportpdf', 'userid_stuartcullinan_uploadfilename_karishma-04-sustainability-highlights-report-2021-19-finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-01-des-annualreport-2021-e-spdf', 'userid_stuartcullinan_uploadfilename_al_9_relazione-governance-2021-final_eng-con-tabellepdf', 'userid_stuartcullinan_uploadfilename_jeon_07_a2-milk-company_annual-report_2021pdf', 'userid_stuartcullinan_uploadfilename_jason_14_gpgpdf', 'userid_stuartcullinan_uploadfilename_karishma-04-savills-plc-ar21pdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-nz-2022-greenhouse-gas-inventory-report_finalpdf', 'userid_stuartcullinan_uploadfilename_karishma-13-air-new-zealand-sustainability-report-2020pdf']
"""

# ### extract page images from documents
write_img_responses = []
for doc_num, doc_name in enumerate(doc_names):
    resp_ = bg_async.write_pdf_img(
        doc_name=doc_name
    )
    write_img_responses = write_img_responses + [resp_]
