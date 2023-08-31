# # Manage data extracted from documents
"""
All the data extracted from a document is linked to that document,
and can be accessed using simple API calls
"""


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


# ## set documents
doc_names = [
    'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf',
    'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf',
]
"""
In order to access the extracted data for documents, the documents first need to be processed.
See company_research/document_processing.py (.ipynb) file to find an example of how to trigger document processing. 
"""

# ## get extracted text segments (output of OCR + layout parsing)
text_segment_files = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"listing files for ({doc_num}/{len(doc_names)}): {doc_name}")
    text_segment_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=text-blocks/**.csv',
    ).get_data()
    logger.info(f"found {len(text_segment_files_)} files for {doc_name}")
    text_segment_files = text_segment_files + text_segment_files_
# ## check files found
logger.info(f"{len(text_segment_files)} text segment files found")
"""
text_segment_files[:6]
[
    'gs://db-genie/entity_type=url/entity=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-0_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-10_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-11_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-12_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-13_text-blocks.csv', 
    'gs://db-genie/entity_type=url/entity=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/data_type=semi-structured/format=csv/variable_desc=text-blocks/source=esgnie.com/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-14_text-blocks.csv',
]
A separate csv file is generated for each page
"""

# ## read one OCR output file
df_text_segment = bg_sync.read_file(text_segment_files[25]).get_data()
df_text_segment = pd.DataFrame(df_text_segment)

# ## check df_text_segment
"""
df_text_segment.head().to_dict('records')
[
    {'bbox_area': 10333.005895824135, 'bbox_area_frac': 0.0009405179758273, 'bottom': 125.8584910221398, 'cc_num': 1, 'confidence': 99.8708038330078, 'h': 48.057115875184536, 'height': 48.057115875184536, 'left': 335.1334784924984, 'max_empty_width_x': '.150.0.840.0.13.0', 'max_empty_width_y': '.374.0.141.0', 'n_chr': 7, 'pagenum': 32, 'right': 550.1485851556063, 'row_num': 0.0, 'text': 'CLIMATE', 'text_len': 7, 'top': 77.80137514695525, 'w': 215.01510666310787, 'width': 215.01510666310787, 'x0': 335.1334784924984, 'x1': 550.1485851556063, 'x_expand': 0, 'x_group': 'bb.0.x.1.1.', 'xbar': 442.64103182405233, 'xy_expand': 0, 'xy_group': 'bb.0.x.1.1.-y.1.1.', 'y0': 77.80137514695525, 'y1': 125.8584910221398, 'y_expand': 0, 'y_group': 'bb.0.y.1.1.', 'ybar': 101.82993308454752}, 
    {'bbox_area': 9592.03049789073, 'bbox_area_frac': 0.000873073837265, 'bottom': 124.7534235008061, 'cc_num': 2, 'confidence': 99.7629623413086, 'h': 45.85704496130347, 'height': 45.85704496130347, 'left': 564.1618012860417, 'max_empty_width_x': '.150.0.840.0.13.0', 'max_empty_width_y': '.374.0.141.0', 'n_chr': 6, 'pagenum': 32, 'right': 773.3342524543405, 'row_num': 0.0, 'text': 'CHANGE', 'text_len': 6, 'top': 78.89637853950262, 'w': 209.17245116829872, 'width': 209.17245116829872, 'x0': 564.1618012860417, 'x1': 773.3342524543405, 'x_expand': 0, 'x_group': 'bb.0.x.1.1.', 'xbar': 668.7480268701911, 'xy_expand': 0, 'xy_group': 'bb.0.x.1.1.-y.1.1.', 'y0': 78.89637853950262, 'y1': 124.7534235008061, 'y_expand': 0, 'y_group': 'bb.0.y.1.1.', 'ybar': 101.82490102015436}, 
    {'bbox_area': 14845.332305642216, 'bbox_area_frac': 0.0013512333227477, 'bottom': 124.47516227513552, 'cc_num': 3, 'confidence': 99.7822265625, 'h': 45.45856860280037, 'height': 45.45856860280037, 'left': 789.1467301100492, 'max_empty_width_x': '.150.0.840.0.13.0', 'max_empty_width_y': '.374.0.141.0', 'n_chr': 10, 'pagenum': 32, 'right': 1115.7151365056634, 'row_num': 0.0, 'text': 'SUPPLEMENT', 'text_len': 10, 'top': 79.01659367233515, 'w': 326.56840639561415, 'width': 326.56840639561415, 'x0': 789.1467301100492, 'x1': 1115.7151365056634, 'x_expand': 0, 'x_group': 'bb.0.x.1.1.', 'xbar': 952.4309333078564, 'xy_expand': 0, 'xy_group': 'bb.0.x.1.1.-y.1.1.', 'y0': 79.01659367233515, 'y1': 124.47516227513552, 'y_expand': 0, 'y_group': 'bb.0.y.1.1.', 'ybar': 101.74587797373532}, 
    {'bbox_area': 1396.0140047020934, 'bbox_area_frac': 0.0001270662456952, 'bottom': 308.349902221933, 'cc_num': 5, 'confidence': 99.44402313232422, 'h': 39.41485136188567, 'height': 39.41485136188567, 'left': 1155.262688755989, 'max_empty_width_x': '.150.0.840.0', 'max_empty_width_y': '.374.0.141.0', 'n_chr': 2, 'pagenum': 32, 'right': 1190.68116571242, 'row_num': 1.0, 'text': '1.', 'text_len': 2, 'top': 268.93505086004734, 'w': 35.41847695643082, 'width': 35.41847695643082, 'x0': 1155.262688755989, 'x1': 1190.68116571242, 'x_expand': 0, 'x_group': 'bb.0.x.1.1.', 'xbar': 1172.9719272342045, 'xy_expand': 0, 'xy_group': 'bb.0.x.1.1.-y.1.2.', 'y0': 268.93505086004734, 'y1': 308.349902221933, 'y_expand': 0, 'y_group': 'bb.0.y.1.2.', 'ybar': 288.6424765409902}, 
    {'bbox_area': 18630.82826114749, 'bbox_area_frac': 0.0016957920145233, 'bottom': 775.064107015729, 'cc_num': 13, 'confidence': 99.92632293701172, 'h': 87.48615480959415, 'height': 87.48615480959415, 'left': 250.6318113617599, 'max_empty_width_x': '.150.0.124.0', 'max_empty_width_y': '.374.0.5.0.158.0', 'n_chr': 3, 'pagenum': 32, 'right': 463.58925933390856, 'row_num': 2.0, 'text': 'GHG', 'text_len': 3, 'top': 687.5779522061348, 'w': 212.95744797214863, 'width': 212.95744797214863, 'x0': 250.6318113617599, 'x1': 463.58925933390856, 'x_expand': 0, 'x_group': 'bb.0.x.1.1.', 'xbar': 357.11053534783423, 'xy_expand': 0, 'xy_group': 'bb.0.x.1.1.-y.2.1.', 'y0': 687.5779522061348, 'y1': 775.064107015729, 'y_expand': 0, 'y_group': 'bb.0.y.2.1.', 'ybar': 731.3210296109319}
]
* Text segments that are grouped together can be obtained by concatenating text by xy_group, with each xy_group corresponds to one group of text;
* xbar denotes the average x-axis coordinates of the text on the page;
* ybar denotes the average y-axis coordinates of the text on the page;
"""

# ## get segmented text
"""
Text segments that are grouped together can be obtained by concatenating text by xy_group, with each xy_group corresponds to one group of text.

"""
text_grouped = df_text_segment.groupby(['xy_group'])['text'].apply(lambda x: ' '.join(x)).reset_index(drop=True)

"""
text_grouped.to_list()[:5]
[
    'CLIMATE CHANGE SUPPLEMENT', 
    '1.', 
    'GHG Emissions Intensity in Power Generation', 
    'Our operations in the power sector are a reflection of the decisions of the Brazilian National Electric System Operator (Operador Nacional do Sistema Elétrico ONS), - which determines when a given power generating unit is dispatched. Our emissions in this segment, therefore, are dependent on a series of factors that include the availability of other power plants, climatic conditions and intrinsic seasonality of the Brazilian electrical system. Although we do not have commitments referring exclusively to our power generation activities, we monitor our emission', 
    'intensity in these operations. Our power generation park is essentially powered by gas, and we have high energy efficiency units, with combined cycle and integrated with our other assets for steam export. In the last decade, we have implemented more efficient generation cycles (combined cycle) at the Três Lagoas, Baixada Fluminense and Canoas plants. In addition, made investments were to improve the efficiency of the turbogenerators the Ibirité, at Baixada Fluminense and TermoBahia plants, allowing for a reduction in fuel consumption generated. per energy'
]
"""