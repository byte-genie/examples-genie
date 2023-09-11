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

# ## Extracted text segments (output of OCR + layout parsing)

# ### list relevant files
text_segment_files = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"listing files for ({doc_num}/{len(doc_names)}): {doc_name}")
    text_segment_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=text-blocks/**.csv',
    ).get_data()
    logger.info(f"found {len(text_segment_files_)} files for {doc_name}")
    text_segment_files = text_segment_files + text_segment_files_

# ### list relevant files
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

# ### read one text segments file
df_text_segment = bg_sync.read_file(text_segment_files[25]).get_data()
df_text_segment = pd.DataFrame(df_text_segment)

# ### check df_text_segment
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

# ### get segmented text
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


# ## Extracted tables
"""
Extracted table files process the OCR output to re-construct tables in their original form---the form in which the table was present in the original document. 
"""

# ### list files
table_files = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"listing files for ({doc_num}/{len(doc_names)}): {doc_name}")
    table_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=orig-table/**.csv',
    ).get_data()
    logger.info(f"found {len(table_files_)} files for {doc_name}")
    table_files = table_files + table_files_
# ## check files found
logger.info(f"{len(table_files)} table files found")

# ### read one table file
df_table = bg_sync.read_file(table_files[10]).get_data()
df_table = pd.DataFrame(df_table)

# ### check df_table
logger.info(f"{len(df_table)} rows found in the table")
"""
list(df_table.columns)
['SCENARIO BASE', 'SCENARIO GROWTH', 'SCENARIO RESILIENCE']
df_table.head().to_dict('records')
[{'SCENARIO BASE': 'In the short term, this scenario is characterized by a more gradual recovery trajectory after the effects of COVID-19. Even after the mass vaccination of the population and drop in the number of cases, there are important consequences. In this sense, the increase in unemployment and poverty, as well as the level of indebtedness of the private sector, affect the dynamics of demand in a structural way. This fragility and the increase in uncertainty are reflected in the level of household consumption in a lasting way. In the medium and long term, economic growth is average, there is greater concern with mobility and air quality in large centers. The global articulations for the transition to a low carbon economy still face coordination and financing problems, but a series of more dispersed initiatives are starting to take shape in an important way. More direct solutions driven by large cities and popular pressure characterize this scenario. The world energy matrix has undergone important changes, especially with regard to the share of coal and renewable sources, and commodity prices, especially energy, have grown in line with historic trends.', 'SCENARIO GROWTH': 'In the short term, shows a rapid recovery of the economy after the effects of COVID-19. The impacts are limited to the period in which measures to restrict the flows of people, goods and services were used to control the pandemic. After mass vaccination of the world population and control of the number of cases, social habits and consumer behavior are returning to the previous state before COVID-19. There is little coordination between developed and developing countries on the need for and financing of policies for transitioning to a low carbon economy. The energy matrix continues to be concentrated in fossil sources and commodity prices, particularly energy, are higher.', 'SCENARIO RESILIENCE': "In the short term, this scenario is characterized by delay in solving the pandemic and a slow recovery trajectory. Resistant strains and the public's choice not to get vaccinated cause sporadic outbreaks that hamper robust recovery. Furthermore, the social and economic scars are significant. There is a change in the habits, behaviors of consumers and economic agents. There are structural impacts both on the demand side and on the supply side. In addition to uncertainty and unemployment affecting consumption, the lower level of investment has a negative impact on productivity. Global value chains are disrupted and global trade declines. In the medium and long term, there is lower global growth, greater environmental risk and greater concern with these issues. Countries are encouraged to cooperate and coordinate efforts for a rapid transition to a low carbon economy. Despite the increase in speed of the energy transition, reducing investments in the expansion of production of fossil energy sources, restricting their supply, total energy demand is increasingly met by alternative sources, reducing demand pressure on fossil energy prices."}]
"""

# ## Translated text

# ### list files
translated_text_files = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"listing files for ({doc_num}/{len(doc_names)}): {doc_name}")
    translated_text_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=trans-text/**text-blocks*.csv',
    ).get_data()
    logger.info(f"found {len(translated_text_files_)} files for {doc_name}")
    translated_text_files = translated_text_files + translated_text_files_
# ## check files found
logger.info(f"{len(translated_text_files)} translated text files found")

# ### read one translated text file
df_trans_text = bg_sync.read_file(translated_text_files[25]).get_data()
df_trans_text = pd.DataFrame(df_trans_text)


# ### check df_trans_text
logger.info(f"{len(df_trans_text)} rows found in the table")
"""
list(df_trans_text.columns)
['english_text', 'language', 'pagenum', 'text', 'xy_group']
df_trans_text.head().to_dict('records')
[{'english_text': 'CLIMATE CHANGE SUPPLEMENT', 'language': 'en', 'pagenum': 32, 'text': 'CLIMATE CHANGE SUPPLEMENT', 'xy_group': 'bb.0.x.1.1.-y.1.1.'}, {'english_text': '1.', 'language': 'en', 'pagenum': 32, 'text': '1.', 'xy_group': 'bb.0.x.1.1.-y.1.2.'}, {'english_text': 'GHG Emissions Intensity in Power Generation', 'language': 'en', 'pagenum': 32, 'text': 'GHG Emissions Intensity in Power Generation', 'xy_group': 'bb.0.x.1.1.-y.2.1.'}, {'english_text': 'Our operations in the power sector are a reflection of the decisions of the Brazilian National Electric System Operator (Operador Nacional do Sistema Elétrico ONS), - which determines when a given power generating unit is dispatched. Our emissions in this segment, therefore, are dependent on a series of factors that include the availability of other power plants, climatic conditions and intrinsic seasonality of the Brazilian electrical system. Although we do not have commitments referring exclusively to our power generation activities, we monitor our emission', 'language': 'en', 'pagenum': 32, 'text': 'Our operations in the power sector are a reflection of the decisions of the Brazilian National Electric System Operator (Operador Nacional do Sistema Elétrico ONS), - which determines when a given power generating unit is dispatched. Our emissions in this segment, therefore, are dependent on a series of factors that include the availability of other power plants, climatic conditions and intrinsic seasonality of the Brazilian electrical system. Although we do not have commitments referring exclusively to our power generation activities, we monitor our emission', 'xy_group': 'bb.0.x.1.1.-y.2.2.'}, {'english_text': 'intensity in these operations. Our power generation park is essentially powered by gas, and we have high energy efficiency units, with combined cycle and integrated with our other assets for steam export. In the last decade, we have implemented more efficient generation cycles (combined cycle) at the Três Lagoas, Baixada Fluminense and Canoas plants. In addition, made investments were to improve the efficiency of the turbogenerators the Ibirité, at Baixada Fluminense and TermoBahia plants, allowing for a reduction in fuel consumption generated. per energy', 'language': 'en', 'pagenum': 32, 'text': 'intensity in these operations. Our power generation park is essentially powered by gas, and we have high energy efficiency units, with combined cycle and integrated with our other assets for steam export. In the last decade, we have implemented more efficient generation cycles (combined cycle) at the Três Lagoas, Baixada Fluminense and Canoas plants. In addition, made investments were to improve the efficiency of the turbogenerators the Ibirité, at Baixada Fluminense and TermoBahia plants, allowing for a reduction in fuel consumption generated. per energy', 'xy_group': 'bb.0.x.1.2.-y.2.1.'}]
"""


# ## Quant files

# ### list files
quant_files = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"listing files for ({doc_num}/{len(doc_names)}): {doc_name}")
    quant_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=structured-quant-summary/**.csv',
    ).get_data()
    logger.info(f"found {len(quant_files_)} files for {doc_name}")
    quant_files = quant_files + quant_files_
# ## check files found
logger.info(f"{len(quant_files)} quant files found")

# ### read one quant file
df_quants = bg_sync.read_file(quant_files[13]).get_data()
df_quants = pd.DataFrame(df_quants)

# ### check df_trans_text
logger.info(f"{len(df_quants)} rows found in the quants file")
"""
list(df_quants.columns)
['category', 'company name', 'context', 'date', 'doc_name', 'pagenum', 'relevant quote', 'unit', 'value', 'variable', 'variable description']
df_quants.head().to_dict('records')
[{'category': 'Zero Routine Flaring', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': 2018.0, 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'We announced our support for the World Bank\'s "Zero Routine Flaring by 2030" initiative', 'unit': '', 'value': '', 'variable': '', 'variable description': 'Support for the World Bank\'s "Zero Routine Flaring by 2030" initiative'}, {'category': 'Sustainability Commitments', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': '', 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'We already have a high rate of average use of produced gas, reaching 97.2% in 2021', 'unit': '', 'value': '', 'variable': '', 'variable description': 'High rate of average use of produced gas'}, {'category': 'Flare Gas', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': '', 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year', 'unit': 'tCO2e/year', 'value': '80,000 tCO2e/year', 'variable': 'Emissions Reduction', 'variable description': 'Reduction in emissions using the Flare Gas Recovery Unit on three platforms'}, {'category': 'Flare Gas', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': 2021.0, 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases', 'unit': '', 'value': '', 'variable': 'Quantity of Recovered Gas', 'variable description': 'Gas returned for processing in the unit using the Flare Gas Recovery Unit'}, {'category': 'Flare Gas Recovery Unit', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': 2022.0, 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022', 'unit': '', 'value': '8 platforms', 'variable': 'Planned Platforms', 'variable description': 'Planned platforms to start operating the Flare Gas Recovery Unit'}]
"""

# ## Synthesized quants
"""
Synthesized quant files synthesized some document-level info, such as the document author, document year, document type, document organisation, etc, 
with the specific quantitative info extracted from within the document. This is useful for putting the specific quantitative info into the broader document context, e.g. for filtering 
quantitative info published within the last two years, or information published in a specific doc_type.
"""

# ### list files
syn_quant_files = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"listing files for ({doc_num}/{len(doc_names)}): {doc_name}")
    syn_quant_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=synthesized-quants/**.csv',
    ).get_data()
    logger.info(f"found {len(syn_quant_files_)} files for {doc_name}")
    syn_quant_files = syn_quant_files + syn_quant_files_
# ## check files found
logger.info(f"{len(syn_quant_files)} synthesized quant files found")
"""
Typically, there will be only one syntehsized quant file for one document (doc_name), 
and all the quants extracted from that document will be consolidated in this one file.
"""

# ### read one syntehsized quants file
df_syn_quants = bg_sync.read_file(syn_quant_files[0]).get_data()
df_syn_quants = pd.DataFrame(df_syn_quants)

# ### check df_trans_text
logger.info(f"{len(df_syn_quants)} rows found in the synthesized quants file")
"""
list(df_syn_quants.columns)
['category', 'company name', 'company_verification_flag', 'context', 'date', 'doc_name', 'doc_org', 'doc_type', 'doc_year', 'embedding', 'fuzzy_verification_flag', 'lm_verification_flag', 'pagenum', 'relevant quote', 'relevant quote from text', 'row_id', 'unit', 'value', 'variable', 'variable description']
df_quants.head().to_dict('records')
[
    {'category': 'Zero Routine Flaring', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': 2018.0, 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'We announced our support for the World Bank\'s "Zero Routine Flaring by 2030" initiative', 'unit': '', 'value': '', 'variable': '', 'variable description': 'Support for the World Bank\'s "Zero Routine Flaring by 2030" initiative'}, 
    {'category': 'Sustainability Commitments', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': '', 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'We already have a high rate of average use of produced gas, reaching 97.2% in 2021', 'unit': '', 'value': '', 'variable': '', 'variable description': 'High rate of average use of produced gas'}, 
    {'category': 'Flare Gas', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': '', 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year', 'unit': 'tCO2e/year', 'value': '80,000 tCO2e/year', 'variable': 'Emissions Reduction', 'variable description': 'Reduction in emissions using the Flare Gas Recovery Unit on three platforms'}, 
    {'category': 'Flare Gas', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': 2021.0, 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases', 'unit': '', 'value': '', 'variable': 'Quantity of Recovered Gas', 'variable description': 'Gas returned for processing in the unit using the Flare Gas Recovery Unit'}, 
    {'category': 'Flare Gas Recovery Unit', 'company name': '', 'context': '"Zero Routine Flaring \\n\\n In 2018, we announced our support for the World Bank\'s \\"Zero Routine Flaring by 2030\\" initiative. Thus, meeting its criteria is one of our Sustainability Commitments. We emphasize that we already have a high rate of average use of produced gas, reaching 97.2% in 2021. >> See Investments and Initiatives \\n\\n COMMISSIONING AND OPERATION OF CLOSED FLARE SYSTEMS \\n\\n Platforms P-66, P-70 and P-77, located in the Tupi, Atapu and B\\u00fazios fields, started to operate with minimum flaring, which will occur only in exceptional safety situations, reducing emissions of greenhouse gases. The flare of the units has the function of burning gas not used on the platforms, in order to safely dispose of it. The start-up of the Flare Gas Recovery Unit (FGRU) allows this gas to return for processing in the unit, preventing its burning and the consequent emission of greenhouse gases. The potential for reducing emissions using the system on these three platforms is around 80,000 tCO2e per year. \\n\\n In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022. All our new projects also already have the FGRU, as it is already a standard in our company. The FPSO Almirante Tamandar\\u00e9 will be the first chartered unit to adopt the system as a standard in the project. \\n\\n CLIMATE CHANGE SUPPLEMENT \\n\\n 1. \\n\\n 2. \\n\\n 3. \\n\\n 4. \\n\\n 5. \\n\\n 6. \\n\\n 7. \\n\\n 8. \\n\\n 24 \\n\\n "', 'date': 2022.0, 'doc_name': 'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf', 'pagenum': 24, 'relevant quote': 'In addition to the P-66, P-70 and P-77, we expect to start operating the FGRU of another 8 platforms in the Campos and Santos Basins in 2022', 'unit': '', 'value': '8 platforms', 'variable': 'Planned Platforms', 'variable description': 'Planned platforms to start operating the Flare Gas Recovery Unit'}
]
"""
