# # Generate training data

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

# ## Set documents to use for generating training data
doc_names = [
    'httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf',
    'httpswwwvedantalimitedcomimghomepagesustainability20report2022pdf',
]

# ## Masked modeling

# ### Brief description
"""
Masked modelling is suitable for making the model learn structured representations of the data, 
such as the structured quantitative data created in company_research/document_processing.py.
Such a model can then be used fill-in masked cells in a table, to fill-in company disclosed values for 
quantities of interest, or to retrieve content given a document name and page number.
"""

# ### generate training data for masked-modeling
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"generating training data for ({doc_num}/{len(doc_names)}): {doc_name}")
    ## training data for masked original tables
    resp = bg_async.generate_training_data(
        doc_name=doc_name,
        data_format='masked-original-tables'
    )
    responses = responses + [resp]
    ## training data for masked structured data
    resp = bg_async.generate_training_data(
        doc_name=doc_name,
        data_format='masked-structured-data'
    )
    responses = responses + [resp]
    ## training data for masked ranked data
    resp = bg_async.generate_training_data(
        doc_name=doc_name,
        data_format='masked-ranked-data'
    )
    responses = responses + [resp]

# ### wait for the task to finish
time.sleep(15 * 60)

# ### loop over responses, and read output, if it exists
masked_data_files = []
missing_masked_files = []
for resp_num, resp in enumerate(responses):
    logger.info(f"processing response # ({resp_num}/{len(responses)})")
    ## get output file
    output_file = bg_sync.get_response_output_file(resp)
    ## check if output file exists
    output_file_exists = bg_sync.get_response_data(bg_sync.check_file_exists(output_file))
    ## if output file exists
    if output_file_exists:
        ## get training data files
        masked_data_files_ = bg_sync.get_response_data(bg_sync.read_file(output_file))
        ## add to masked_data_files
        if isinstance(masked_data_files_, list):
            masked_data_files  = masked_data_files + masked_data_files_
        elif isinstance(masked_data_files_, dict):
            for key in masked_data_files_.keys():
                masked_data_files = masked_data_files + masked_data_files_[key]
    ## if output_file does not yet exist
    else:
        missing_masked_files = missing_masked_files + [output_file]

# ### check masked_data_files
logger.info(f"masked data files: {masked_data_files}")
"""
masked_data_files[:5]
[
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=masked-table-cells/format=csv/variable_desc=masked-orig-tables/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-17_table-cells_trans_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=masked-table-cells/format=csv/variable_desc=masked-orig-tables/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-18_table-cells_trans_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=masked-table-cells/format=csv/variable_desc=masked-orig-tables/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-18_table-cells_trans_orig-table_tablenum-1.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=masked-table-cells/format=csv/variable_desc=masked-orig-tables/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-23_table-cells_trans_orig-table_tablenum-0.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=masked-table-cells/format=csv/variable_desc=masked-orig-tables/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-25_table-cells_trans_orig-table_tablenum-0.csv',
]
"""

# ### read masked training file
df_masked_data = bg_sync.get_response_data(bg_sync.read_file(masked_data_files[10]))
df_masked_data = pd.DataFrame(df_masked_data)

# ### check masked training data
logger.info(f"sample masked training data: {df_masked_data.head().to_dict('records')}")
"""
df_masked_data.head().to_dict('records')
[{'prompt': 'SCENARIO GROWTH,SCENARIO BASE,SCENARIO RESILIENCE,pagenum,doc_name\n<masked-cell>,<masked-cell>,<masked-cell>,<masked-cell>,<masked-cell>\n\n\nFill in the masked-cells in the data above', 'response': 'SCENARIO GROWTH,SCENARIO BASE,SCENARIO RESILIENCE,pagenum,doc_name\n"In the short term, shows a rapid recovery of the economy after the effects of COVID-19. The impacts are limited to the period in which measures to restrict the flows of people, goods and services were used to control the pandemic. After mass vaccination of the world population and control of the number of cases, social habits and consumer behavior are returning to the previous state before COVID-19. There is little coordination between developed and developing countries on the need for and financing of policies for transitioning to a low carbon economy. The energy matrix continues to be concentrated in fossil sources and commodity prices, particularly energy, are higher.","In the short term, this scenario is characterized by a more gradual recovery trajectory after the effects of COVID-19. Even after the mass vaccination of the population and drop in the number of cases, there are important consequences. In this sense, the increase in unemployment and poverty, as well as the level of indebtedness of the private sector, affect the dynamics of demand in a structural way. This fragility and the increase in uncertainty are reflected in the level of household consumption in a lasting way. In the medium and long term, economic growth is average, there is greater concern with mobility and air quality in large centers. The global articulations for the transition to a low carbon economy still face coordination and financing problems, but a series of more dispersed initiatives are starting to take shape in an important way. More direct solutions driven by large cities and popular pressure characterize this scenario. The world energy matrix has undergone important changes, especially with regard to the share of coal and renewable sources, and commodity prices, especially energy, have grown in line with historic trends.","In the short term, this scenario is characterized by delay in solving the pandemic and a slow recovery trajectory. Resistant strains and the public\'s choice not to get vaccinated cause sporadic outbreaks that hamper robust recovery. Furthermore, the social and economic scars are significant. There is a change in the habits, behaviors of consumers and economic agents. There are structural impacts both on the demand side and on the supply side. In addition to uncertainty and unemployment affecting consumption, the lower level of investment has a negative impact on productivity. Global value chains are disrupted and global trade declines. In the medium and long term, there is lower global growth, greater environmental risk and greater concern with these issues. Countries are encouraged to cooperate and coordinate efforts for a rapid transition to a low carbon economy. Despite the increase in speed of the energy transition, reducing investments in the expansion of production of fossil energy sources, restricting their supply, total energy demand is increasingly met by alternative sources, reducing demand pressure on fossil energy prices.",43,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n'}]
"""

# ## Generative question-answering

# ### Brief description
"""
Generative question-answering is suitable for making the model provide factual answers from the data, 
citing relevant sources, based on free-form question.
"""

# ### generate training data for generative question-answering
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"generating training data for ({doc_num}/{len(doc_names)}): {doc_name}")
    ## training data for masked original tables
    resp = bg_async.generate_training_data(
        doc_name=doc_name,
        data_format='generative-question-answering'
    )
    responses = responses + [resp]

# ### loop over responses, and read output, if it exists
gen_qa_files = []
missing_gen_qa_files = []
for resp_num, resp in enumerate(responses):
    logger.info(f"processing response # ({resp_num}/{len(responses)})")
    ## get output file
    output_file = bg_sync.get_response_output_file(resp)
    ## check if output file exists
    output_file_exists = bg_sync.get_response_data(bg_sync.check_file_exists(output_file))
    ## if output file exists
    if output_file_exists:
        ## get training data files
        gen_qa_files_ = bg_sync.get_response_data(bg_sync.read_file(output_file))
        ## add to masked_data_files
        if isinstance(gen_qa_files_, list):
            gen_qa_files = gen_qa_files + gen_qa_files_
        elif isinstance(gen_qa_files_, dict):
            for key in gen_qa_files_.keys():
                gen_qa_files = gen_qa_files + gen_qa_files_[key]
    ## if output_file does not yet exist
    else:
        missing_gen_qa_files = missing_gen_qa_files + [output_file]


# ### check generative-qa files
logger.info(f"generative-qa files: {gen_qa_files}")
"""
gen_qa_files[:5]
[
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=generative-qa/format=csv/variable_desc=generative-passage-answers/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-0_text-blocks_trans.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=generative-qa/format=csv/variable_desc=generative-passage-answers/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-10_text-blocks_trans.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=generative-qa/format=csv/variable_desc=generative-passage-answers/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-11_text-blocks_trans.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=generative-qa/format=csv/variable_desc=generative-passage-answers/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-12_text-blocks_trans.csv', 
    'gs://db-genie/entity_type=training-data/entity=llm-training-data/data_type=generative-qa/format=csv/variable_desc=generative-passage-answers/source=httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf/httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf_pagenum-13_text-blocks_trans.csv'
]
"""

# ### read generative qa file
df_gen_qa = bg_sync.get_response_data(bg_sync.read_file(gen_qa_files[10]))
df_gen_qa = pd.DataFrame(df_gen_qa)

# ### check generative-qa training data
logger.info(f"sample generative-qa training data: {df_gen_qa.head().to_dict('records')}")
"""
df_gen_qa.head().to_dict('records')
[
    {
        'prompt': 'According to page number 19 of httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf, What was the reason for the increase in our emissions in 2021?', 
        'response': 'text,pagenum,doc_name\nAbsolute Operating GHG Emissions,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n"Our emission reduction target encompasses 100% of the assets operated in all our activities, including energy generation, for all greenhouse gases, being a material, relevant, short-and medium-term contribution to mitigate climate change. Our GHG emissions intensity targets (E&P and Refining) represented a coverage of 67% of the emissions from the activities we operated in 2021.",19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n"Between 2015 and 2021, our absolute operating emissions decreased by 21%. In 2021, emissions totaled 61.8 million tCO2e, higher than the result of the previous three years. This increase is a direct consequence of the atypical thermoelectric dispatch in a year of water scarcity.",19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n"Scope 1 emissions represented 99% of our operational emissions in 2021. Scope 2 emissions, therefore, have low materiality.",19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\nCLIMATE CHANGE SUPPLEMENT,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n1.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n2.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n3.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n"2020 was an atypical year due to the impact of the COVID-19 pandemic on demand for our products. While the first half was marked by a strong retraction in demand, with the hibernation of less efficient production assets and a reduction in the processed load at our refineries, half of for refined in the second we observed a resumption demand oil and oil Additionally, half of 2020, demand for products. in the second there was an increase in the from thermoelectric plants Brazilian National System Operator energy power by the Electric (Operador Nacional Sistema Elétrico-ONS), from do a measure taken to save water hydroelectric of reservoirs at the end the dry season.",19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n"2021, of ONS In the dispatch thermoelectric plants coordinated by the remained high, of thermoelectric five This resulting in the highest average generation in the last years. fact GHG 2021, is the main cause for the increase in our emissions in even in a scenario of reduction E&P Refining. in our carbon intensities in and",19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n4.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n5.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n19,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n6.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n7.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n8.,19,httpspetrobrascombrdatafilese897b4615e56f7105fc7bcd7e9e99ea811_pet_clima_ingles_2022_fzpdf\n'
    }
]
"""

# ## Next steps
# ### Once we have the training data, we can proceed to training a model using this data
# ### See model_training/train_llm.py file for an example of running model training.