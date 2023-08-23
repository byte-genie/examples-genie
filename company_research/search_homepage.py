# # Find an organisation's homepage, and search relevant info from it

import time
import pandas as pd
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie

# ### init byte-genie in async mode (tasks will run in the background, i.e. API call will return a response before completing the task)
bg_async = ByteGenie(
    secrets_file='secrets.json',
    task_mode='async',
    verbose=1,
)

# ### init byte-genie in sync mode (tasks will run in the foreground, i.e. API call will keep running until the task is finished)
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

# ## Define inputs

# ### set organisation names to download documents for
org_names = [
    'Vedanta Limited',
    'GHG protocol',
    'World Bank',
]

# ## Find homepage
resp = bg_async.find_homepage(
    entity_names=org_names
)

# ### file where output will be written
output_file = resp['response']['task_1']['task']['output_file']
"""
output_file
gs://db-genie/entity_type=api-tasks/entity=22daacbd638ff9c859bc350ba3e48c3b/data_type=structured/format=pickle/variable_desc=find_homepage/source=api-genie/22daacbd638ff9c859bc350ba3e48c3b.pickle
"""

# ### check whether output_file exists or not
output_file_exists = bg_sync.get_response_data(bg_sync.check_file_exists(output_file))

# ### read output_file
if output_file_exists:
    resp = bg_sync.read_file(output_file)
    df_homepage = bg_sync.get_response_data(resp)
    df_homepage = pd.DataFrame(df_homepage)
"""
df_homepage[['entity_name', 'url']].to_dict('records')
[{'entity_name': 'Vedanta Limited', 'url': 'www.vedantalimited.com'}, {'entity_name': 'GHG protocol', 'url': 'www.ghgprotocol.org'}, {'entity_name': 'World Bank', 'url': 'www.worldbank.org'}, {'entity_name': 'World Bank', 'url': 'data.worldbank.org'}]
Notice that in case an organisation has multiple websites that show up in the search, as is often the case, 
the results will have multiple homepages, as is the case with World Bank in this example.
"""

# ## Search organisation homepage

# ### set keyphrases to search
keyphrases = [
    'sustainability',
    'materiality assessment',
]

# ### select urls to search from
selected_urls = df_homepage['url'].unique().tolist()[:1]

# ### trigger search
responses = []
for url_num, selected_url in enumerate(selected_urls):
    logger.info(f"searching {selected_url} ({url_num}/{len(selected_urls)})")
    resp = bg_async.search_web(
        keyphrases=keyphrases,
        site=selected_url
    )
    responses = responses + [resp]

# ### wait for output to be ready
time.sleep(15 * 60)

# ### read search results
df_search = pd.DataFrame()
missing_files = []
for resp_num, resp in enumerate(responses):
    logger.info(f"processing response: {resp_num}/{len(responses)}")
    ## get output file
    output_file = bg_sync.get_response_output_file(resp)
    ## check if output file exists
    output_file_exists = bg_sync.check_file_exists(output_file)
    ## if output file already exists
    if output_file_exists:
        logger.info(f"{output_file} exists: reading it")
        ## read output file
        df_search_ = bg_sync.get_response_data(bg_sync.read_file(output_file))
        ## convert output to df
        df_search_ = pd.DataFrame(df_search_)
        ## add to df_search
        df_search = pd.concat([df_search, df_search_])
    ## if output file does not yet exist
    else:
        logger.warning(f"{output_file} does not exists: storing it to missing files")
        ## add it to missing files
        missing_files = missing_files + [output_file]

# ### check search data
"""
list(df_search.columns)
['href', 'href_type', 'keyphrase', 'result_html', 'result_text']
df_search[['href', 'result_text']].to_dict('records')
[
    {'href': 'https://www.vedantalimited.com/Vedanta2021/sustainability-esg.html', 'result_text': 'well-positioned to deliver sustainable solutions\nvedantalimited.com\nhttps://www.vedantalimited.com › sustainability-esg\nVSAP is our sustainability risk assurance tool, which is used to assess the compliance of all our businesses with the Vedanta Sustainability Framework.'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY22/business-responsibility-and-sustainability-report.html', 'result_text': "Business Responsibility & Sustainability Report\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY22 › busi...\nNote: Vedanta Limited's primary disclosure document on its sustainability & ESG practices, performance is its Annual Sustainability Report, which is written ..."}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/pdf/Business_Responsibility_and_Sustainability_Report_compressed.pdf', 'result_text': 'BUSINESS RESPONSIBILITY & SUSTAINABILITY REPORT\nvedantalimited.com\nhttps://www.vedantalimited.com › pdf › Busine...\nPDF\nThis audit is conducted across all business locations to ensure Vedanta Sustainability Framework (VSF) compliance. The VSAP outcomes are specifically tracked by ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Responsible-Operations-for-Sustainable-Future.pdf', 'result_text': 'Responsible Operations for Sustainable Future\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJul 26, 2021 — We demonstrate world-class standards of governance, safety, sustainability and social responsibility. Our Value Chain. Our Core Values. Our Core ...\n132 pages'}, 
    {'href': 'https://www.vedantalimited.com/uploads/investor-overview/annual-report/Executive-Summary-SR-FY23.pdf', 'result_text': "Sustainability Report (Executive Summary) FY 23\nvedantalimited.com\nhttps://www.vedantalimited.com › annual-report\nPDF\nVedanta's Sustainability strategy enables the company to address the evolving expectations of its key stakeholders regarding critical areas such as climate ...\n29 pages"}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Social-Investment-Management.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — The purpose of this Technical Standard is to provide guidance on how best to establish Social. Investment and Community Development programmes, ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Leadership-Responsibility-and-Resources.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — The purpose of this Management Standard is to ensure all front-line leaders, senior managers and those employees with appropriate sustainability ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Vedanta-HSE-Policy.pdf', 'result_text': 'Health, Safety & Environment Policy\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nCommunicate with all our stakeholders on the progress and performance of Health, Safety, Environment and Sustainability management;.'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Stakeholder-Materiality-and-Risk-Management.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — Issues include, but are not limited to, health, safety, environmental and social risk management, community relations, human rights,.'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Employee-Consultation-and-Participation.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — The purpose of this Technical Standard is to establish the programme design, risk management controls and supporting information, to ensure the ...'}, 
    {'href': 'https://www.vedantalimited.com/eng/esg_community_development.php', 'result_text': 'Community Development | Vedanta - Improving Lives with ...\nvedantalimited.com\nhttps://www.vedantalimited.com › eng › esg_commun...\nAt Vedanta, we are committed to delivering meaningful and sustainable social impact. ... Governed by our in-house CSR Policy and Sustainability Framework, ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/New-Projects-Planning-Processes-and-Site-Closure.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — for closure – incorporation of sustainability into the operational phase of a project/site is covered by other Vedanta Group Standards.'}, 
    {'href': 'https://www.vedantalimited.com/InteractiveAnnualReport_FY20/sustainability-and-esg/people-and-culture/', 'result_text': 'Vedanta - People and Culture\nvedantalimited.com\nhttps://www.vedantalimited.com › sustainability-and-esg\nVedanta has always aspired to build a culture that demonstrates world-class standards in safety, environment and sustainability.'}, 
    {'href': 'https://www.vedantalimited.com/uploads/corporate-governance/policies_practices/vedanta_supplier_annd_customer_sustainability_policy.pdf', 'result_text': 'Supplier and Business Partner Sustainability Management ...\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › ve...\nPDF\nthemselves, Vedanta employees and others, and supports our policies in relation to sustainability and protection of the environment. Vedanta will:.'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Management-Review-and-Continual-Improvement.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — The purpose of this Management Standard is to describe the arrangements and requirements for the annual management review of the sustainability ...'}, 
    {'href': 'https://www.vedantalimited.com/img/homepage/Sustainability%20Report%2022.pdf', 'result_text': "Sustainability Report\nvedantalimited.com\nhttps://www.vedantalimited.com › img › homepage\nPDF\nJul 30, 2022 — In other words, we had to align our vision to that of the UN's. Sustainable Development Goals (UN SDGs). What emerged at the end of these ...\n76 pages"}, 
    {'href': 'https://www.vedantalimited.com/', 'result_text': "India's leading natural resources and technology ...\nvedantalimited.com\nhttps://www.vedantalimited.com\nAt Vedanta, we are focused on nation building through sustainable growth while setting high standards of corporate governance and transparency."}, 
    {'href': 'https://www.vedantalimited.com/eng/media-safety.php', 'result_text': 'Safety Gear 101 | Prioritizing Safety for All\nvedantalimited.com\nhttps://www.vedantalimited.com › eng › media-safety\n... to providing employees with a safe and healthy work environment and has put in place world-class standards in safety, environment, and sustainability.'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Waste-Management-Standard.pdf', 'result_text': 'Waste Management Standard\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nThe Plan must be approved by site management. 2.1.4. Undertake geochemical characterization of the mineral waste and ensure that there is no environmental or ...'}, 
    {'href': 'https://www.vedantalimited.com/InteractiveAnnualReport_FY20/sustainability-and-esg/materiality-assessment/', 'result_text': 'Materiality Assessment\nvedantalimited.com\nhttps://www.vedantalimited.com › sustainability-and-esg\nMATERIALITY ASSESSMENT. DELIVERING ON ... Vedanta, conducts a formal stakeholder engagement and materiality assessment exercise, once in every three years.'}, 
    {'href': 'https://www.vedantalimited.com/vedanta-sr3/materiality-assessment.html', 'result_text': 'Materiality assessment\nvedantalimited.com\nhttps://www.vedantalimited.com › vedanta-sr3 › mater...\nTo align our priorities and actions towards the new ESG purpose, we refreshed our materiality assessment in FY2022 through a detailed peer benchmarking ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Stakeholder-Materiality-and-Risk-Management.pdf', 'result_text': 'Sustainability Governance System\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJan 30, 2021 — Material risk assessment shall take into account information gathered across the business, using operational/business sustainability risk.'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/materiality.html', 'result_text': 'Materiality\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY23 › mat...\nMateriality matrix ; 1, Community Engagement and Development. Total community spend ; 2, Water Management. Recycling % ; 3, Health, Safety and Well-Being. Zero ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/VEDL-SR-FY2022.pdf', 'result_text': 'Vedanta Sustainability Report 2021-22\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJul 30, 2022 — Materiality assessment | Our ESG scorecard ... refreshed our materiality assessment in FY2022 through a detailed peer.'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/stakeholder-engagement.html', 'result_text': 'Stakeholder Engagement\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY23 › stak...\nThis builds successful, long-lasting relationships by identifying and addressing material problems that help us to anticipate emerging risks, ...'}, 
    {'href': 'https://www.vedantalimited.com/vedanta-sr3/sustainability-strategy.html', 'result_text': 'Sustainability strategy\nvedantalimited.com\nhttps://www.vedantalimited.com › vedanta-sr3 › sustai...\nWork begun to improve social licence to operate – perception surveys, materiality assessment, social performance review, FPIC requirements review ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Responsible-Operations-for-Sustainable-Future.pdf', 'result_text': "Responsible Operations for Sustainable Future\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJul 26, 2021 — CEO's message | Materiality matrix | Strategic priorities framework. Sustainability journey | COVID-19 response. Governance.\n132 pages"}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY22/our-esg-strategy.html', 'result_text': 'Our ESG strategy\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY22 › our...\nOur ESG KPIs are focused on responding to those issues identified as High in our materiality assessment. The top priorities across Environment, Social and ...'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/pdf/Business_Responsibility_and_Sustainability_Report_compressed.pdf', 'result_text': 'BUSINESS RESPONSIBILITY & SUSTAINABILITY REPORT\nvedantalimited.com\nhttps://www.vedantalimited.com › pdf › Busine...\nPDF\nMateriality assessment was conducted at Vedanta Group level as well as at 3 Business Units (Vedanta Aluminium, Cairn and HZL) individually.'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/risk-management.html', 'result_text': 'Risk Management\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY23 › risk...\nBy identifying and assessing changes in risk exposure, ... The key considerations of our decision-making are materiality and risk tolerance.'}, 
    {'href': 'https://www.vedantalimited.com/InteractiveAnnualReport_FY20/sustainability-and-esg/our-sustainability-management-approach/', 'result_text': 'Our Sustainability Management Approach\nvedantalimited.com\nhttps://www.vedantalimited.com › sustainability-and-esg\nVSAP is our sustainability risk assurance tool, used to assess the compliance of all our businesses with the Vedanta Sustainability Framework.'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/corporate-governance.html', 'result_text': 'Corporate Governance\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY23 › cor...\nManagement Discussion and Analysis · Statutory Reports · Financial Statements. Copyright ©2023 India Ltd. All rights reserved.'}, 
    {'href': 'https://www.vedantalimited.com/vedantaFY23/transforming-together.html', 'result_text': 'transforming together inclusive. responsible. value- ...\nvedantalimited.com\nhttps://www.vedantalimited.com › vedantaFY23 › tran...\nManagement Discussion and Analysis · Statutory Reports · Financial Statements. Copyright ©2023 India Ltd. All rights reserved.'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Our-journey-towards-a-Sustainable-Future.pdf', 'result_text': 'Our journey… towards a sustainable future\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nMateriality matrix. Related information: vedantaresources.com/SustainableDevelopment2013-14/overview/materiality. Sustainability priorities covered in our ...'}, 
    {'href': 'https://www.vedantalimited.com/vedanta-sr3/', 'result_text': 'Vedanta Sustainibility Report 2021-22\nvedantalimited.com\nhttps://www.vedantalimited.com › vedanta-sr3\nI am pleased to present our 14th Sustainability Report, which gives our stakeholders a concise, complete, and transparent assessment of our ability to ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/corporate-governance/policies_practices/vedl-policy-for-materiality-archival-lodr-eng.pdf', 'result_text': 'VEDANTA LIMITED Policy for determination of Materiality ...\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › ve...\nPDF\nJul 14, 2023 — procedures for fair disclosure of Material events and Unpublished Price Sensitive ... D. MATERIALITY ASSESSMENT AND DISCLOSURE REQUIREMENT.\n8 pages'}, 
    {'href': 'https://www.vedantalimited.com/InteractiveAnnualReport_FY20/overview/about-the-report/', 'result_text': 'About the Report\nvedantalimited.com\nhttps://www.vedantalimited.com › overview › about-t...\n... to make an informed assessment of our ability to create value over the short ... It includes measures of engagement with identified material stakeholder ...'}, 
    {'href': 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Elements-of-a-Sustainable-Future.pdf', 'result_text': 'elements of a - sustainable futures ustainable\nvedantalimited.com\nhttps://www.vedantalimited.com › uploads › esg\nPDF\nJun 16, 2017 — This materiality matrix is reviewed and ratified by both the Executive Committee (EXCO) and the Sustainability Committee.\n107 pages'}
]
"""

# ### save search results
df_search.to_csv('/tmp/venda_ltd_search_results.csv', index=False)

# ### read saved results
df_search = pd.read_csv('/tmp/venda_ltd_search_results.csv')

# ## Download files

# ### select files to download
urls_to_download = df_search['href'].unique().tolist()[:5]

# ### trigger download
resp = bg_async.download_file(
    urls=urls_to_download
)

# ### get output file
output_file = bg_sync.get_response_output_file(resp)

# ### check if output file exists
output_file_exists = bg_sync.get_response_data(bg_sync.check_file_exists(output_file))

# ### read output file
if output_file_exists:
    downloaded_urls = bg_sync.get_response_data(bg_sync.read_file(output_file))

# ## Check data for downloaded URLs
logger.info(f"downloaded URLs: {downloaded_urls}")
"""
downloaded_urls
['gs://db-genie/entity_type=url/entity=httpswwwvedantalimitedcomvedantafy23pdfbusiness_responsibility_and_sustainability_report_compressedpdf/data_type=unstructured/format=.pdf/variable_desc=document/source=vedantalimited.com/httpswwwvedantalimitedcomvedantafy23pdfbusiness_responsibility_and_sustainability_report_compressedpdf.pdf', 
'gs://db-genie/entity_type=url/entity=httpswwwvedantalimitedcomuploadsesgesg-sustainability-frameworkresponsible-operations-for-sustainable-futurepdf/data_type=unstructured/format=.pdf/variable_desc=document/source=vedantalimited.com/httpswwwvedantalimitedcomuploadsesgesg-sustainability-frameworkresponsible-operations-for-sustainable-futurepdf.pdf', 
'gs://db-genie/entity_type=url/entity=httpswwwvedantalimitedcomuploadsinvestor-overviewannual-reportexecutive-summary-sr-fy23pdf/data_type=unstructured/format=.pdf/variable_desc=document/source=vedantalimited.com/httpswwwvedantalimitedcomuploadsinvestor-overviewannual-reportexecutive-summary-sr-fy23pdf.pdf']
"""

# ## Next Steps
# ### Once we have donwloadedthe relevant pages/documents from an organisation's homepage, we can move on to processing these documents
# ### See document processing examples, e.g. company_research/document_processing.py, to get started with document processing