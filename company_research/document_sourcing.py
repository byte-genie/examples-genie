# # Source company documents

import pandas as pd
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

# ## Define inputs

# ### set company names to download documents for
company_names = [
    'Rosneft Oil Co.',
    'Petroleo Brasileiro SA',
    'American Electric Power Company, Inc.'
    'Vedanta Limited',
    'Shell PLC',
    'UltraTech Cement Limited',
]

# ### set documnet keywords to search documents for
doc_keywords = ['sustainability report']

# ## Data sourcing

# ### trigger document download
resp = bg_async.download_documents(
    entity_names=company_names,
    doc_keywords=doc_keywords,
)
# ### check resp status
status = resp.get_status()
"""
status
scheduled # if the task is scheduled, and the output is not yet available
successful # if the task is already completed successfully, and output is available
"""

# ### file where output will be written
output_file = resp.get_output_file()
"""
output_file
gs://db-genie/entity_type=api-tasks/entity=593a5370f106bf174d115e2fc2c2a3c9/data_type=structured/format=pickle/variable_desc=download_documents/source=api-genie/593a5370f106bf174d115e2fc2c2a3c9.pickle
"""

# ### check whether output_file exists or not
output_file_exists = resp.check_output_file_exists()
"""
output_file_exists
True # if the output file exists
False # if the output file does not exist
"""

# ## Read sourced data

# ### read output file
# resp = bg_sync.read_file(output_file)
df_reports = resp.read_output_data()
df_reports = pd.DataFrame(df_reports)

# ### check df columns
list(df_reports.columns)
"""
list(df_reports.columns)
['entity_name', 'href', 'href_text', 'keyphrase', 'page_summary', 'result_html', 'result_text']
"""

# ### check the document urls found
df_reports['href'].unique().tolist()
"""
df_reports['href'].unique().tolist()
['https://www.rosneft.com/upload/site2/document_file/Rosneft_CSR2021_ENG.pdf', 'https://www.rosneft.com/upload/site2/document_file/Rosneft_CSR2020_ENG.pdf', 'https://www.rosneft.com/upload/site2/document_file/GMYxKoY3Jq.pdf', 'https://www.rosneft.com/upload/site2/document_file/8hXhu2BppP.pdf', 'https://www.rosneft.com/upload/site2/document_file/Rosneft_CSR18_EN_Book.pdf', 'https://www.rosneft.com/upload/site2/document_file/D37RlV1B0q.pdf', 'https://www.rosneft.com/upload/site2/document_file/VihCbI5mWq.pdf', 'https://www.rosneft.com/upload/site2/document_file/Rosneft_CSR2019_ENG.pdf', 'https://www.rosneft.com/upload/site2/document_file/RN_SR2016_eng_20160929.pdf', 'https://www.rosneft.com/upload/site2/document_file/RN_SR_2016_EN(2).pdf', 'https://www.rosneft.com/upload/site2/document_file/RN_SR2018_eng_web_1.pdf', 'https://www.rosneft.com/upload/site2/document_file/TQClh5jK3Y.pdf', 'https://www.rosneft.com/upload/site2/document_file/ivYaF8XHjn.pdf', 'https://www.rosneft.com/upload/site2/document_file/TnUlZT2ih4.pdf', 'https://www.rosneft.com/upload/site2/attach/3/23/Rosneft_UN_en_2023.pdf', 'https://www.rosneft.com/upload/site2/document_file/a_report_2022_eng.pdf', 'https://www.rosneft.com/upload/site2/attach/3/23/Rosneft_UN_en_2022.pdf', 'https://www.rosneft.com/upload/site2/document_file/a_report_2021_eng.pdf', 'https://www.rosneft.com/upload/site2/document_file/P3-10_01_R-0001_UL-001_EN.pdf', 'https://www.rosneft.com/upload/site2/document_file/176419/RN_SR_2014_ENG_WEB.pdf', 'https://petrobras.com.br/data/files/E8/97/B4/61/5E56F7105FC7BCD7E9E99EA8/11_PET_clima_ingles_2022_fz.pdf', 'https://petrobras.com.br/data/files/70/61/C7/14/04C45710A9CBFF47D438E9C2/Social%20Responsibility%20Policy_out2020.pdf', 'https://petrobras.com.br/data/files/02/73/FC/24/DF0C5810FB6B7E48B8E99EA8/Petrobras%20Compliance%20Program_23.pdf', 'https://petrobras.com.br/data/files/37/55/FB/5B/B3438810819C6568B8E99EA8/CDHCC_2022_ENG.pdf', 'https://petrobras.com.br/data/files/78/94/6C/57/D94426100E7FA126675391A8/Water_at_Petrobras.pdf', 'https://petrobras.com.br/data/files/2A/44/1C/0E/1BAB1810B4DA1818B8E99EA8/MSA_ingles%202021.pdf', 'https://petrobras.com.br/data/files/4C/91/84/55/ACB44710D16B8537D438E9C2/code-of-ethical-conduct.pdf', 'https://s3.amazonaws.com/mz-filemanager/25fdf098-34f5-4608-b7fa-17d60b2de47d/46b7ad13-4d71-4546-ae29-5007d1c51b80_Regimento_Interno_do_Comite_de_Auditoria_Estatutario_do_Conglomerado_Petrobras_1.pdf', 'https://www.vedantalimited.com/img/homepage/Sustainability%20Report%2022.pdf', 'https://www.vedantalimited.com/vedantaFY23/pdf/Business_Responsibility_and_Sustainability_Report_compressed.pdf', 'https://www.vedantalimited.com/uploads/investor-overview/annual-report/Executive-Summary-SR-FY23.pdf', 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Responsible-Operations-for-Sustainable-Future.pdf', 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Our-journey-towards-a-Sustainable-Future.pdf', 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Partners-for-the-Future-2.pdf', 'https://www.vedantalimited.com/uploads/investor-overview/annual-report/TCFD-Report-FY23.pdf', 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Delivering-Sustainability.pdf', 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Enduring-Value-Through-Values.pdf', 'https://www.vedantalimited.com/uploads/stock-exchange-announcements/2012-22/Intimation-for-Sustainability-Report-and-TCFD-Report-for-FY-2021-22.pdf', 'https://www.vedantalimited.com/img/media_mentions/media_reports/pdf/Vedanta-Annual-Integrated-Report.pdf', 'https://www.vedantalimited.com/InteractiveAnnualReport_FY20/static/Business-responsibility-report-mapping-fce8dfbe7e4dbe32fa098180a998c74c.pdf', 'https://www.vedantalimited.com/img/media_mentions/press_release/vedanta-reports-sustainable-development-as-per-gri-g4-3-august-2015.pdf', 'https://www.vedantalimited.com/img/esg-overview/upload-pdf/ESG%20Committee%20Charter.pdf', 'https://www.vedantalimited.com/uploads/esg/esg-sustainability-framework/Commit-Connect-Care.pdf', 'https://www.vedantalimited.com/img/media_mentions/media_reports/pdf/Assurance-Statement-for-Sustainability-Report-FY2022.pdf', 'https://www.vedantalimited.com/img/media_mentions/press_release/2022/PR-Vedanta%20Aluminium%20ranks%202nd%20in%20DJSI%20rankings%20for%20FY22-23Jan2023.pdf', 'https://www.vedantalimited.com/SiteAssets/Images/TCFD-FY2022.pdf', 'https://www.vedantalimited.com/img/media_mentions/media_reports/pdf/Vedanta%20Social%20Impact%20Report%202022%20Final%20Copy.pdf', 'https://www.vedantaresources.com/VedantaDocuments/Executive-Summary-SR-FY23.pdf', 'https://sd.vedantaresources.com/SustainableDevelopment2020-21/assets/vedanta_sdr_2021.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/vedanta-full-report.pdf.downloadasset.pdf', 'https://www.vedantaresources.com/VedantaDocuments/VedantaSDR2018-19.pdf', 'https://www.vedantaresources.com/VedantaDocuments/SDR_2020_full.pdf', 'https://www.vedantaresources.com/VedantaDocuments/TCFD-Report-FY23.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/VEDANTA_Sustainable_Development_Report%202010-11.pdf.downloadasset.pdf', 'https://sd.vedantaresources.com/SustainableDevelopment2020-21/assets/go_vedanta_sdr_2021.pdf', 'https://sd.vedantaresources.com/SustainableDevelopment2017-18/pdf/VedantaSDReport2014-15.pdf', 'https://www.vedantaresources.com/InvestorRelationDoc/TCFD%20report2020.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_Oct2013.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_Mar2013.pdf.pdf', 'https://sd.vedantaresources.com/SustainableDevelopment2017-18/pdf/VedantaSDReport2017-18.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_Nov2012.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_Dec2011.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_June2011.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_Jan2013.pdf.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_Nov2010.pdf', 'https://www.vedantaresources.com/SustainabilityDocs/ScottWilson_July2011.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657186978098/6b314ba848da94854d7291444e9c4e4e1219c419/shell-sustainability-report-english-2008.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657185136417/8c7cf7e17abcd9772af39994b88ed37a5a86e216/shell-sustainability-report-1998-1997.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657186623024/8cd24d2e9ec6886a6fc4de060a246196292c186f/shell-sustainability-report-2009.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657185138283/5ac60c0e204c8f180115bdcb3f3507acd8f197d0/shell-sustainability-report-2000-1999.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657185134567/3fd713226158687e0d7dac0a8506101868e96da5/shell-sustainability-report-1999-1998.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657185130114/6a8cd4fb3d0186c6c166400778d49b764647da29/shell-sustainability-report-2001.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657187941845/0c0475e570e524416e49dec411d27471b63234d3/shell-sustainability-review-english-2007.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657185128479/ca478a4d7e025f1a1ecb052267f17f03fc6ece6b/shell-sustainability-report-2003.pdf', 'https://www.shell.com/investors/information-for-shareholders/bg-documents/_jcr_content/root/main/section/simple/text_1316103549.multi.stream/1663748315262/bd22e5e1f0ba6a6ab2316bd005b1cf86c45c08c9/bg-group-sustainability-report-2015.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657186964992/0cdfe1fbe7e52c83ae1ac8907c0e347ad2117748/shell-sustainability-review-english-2008.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657183466402/65082acc8cb2e5336a9d332edaa5024bf1361080/shell-sustainability-summary-english-2012.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657185541738/1051284bed71f7eb4af6976a1d67d0c40461e530/shell-sustainability-report-2010.pdf', 'https://www.shell.com/investors/information-for-shareholders/bg-documents/_jcr_content/root/main/section/simple/text_1316103549.multi.stream/1663748566503/ffc3795d4f5c2feaf2004d86ba24757e0886b57a/bg-group-2014-sustainability-report-data-methodology.pdf', 'https://www.shell.com/sustainability/transparency-and-sustainability-reporting/sustainability-reports/_jcr_content/root/main/section/list/list_item.multi.stream/1657187594214/9529f21f280c8ad11ddaf2d1c2238edcf7374dd8/shell-sustainability-report-english-2007.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/1947/04/shell-background-report.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/1947/04/FinalPaperComp-1.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/1947/04/A-Case-Study-of-Nigeria.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2005/12/ShellNigeriaHistory.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2016/06/Slides2016Comp.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/1947/04/ArcticSovereigntySecurity-831.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2011/04/Karoo4.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2011/04/Karoo3.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2019/09/UNPO-REPORT-FEB-1995-John-Donovan-copy.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/1947/04/House-of-Commons-Environmental-Audit-Minutes-of-Evidence.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2009/06/ec.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2021/02/Okpabi-and-others-v-Royal-Dutch-Shell-Plc.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2014/02/eis-supplement.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2017/12/13Dec2017.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2017/04/oaks1.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2015/12/RDS-Wikipedia-Article-19-Jan-15-Comp.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2017/12/SundayTimesMI6.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2017/11/Criminal.pdf', 'https://royaldutchshellplc.com/wp-content/uploads/2019/06/ShellHistoryV2COMP29pages.pdf', 'https://www.shell.ca/en_ca/business-customers/lubricants-for-business/sustainability/sustainability-bottom-line/_jcr_content/par/textimage.stream/1613672743530/a83c9805e9f57b0f45de6d486afb2dcd9cfd003c/shell-lubricant-solutions-sustainability-whitepaper-2020.pdf', 'https://www.shell.ca/en_ca/sustainability/transparency/_jcr_content/par/textimage_934278137.stream/1686325509134/dfc146d8090424ceb6637234d99b27f67c886797/estma-shell-canada-energie-2022-updated.pdf', 'https://www.shell.ca/en_ca/about-us/powering-progress/_jcr_content/par/toptasks.stream/1635420766343/cbf0f8960d75c99ff359936c5c69009d9dd3b7d6/shell-powering-progress-20210412.pdf', 'https://www.shell.ca/en_ca/sustainability/transparency/_jcr_content/par/textimage_934278137.stream/1496700672787/1362f6ce9c5f7e05732230e79adb655c97a4e43f/estma-shell-canada-energy.pdf', 'https://www.shell.ca/en_ca/sustainability/transparency/_jcr_content/par/textimage_934278137.stream/1654044804248/9ab0a6f219724d94a958288015a851c06055ff49/2021-estma-shell-canada-energy.pdf', 'https://www.shell.ca/en_ca/sustainability/transparency/_jcr_content/par/textimage_934278137.stream/1500518780595/03d867fea5ef3c789f838118892f03772fa2e0c6/estma-shell-canada-energy-mocc.pdf', 'https://www.shell.ca/fr_ca/careers/retail-business-opportunities/cluster-opportunities/_jcr_content/par/tabbedcontent_17d/tab_2450/textimage_df81.stream/1519802591988/e581e5342d159cd4072fd5c121b90ae6b71b844f/the-power-of-shellretail.pdf', 'https://www.shell.ca/en_ca/sustainability/transparency/_jcr_content/par/textimage_934278137.stream/1590753229836/92f5e98da9d2d6684367ee468a7a5e08bedae8b3/2019-estma-1745844-alberta-ltd-eng-updated.pdf', 'https://www.shell.ca/fr_ca/about-us/projects-and-sites/caroline-gas-complex/_jcr_content/par/tabbedcontent_fe71/tab_eb87/textimage_ac67.stream/1519795007248/b9fdcd110b420b030ff0f3cb7288d54fdca6fba1/reclamation-in-alberta-foothills.pdf', 'https://www.shell.ca/fr_ca/about-us/projects-and-sites/deepwater-shelburne-basin-venture-exploration-project/_jcr_content/par/tabbedcontent_1019/tab_e76e/textimage_abf8.stream/1519810988190/165ab7b0a9db6cc383613623d11a89694041f036/project-implementation-schedule-cheshire-well.pdf', 'https://www.shell.ca/en_ca/about-us/projects-and-sites/sarnia-manufacturing-centre/_jcr_content/par/expandablelist/expandablesection_1158510984.stream/1617041657331/e9d0773d82fc55c83188b15c27319e1de0d60a06/2020ry-annual-ambient-monitoring-report-for-petroleum-refining-industry-standard.pdf', 'https://www.shell.ca/fr_ca/about-us/projects-and-sites/waterton-gas-complex/_jcr_content/par/tabbedcontent_5b8f/tab_eb87/textimage_ac67.stream/1519810862440/20af971458ffa7f611d563501fb25f7525de31cc/fall-waterton-chinook-newsletter.pdf', 'https://www.shell.ca/en_ca/about-us/projects-and-sites/sarnia-manufacturing-centre/_jcr_content/par/expandablelist/expandablesection_202774482.stream/1587748073980/18899dbdcfd4cff552cb1a1316e825282698b853/scl-pipeline-emergency-response-plan-2020.pdf', 'https://www.shell.ca/fr_ca/about-us/projects-and-sites/deepwater-shelburne-basin-venture-exploration-project/_jcr_content/par/tabbedcontent_1019/tab_e76e/textimage_abf8.stream/1519811047239/bfa73a5c93f6d8bf2ab8b2351d159810c96d4f69/project-implementation-schedule-monterey-jack.pdf', 'http://www.shell.ca/en_ca/about-us/projects-and-sites/deepwater-shelburne-basin-venture-exploration-project/_jcr_content/par/tabbedcontent_1019/tab_7a33/textimage_5b67.stream/1484600071925/12f38483f876bfb2258f1117b27089e5f3fb8e21220f6db42dc1bf7c848c9839/shelburne-ceaa-2-4-closure-report.pdf', 'https://www.shell.ca/en_ca/about-us/projects-and-sites/sarnia-manufacturing-centre/_jcr_content/par/expandablelist/expandablesection_202774482.stream/1587748152029/057bc32efe738cd94d27eac6a88473789b81ce3c/scl-pipeline-emergency-management-system-2020.pdf', 'https://www.shell.ca/en_ca/media/news-and-media-releases/news-releases-2019/in-partnership-with-htec/_jcr_content/par/textimage.stream/1575498447668/f70a0a8530eb73afae3e56851433215db9f02483/shell-hydrogen-research-overview-final.pdf', 'https://www.shell.ca/fr_ca/about-us/projects-and-sites/waterton-gas-complex/_jcr_content/par/tabbedcontent_5b8f/tab_eb87/textimage_ac67.stream/1519810933886/8943165e48411cbb7d33c4a515406664f7b9f079/waterton-fall-2013.pdf', 'https://www.shell.ca/en_ca/about-us/projects-and-sites/brockville-lubricants-plant/_jcr_content/par/expandablelist/expandablesection_239785051.stream/1519803138855/a2d20af64bf0f4eda632401e3d20e349074376f8/2012-toxic-reduction-plan.pdf', 'https://sustainability.adityabirla.com/ABG-ESG-Report/ABG-ESG-Full-Report-2021-Final-File-for-Web-Upload-28-Feb-2022.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/Hindalco-Sustainability-Report-2016-17-2017.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/sustainability-report-20-21.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/policies_reports_pdf_30_1614145577.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/hindalco-sustainability-report-fy19.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/Hindalco-Sustainability-Report-2015-16-2016.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/ABFRL-Sustainability-Report-2020-21.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/UltraTech_Cement_Sustainability_Report_FY21.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/policies_reports_pdf_36_1636614623.pdf', 'https://www.domsjo.adityabirla.com/Documents/Hallbarhetsredovisning/Domsj%C3%B6%20Fabriker%20Sustainability%20report%20FY22.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/idea/Idea-Cellular-Sustainable-Business-Report-2018.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/birla-sr-2021.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/Pulp%20and%20Fibre/2014.pdf', 'https://www.domsjo.adityabirla.com/en/Documents/Sustainability_report_Domsjo_Fabriker_AB_FY21.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/Pulp%20and%20Fibre/2016.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/Pulp%20and%20Fibre/2013.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/ABCL-SR-2020-21_Final.pdf', 'https://www.domsjo.adityabirla.com/Documents/Hallbarhetsredovisning/Domsjo_hallbarhetsredovisning.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/CFI-Sustainability-Report-2015-16-2016.pdf', 'https://sustainability.adityabirla.com/pdf/reportspdf/idea/2016.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/financials/annual-reports/integrated-and-sustainability-report-2023-double-page.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/ultratech-sr-2021-22.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/ultratTech-cement-sustainability-report-2020-21.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/ultratech-tcfd-report.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/UltraTech%20SDR%202017-18%20-%20Final%20web%20version%20-%20Feb19.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/UltraTech%20Sustainability%20Report%202016-17.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/UltraTech%20Cement%20Sustainbility%20Report%202018-19.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/financials/investor-update/ultratech-esg-presentation.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/UltraTech%20Cement_Sustainability_Report_2019-20.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/slb-report-june-2022.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/ultratech-esg-databook-fy-2022-23-002.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/financials/investor-update/7jpriOJT876qF95zhG02.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/Ultratech_Sustainability_Report%202010-2012-final.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/UltraTech%20Cement%20Sustsainability%20Report%202015-16.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/biodiversity-assesssment-mapping-with-cdsb.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/Alternatives_in_Action-UltraTech_Sustainability_Report.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/financials/investor-update/slb-report-june-2023.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/financials/annual-reports/annual-report-single-view.pdf', 'https://www.ultratechcement.com/content/dam/ultratechcementwebsite/pdf/sustainability-reports/UCL_SR2010-12_GRIContentIndex.pdf']
"""

# ## Next Steps
# ### The documents in df_reports have already been downloaded.
# ### We can now move on to processing the ones we would like to.
# ### See company_research/document_processing.py to see how to process documents.
