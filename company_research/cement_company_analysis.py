# # Analyse data for cement companies

import time
import pandas as pd
import utils.common
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

# ## set inputs

# ### set company names
company_names = [
    'Ultratech Cement',
    'Cemex Inc',
    'ACC Limited',
    'Heidelberg Materials Inc',
    'JK Cement',
    'Shree Cement',
    'China Resources Cement',
    'Eurocement Group',
    'Birla Corporation',
    'Lafarge Inc',
]

# ## set document keywords to search
doc_keywords = [
    'sustainability reports',
    'annual reports',
]

# ## Data sourcing

# ### trigger document download
resp = bg_async.download_documents(
    entity_names=company_names,
    doc_keywords=doc_keywords,
)

# ### wait for output to exist
time.sleep(60 * 60)

# ### check download output status
resp.check_output_file_exists()

# ### read output file
df_document_urls = pd.DataFrame(resp.read_output_data())

# ### Get unique doc_name
doc_names = df_document_urls['doc_name'].unique().tolist()

# ### save df_document_urls to local file
df_document_urls.to_csv(f"/tmp/document-urls_cement-companies.csv", index=False)

# ## Extract document info for downloaded documents

# ### make api calls
responses = []
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"Extracting document info for ({doc_num}/{len(doc_names)}): {doc_name}")
    resp_ = bg_async.extract_doc_info(
        doc_name=doc_name
    )
    responses = responses + [resp_]

# ### get doc_info data
missing_files = []
df_doc_info = pd.DataFrame()
for resp_num, resp in enumerate(responses):
    logger.info(f"Reading document info for ({resp_num}/{len(responses)})")
    if resp.check_output_file_exists():
        df_doc_info_ = pd.DataFrame(resp.read_output_data())
        df_doc_info = pd.concat([df_doc_info, df_doc_info_])
    else:
        missing_files = missing_files + resp.get_output_file()

# ### check df_doc_info
logger.info(f"{len(df_doc_info)} rows found in df_doc_info")
logger.info(f"{len(missing_files)} files with missing document info")

# ## save document info locally
tmp_file = f"/tmp/downloaded-docs_cement-companies.csv"
df_doc_info.to_csv(tmp_file)

# ### read data from local file
df_doc_info = pd.read_csv(tmp_file)

# ## Process document info

# ### get unique doc_year
doc_years = df_doc_info['doc_year'].unique().tolist()

# ### convert years to numeric
df_years_num = pd.DataFrame()
for yr in doc_years:
    logger.info(f"converting {yr} to numeric")
    df_yr_num_ = bg_sync.extract_text_years(str(yr)).get_data()
    df_yr_num_ = pd.DataFrame(df_yr_num_)
    df_years_num = pd.concat([df_years_num, df_yr_num_])

# ### merge numeric years onto df_doc_info
df_doc_info = pd.merge(
    left=df_doc_info,
    right=df_years_num.rename(
        columns={'text': 'doc_year',
                 'year': 'doc_year_num'}
    ),
    on=['doc_year'],
    how='left'
)

# ### convert doc_year_num to float type
df_doc_info['doc_year_num'] = df_doc_info['doc_year_num'].astype(float)

# ### check numeric years
"""
df_doc_info[['doc_year', 'doc_year_num']].drop_duplicates().values.tolist()
[['2022', '2022'], ['2018', '2018'], ['2009', '2009'], ['2019', '2019'], ['2021', '2021'], ['2017', '2017'], ['2020', '2020'], ['2014', '2014'], ['2023', '2023'], ['2020-21', '2020'], ['2018-2019', '2018'], ['2016-17', '2016'], ['2019-20', '2019'], ['2011', '2011'], ['2016', '2016'], ['2012', '2012'], ['2015', '2015'], ['2018-19', '2018'], ['2006', '2006'], [nan, nan], ['2013', '2013'], ['2011/2012', '2011'], ['2011/2012', '2012'], ['2002', '2002'], ['2007', '2007'], ['2010', '2010'], ['2004', '2004'], ['2019-2020', '2019']]
"""

# ### convert doc_type to string format
df_doc_info = utils.common.convert_list_cols_to_str(
    df=df_doc_info,
    cols=['doc_type']
)

# ### check doc_type
"""
df_doc_info['doc_type'].unique().tolist()
['annual report', 'sustainability report', 'TCFD report', 'financial statement', nan, 'other', 'sustainable financing document', 'human rights policy', 'sustainable financing document; sustainability report', 'press release']
"""

# ### save data locally
df_doc_info.to_csv(f"/tmp/downloaded-docs_cement-companies_processed.csv", index=False)

# ## Merge doc_info with document_url data
df_document_urls = pd.read_csv("/tmp/document-urls_cement-companies.csv")
df_doc_info = pd.read_csv(f"/tmp/downloaded-docs_cement-companies_processed.csv")
df_doc_details = pd.merge(
    left=df_document_urls,
    right=df_doc_info,
    on=['doc_name'],
    how='left'
)

# ### check df_doc_details
"""
df_doc_details.head().to_dict('records')
[{'doc_name': 'httpssustainabilityadityabirlacomabg-esg-reportabg-esg-full-report-2021-final-file-for-web-upload-28-feb-2022pdf', 'entity_name': 'Ultratech Cement', 'href': 'https://sustainability.adityabirla.com/ABG-ESG-Report/ABG-ESG-Full-Report-2021-Final-File-for-Web-Upload-28-Feb-2022.pdf', 'href_text': nan, 'keyphrase': 'sustainability reports', 'page_summary': nan, 'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_JaCqcb"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><a href="https://sustainability.adityabirla.com/ABG-ESG-Report/ABG-ESG-Full-Report-2021-Final-File-for-Web-Upload-28-Feb-2022.pdf" jscontroller="M9mgyc" jsname="qOiK6e" jsaction="rcuQ6b:npT2md" data-jsarwt="1" data-usg="AOvVaw1hnKyMYvfpmRnyfkoFwHBM" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8QFnoECBEQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">ABG Report for PDF</h3><div class="TbwUpd NJjxre iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAABQ0lEQVR4AWP4////779/geSdt7dq9tftvL0LyP7779d/EPj3HwMw/Pn3B0i9+vrKZaGTXK+c2hTVXbf3/v+DrOcfFg2th5qBqs1nmwM1KLf5pnT9Onb1J0QPRBtcM8hJX399BRqvMUXTeLahXJc+V62Kfe3JpMafbydH/b+67s//b8j2gDScf34eaDBQNVCPQL0BZ6O8RVN/UPaPLXnJ/3tYf3+z/vvvGNCRQD1QDWuurwW6B6hBbaIeRINGdTJQQ3fZtP+VLH+vy35+pvRgcR7QhVANM07PQNOgWOUfnPm8pGgdUMPvXWLfPvFcbxB6d34zigaIB4AagH6QrXYNznqQU7AbZMNKLqCGO5PEnmybANIACSJiNWA6CaIBl5NI9jSZwUo44iCpC6oBmPLwJA2ILFAp4cQHFMee+IB87MkbBwAAu40NAwqRmZUAAAAASUVORK5CYII=" style="height:18px;width:18px" alt="" data-atf="1" data-frt="0"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › ABG-ES...</span></cite></div></div></div></a><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › ABG-ES...</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-isdesktop="true" jsdata="l7Bhpb;_;AQQNpg cECq7c;_;AQQNps" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Q2esEegQIERAK"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Qh-4GegQIERAL"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf fS1kJf MUxGbd yDYNvb lyLwlc lEBKkf" style="-webkit-line-clamp:2"><span class="MUxGbd wuQ4Ob WZ8Tjf"><span>Mar 9, 2022</span> — </span><span>This <em>report</em> summarises the performance of ABG businesses, with their diversity of sectors, geographies, across Environment, Social and.</span></div><div class="MUxGbd wuQ4Ob WZ8Tjf"><span>82 pages</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>', 'result_text': 'ABG Report for PDF\nadityabirla.com\nhttps://sustainability.adityabirla.com › ABG-ES...\nPDF\nMar 9, 2022 — This report summarises the performance of ABG businesses, with their diversity of sectors, geographies, across Environment, Social and.\n82 pages', 'doc_org': 'GSE', 'doc_type': "['annual report']", 'doc_year': '2022', 'num_pages': 82.0, 'doc_year_num': 2022.0}, {'doc_name': 'httpssustainabilityadityabirlacompdfreportspdfhindalco-sustainability-report-2016-17-2017pdf', 'entity_name': 'Ultratech Cement', 'href': 'https://sustainability.adityabirla.com/pdf/reportspdf/Hindalco-Sustainability-Report-2016-17-2017.pdf', 'href_text': nan, 'keyphrase': 'sustainability reports', 'page_summary': nan, 'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_Oe7x2"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><a href="https://sustainability.adityabirla.com/pdf/reportspdf/Hindalco-Sustainability-Report-2016-17-2017.pdf" jscontroller="M9mgyc" jsname="qOiK6e" jsaction="rcuQ6b:npT2md" data-jsarwt="1" data-usg="AOvVaw3cP20T3VzDIpI2K64EJ637" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8QFnoECBQQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">Hindalco Sustainability Report 2016 - 17</h3><div class="TbwUpd NJjxre iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAABQ0lEQVR4AWP4////779/geSdt7dq9tftvL0LyP7779d/EPj3HwMw/Pn3B0i9+vrKZaGTXK+c2hTVXbf3/v+DrOcfFg2th5qBqs1nmwM1KLf5pnT9Onb1J0QPRBtcM8hJX399BRqvMUXTeLahXJc+V62Kfe3JpMafbydH/b+67s//b8j2gDScf34eaDBQNVCPQL0BZ6O8RVN/UPaPLXnJ/3tYf3+z/vvvGNCRQD1QDWuurwW6B6hBbaIeRINGdTJQQ3fZtP+VLH+vy35+pvRgcR7QhVANM07PQNOgWOUfnPm8pGgdUMPvXWLfPvFcbxB6d34zigaIB4AagH6QrXYNznqQU7AbZMNKLqCGO5PEnmybANIACSJiNWA6CaIBl5NI9jSZwUo44iCpC6oBmPLwJA2ILFAp4cQHFMee+IB87MkbBwAAu40NAwqRmZUAAAAASUVORK5CYII=" style="height:18px;width:18px" alt="" data-atf="1" data-frt="0"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite></div></div></div></a><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-isdesktop="true" jsdata="l7Bhpb;_;AQQNqA cECq7c;_;AQQNqM" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Q2esEegQIFBAK"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Qh-4GegQIFBAL"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf MUxGbd yDYNvb lyLwlc lEBKkf" style="-webkit-line-clamp:2"><span>Based on the financial year, our <em>sustainability report</em> is annually published3 and all our <em>sustainability reports</em> are available online on our website http://www.</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>', 'result_text': 'Hindalco Sustainability Report 2016 - 17\nadityabirla.com\nhttps://sustainability.adityabirla.com › reportspdf\nPDF\nBased on the financial year, our sustainability report is annually published3 and all our sustainability reports are available online on our website http://www.', 'doc_org': 'Hindalco', 'doc_type': "['sustainability report']", 'doc_year': '2018', 'num_pages': 82.0, 'doc_year_num': 2018.0}, {'doc_name': 'httpssustainabilityadityabirlacompdfreportspdfpolicies_reports_pdf_30_1614145577pdf', 'entity_name': 'Ultratech Cement', 'href': 'https://sustainability.adityabirla.com/pdf/reportspdf/policies_reports_pdf_30_1614145577.pdf', 'href_text': nan, 'keyphrase': 'sustainability reports', 'page_summary': nan, 'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_XlF2gd"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><a href="https://sustainability.adityabirla.com/pdf/reportspdf/policies_reports_pdf_30_1614145577.pdf" jscontroller="M9mgyc" jsname="qOiK6e" jsaction="rcuQ6b:npT2md" data-jsarwt="1" data-usg="AOvVaw3XTo28c9B2DYbmoJDfki8Y" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8QFnoECA8QAQ"><br><h3 class="LC20lb MBeuO DKV0Md">Sustainability Report 2019-20</h3><div class="TbwUpd NJjxre iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAABQ0lEQVR4AWP4////779/geSdt7dq9tftvL0LyP7779d/EPj3HwMw/Pn3B0i9+vrKZaGTXK+c2hTVXbf3/v+DrOcfFg2th5qBqs1nmwM1KLf5pnT9Onb1J0QPRBtcM8hJX399BRqvMUXTeLahXJc+V62Kfe3JpMafbydH/b+67s//b8j2gDScf34eaDBQNVCPQL0BZ6O8RVN/UPaPLXnJ/3tYf3+z/vvvGNCRQD1QDWuurwW6B6hBbaIeRINGdTJQQ3fZtP+VLH+vy35+pvRgcR7QhVANM07PQNOgWOUfnPm8pGgdUMPvXWLfPvFcbxB6d34zigaIB4AagH6QrXYNznqQU7AbZMNKLqCGO5PEnmybANIACSJiNWA6CaIBl5NI9jSZwUo44iCpC6oBmPLwJA2ILFAp4cQHFMee+IB87MkbBwAAu40NAwqRmZUAAAAASUVORK5CYII=" style="height:18px;width:18px" alt="" data-atf="1" data-frt="0"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite></div></div></div></a><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-isdesktop="true" jsdata="l7Bhpb;_;AQQNp4 cECq7c;_;AQQNqE" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Q2esEegQIDxAK"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Qh-4GegQIDxAL"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf MUxGbd yDYNvb lyLwlc lEBKkf" style="-webkit-line-clamp:2"><span class="MUxGbd wuQ4Ob WZ8Tjf"><span>Feb 24, 2021</span> — </span><span>This <em>report</em> follows the structure of our first <em>report</em> where the first part showcases the intrinsic <em>sustainability</em> attributes of man-made&nbsp;...</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>', 'result_text': 'Sustainability Report 2019-20\nadityabirla.com\nhttps://sustainability.adityabirla.com › reportspdf\nPDF\nFeb 24, 2021 — This report follows the structure of our first report where the first part showcases the intrinsic sustainability attributes of man-made ...', 'doc_org': 'Birla Cellulose', 'doc_type': "['sustainability report']", 'doc_year': '2009', 'num_pages': 79.0, 'doc_year_num': 2009.0}, {'doc_name': 'httpssustainabilityadityabirlacompdfreportspdfhindalco-sustainability-report-fy19pdf', 'entity_name': 'Ultratech Cement', 'href': 'https://sustainability.adityabirla.com/pdf/reportspdf/hindalco-sustainability-report-fy19.pdf', 'href_text': nan, 'keyphrase': 'sustainability reports', 'page_summary': nan, 'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_KTd5ie"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><a href="https://sustainability.adityabirla.com/pdf/reportspdf/hindalco-sustainability-report-fy19.pdf" jscontroller="M9mgyc" jsname="qOiK6e" jsaction="rcuQ6b:npT2md" data-jsarwt="1" data-usg="AOvVaw2akD8Urzw2jWLH0yweqR6w" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8QFnoECBAQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">Sustainability report 2018-19</h3><div class="TbwUpd NJjxre iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAABQ0lEQVR4AWP4////779/geSdt7dq9tftvL0LyP7779d/EPj3HwMw/Pn3B0i9+vrKZaGTXK+c2hTVXbf3/v+DrOcfFg2th5qBqs1nmwM1KLf5pnT9Onb1J0QPRBtcM8hJX399BRqvMUXTeLahXJc+V62Kfe3JpMafbydH/b+67s//b8j2gDScf34eaDBQNVCPQL0BZ6O8RVN/UPaPLXnJ/3tYf3+z/vvvGNCRQD1QDWuurwW6B6hBbaIeRINGdTJQQ3fZtP+VLH+vy35+pvRgcR7QhVANM07PQNOgWOUfnPm8pGgdUMPvXWLfPvFcbxB6d34zigaIB4AagH6QrXYNznqQU7AbZMNKLqCGO5PEnmybANIACSJiNWA6CaIBl5NI9jSZwUo44iCpC6oBmPLwJA2ILFAp4cQHFMee+IB87MkbBwAAu40NAwqRmZUAAAAASUVORK5CYII=" style="height:18px;width:18px" alt="" data-atf="4" data-frt="0"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite></div></div></div></a><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-isdesktop="true" jsdata="l7Bhpb;_;AQQNpc cECq7c;_;AQQNpw" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Q2esEegQIEBAK"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Qh-4GegQIEBAL"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf MUxGbd yDYNvb lyLwlc lEBKkf" style="-webkit-line-clamp:2"><span>We publish our <em>sustainability reports</em> on an annual basis. All of our <em>sustainability reports</em>, including the previous sustainability.</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>', 'result_text': 'Sustainability report 2018-19\nadityabirla.com\nhttps://sustainability.adityabirla.com › reportspdf\nPDF\nWe publish our sustainability reports on an annual basis. All of our sustainability reports, including the previous sustainability.', 'doc_org': 'Hindalco Industries Limited', 'doc_type': "['sustainability report']", 'doc_year': '2019', 'num_pages': 132.0, 'doc_year_num': 2019.0}, {'doc_name': 'httpssustainabilityadityabirlacompdfreportspdfsustainability-report-20-21pdf', 'entity_name': 'Ultratech Cement', 'href': 'https://sustainability.adityabirla.com/pdf/reportspdf/sustainability-report-20-21.pdf', 'href_text': nan, 'keyphrase': 'sustainability reports', 'page_summary': nan, 'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_WjE4zc"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><a href="https://sustainability.adityabirla.com/pdf/reportspdf/sustainability-report-20-21.pdf" jscontroller="M9mgyc" jsname="qOiK6e" jsaction="rcuQ6b:npT2md" data-jsarwt="1" data-usg="AOvVaw06kzNTQnJXMd5hybVR0yEs" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8QFnoECBYQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">REPORT 2020-21 - ABG Sustainability - Aditya Birla Group</h3><div class="TbwUpd NJjxre iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAABQ0lEQVR4AWP4////779/geSdt7dq9tftvL0LyP7779d/EPj3HwMw/Pn3B0i9+vrKZaGTXK+c2hTVXbf3/v+DrOcfFg2th5qBqs1nmwM1KLf5pnT9Onb1J0QPRBtcM8hJX399BRqvMUXTeLahXJc+V62Kfe3JpMafbydH/b+67s//b8j2gDScf34eaDBQNVCPQL0BZ6O8RVN/UPaPLXnJ/3tYf3+z/vvvGNCRQD1QDWuurwW6B6hBbaIeRINGdTJQQ3fZtP+VLH+vy35+pvRgcR7QhVANM07PQNOgWOUfnPm8pGgdUMPvXWLfPvFcbxB6d34zigaIB4AagH6QrXYNznqQU7AbZMNKLqCGO5PEnmybANIACSJiNWA6CaIBl5NI9jSZwUo44iCpC6oBmPLwJA2ILFAp4cQHFMee+IB87MkbBwAAu40NAwqRmZUAAAAASUVORK5CYII=" style="height:18px;width:18px" alt="" data-atf="4" data-frt="0"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite></div></div></div></a><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd iUh30 ojE3Fb"><span class="H9lube"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div><span class="VuuXrf">adityabirla.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://sustainability.adityabirla.com<span class="dyjrff ob9lvb" role="text"> › reportspdf</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-isdesktop="true" jsdata="l7Bhpb;_;AQQNpY cECq7c;_;AQQNpo" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Q2esEegQIFhAK"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwimv-v90oaBAxXOhIkEHUroDK8Qh-4GegQIFhAL"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf fS1kJf MUxGbd yDYNvb lyLwlc lEBKkf" style="-webkit-line-clamp:2"><span>This chapter details EMIL\'s <em>environmental</em> journey, which involves responsible mining, energy initiatives, Scope1, Scope 2 and other air emissions, water&nbsp;...</span></div><div class="MUxGbd wuQ4Ob WZ8Tjf"><span>120 pages</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>', 'result_text': "REPORT 2020-21 - ABG Sustainability - Aditya Birla Group\nadityabirla.com\nhttps://sustainability.adityabirla.com › reportspdf\nPDF\nThis chapter details EMIL's environmental journey, which involves responsible mining, energy initiatives, Scope1, Scope 2 and other air emissions, water ...\n120 pages", 'doc_org': 'Employee', 'doc_type': "['sustainability report']", 'doc_year': '2021', 'num_pages': 120.0, 'doc_year_num': 2021.0}]
"""

# ### save df_doc_details to local file
df_doc_details.to_csv(f"/tmp/doc-details_cement-companies.csv", index=False)

# ## filter documents

# ### read data from file
df_doc_details = pd.read_csv(f"/tmp/doc-details_cement-companies.csv")

# ### filter documents by doc_type, doc_year, num_pages
df_doc_details = df_doc_details[
    (df_doc_details['doc_year_num'] > 2021) &
    (df_doc_details['doc_type'].str.contains('annual report|sustainability report')) &
    (df_doc_details['num_pages'] >= 20)
]

# ### check df_doc_info
"""
df_doc_details.to_dict('records')
"""

# ## trigger processing for selected documents

# ### get document names
doc_names = df_doc_details['doc_name'].unique().tolist()

# ### trigger processing for documents, in batches of 15 documents, to avoid exceeding rate limit
for doc_num, doc_name in enumerate(doc_names[25:]):
    logger.info(f"triggering processing for ({doc_num}/{len(doc_names)}): {doc_name}")
    resp_ = bg_async.structure_quants_pipeline(
        doc_name=doc_name,
    )
    if (doc_num > 0) and (doc_num % 15 == 0):
        time.sleep(1 * 60 * 60)

# ### check if synthesized-quants data exists
quant_files = {}
for doc_num, doc_name in enumerate(doc_names):
    logger.info(f"checking quants data for ({doc_num}/{len(doc_names)}): {doc_name}")
    quant_files_ = bg_sync.list_doc_files(
        doc_name=doc_name,
        file_pattern='variable_desc=synthesized-quants/**.csv',
    ).get_data()
    if quant_files_ is not None:
        logger.info(f"found {len(quant_files_)} quant files for {doc_name}")
        quant_files[doc_name] = quant_files_

# ### check quant_files
"""
len(quant_files)
49
len(quant_files) == len(doc_names)
True
"""

# ### Handle missing output
"""
Note that sometimes document processing may fail to complete successfully due to some random errors, like API call time, rate limit errors, etc.without finishing. 
In  this case, the document processing pipeline can be triggered again. By default API calls check for previously existing output first, 
and generate new output if the output does not already exists. Hence, re-triggering a document processing pipeline will 
just fill up any missing output, while leaving the existing output intact. 
"""



