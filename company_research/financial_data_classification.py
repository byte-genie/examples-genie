# # Extract financial data from financial statements

import time
import pandas as pd
import utils.common
import utils.async_utils
from utils.logging import logger
from utils.byte_genie import ByteGenie

# ## init byte-genie
bg = ByteGenie(
    secrets_file='secrets.json',
    verbose=1,
)

# ## Search annual reports for Nividia
search_payload = bg.create_api_payload(
    func='download_documents',
    args={
        'entity_names': ['Nvidia Inc'],
        'doc_keywords': ['annual reports']
    },
    task_mode='sync',
)
search_resp = bg.call_api(
    payload=search_payload,
)

# ## Check search results
df_search = pd.DataFrame(search_resp.get_output())
"""
These search results containt the URLs of documents found from Nvidia's website that could likely be annual reports.
Here is a sample output for search results, `df_search.head().to_dict('records')`
[
    {
        'href': 'https://www.nvidia.com/content/dam/en-zz/Solutions/about-us/documents/NVDIA-Finance-Team-Code-of-Conduct-External.pdf',
        'result_text': 'FINANCE TEAM CODE\nnvidia.com\nhttps://www.nvidia.com › about-us › documents\nPDF\nJun 22, 2023 — This Finance Team Code sets forth principles adopted by NVIDIA to create the highest level of confidence in its accounting.',
        'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_i26Iob"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><span jscontroller="msmzHf" jsaction="rcuQ6b:npT2md;PYDNKe:bLV6Bd;mLt3mc"><a jsname="UWckNb" href="https://www.nvidia.com/content/dam/en-zz/Solutions/about-us/documents/NVDIA-Finance-Team-Code-of-Conduct-External.pdf" data-jsarwt="1" data-usg="AOvVaw0OtPEyR2mEqgLeXcgjdpkh" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQFnoECA8QAQ"><br><h3 class="LC20lb MBeuO DKV0Md">FINANCE TEAM CODE</h3><div class="notranslate TbwUpd YmJh3d NJjxre iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAACLElEQVR4Ae3WA4wlQRSF4bVt27YRrG3btm3bRrC2bds2xjZ75k9yk9yM7Uq+h1vV1ee9ViVKaKFpCS2hTb6UaAJuRpA+YQmwDT4RZE5od54MG2EJZ3hHegAGlcAM3IQVRkg9BYqgJ47BJUID0NkQl+Apk9/ASrEdc9EUKWV8fmyEe7gCUCyMM/CGGSYiWyDngAETGZNC+qvgXagD8CUxhsMRBvYhk/SlQ3fswnhUQ2tshJ1M9gLFZXxaHIMRogCywUEY8MYEdZwnwkLq3zEKKVTw3DgPHxlXVZ2we2GEJMB1VZwrtVy4K7XXqIXE2IY/aOzn6ngoY82QXurJ8TEkAbaq4gCpDZHvXsjl9xyAHQojMRaqS3KpGjslpP9AIThL8R2SICMspTbEbwBxHofVCbkNicUMFcpL+oM8CeeqjsFSGwoDZsjnN4DihUUSPAW2qB3+QnWMhFlQAVLisXTYoCgSY6/U3qCEDiA+oZm6aT1Qfdf9HL7MWA1PHUCHKIj/6sTLiqRYKhs5Yz3WYAlaIDkyYj6cZFs3zEIyPb/MNRkeQd2IysFE/epiUi8t6UdJ0JJoj+3qXuCNiyjjZ87EaIrnIboVy9n9RAbZYxqyBHEO2GA/avuZJx16y1xGaJ8FKTFFXQmOuIxlWIK5GIzaSK22y4C22AfriHgaZsBAnIIJhqu+tCiDzliKO3CJtPWA7HQTbOAII+LXAzF5RSQB6qBfBKmUKKEltJjQfAHHiLcdp8EE6gAAAABJRU5ErkJggg==" style="height:18px;width:18px" alt="" data-csiid="14" data-atf="4"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://www.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › about-us › documents</span></cite></div></div></div><span jscontroller="IX53Tb" jsaction="rcuQ6b:npT2md" style="display:none"></span></a></span><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd YmJh3d iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://www.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › about-us › documents</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-frm="true" data-isdesktop="true" jsdata="l7Bhpb;_;BY9TTk cECq7c;_;BY9TT0" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQ2esEegQIDxAM"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQh-4GegQIDxAN"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf lVm3ye r025kc hJNv6b Hdw6tb" style="-webkit-line-clamp:2"><span class="LEwnzc Sqrs4e"><span>Jun 22, 2023</span> — </span><span>This Finance Team Code sets forth principles adopted by NVIDIA to create the highest level of confidence in its <em>accounting</em>.</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>',
        'href_text': '', 'page_summary': '',
        'doc_name': 'httpswwwnvidiacomcontentdamen-zzsolutionsabout-usdocumentsnvdia-finance-team-code-of-conduct-externalpdf',
        'entity_name': 'Nvidia Inc', 'keyphrase': 'annual reports'
    },
    {
        'href': 'https://images.nvidia.com/aem-dam/Solutions/documents/FY23-PwC-Assurance-Report-Management-Assertion_SIGNED-FINAL.pdf',
        'result_text': 'Report of Independent Accountants\nnvidia.com\nhttps://images.nvidia.com › documents › FY23-...\nPDF\nMay 5, 2023 — Emissions from the first through the third quarter were calculated based on third-party invoices or annual summaries obtained from waste ...',
        'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_ibpXge"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><span jscontroller="msmzHf" jsaction="rcuQ6b:npT2md;PYDNKe:bLV6Bd;mLt3mc"><a jsname="UWckNb" href="https://images.nvidia.com/aem-dam/Solutions/documents/FY23-PwC-Assurance-Report-Management-Assertion_SIGNED-FINAL.pdf" data-jsarwt="1" data-usg="AOvVaw0N7m2uWjTUsDkgrfT0G4bQ" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQFnoECA4QAQ"><br><h3 class="LC20lb MBeuO DKV0Md">Report of Independent Accountants</h3><div class="notranslate TbwUpd YmJh3d NJjxre iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAACLElEQVR4Ae3WA4wlQRSF4bVt27YRrG3btm3bRrC2bds2xjZ75k9yk9yM7Uq+h1vV1ee9ViVKaKFpCS2hTb6UaAJuRpA+YQmwDT4RZE5od54MG2EJZ3hHegAGlcAM3IQVRkg9BYqgJ47BJUID0NkQl+Apk9/ASrEdc9EUKWV8fmyEe7gCUCyMM/CGGSYiWyDngAETGZNC+qvgXagD8CUxhsMRBvYhk/SlQ3fswnhUQ2tshJ1M9gLFZXxaHIMRogCywUEY8MYEdZwnwkLq3zEKKVTw3DgPHxlXVZ2we2GEJMB1VZwrtVy4K7XXqIXE2IY/aOzn6ngoY82QXurJ8TEkAbaq4gCpDZHvXsjl9xyAHQojMRaqS3KpGjslpP9AIThL8R2SICMspTbEbwBxHofVCbkNicUMFcpL+oM8CeeqjsFSGwoDZsjnN4DihUUSPAW2qB3+QnWMhFlQAVLisXTYoCgSY6/U3qCEDiA+oZm6aT1Qfdf9HL7MWA1PHUCHKIj/6sTLiqRYKhs5Yz3WYAlaIDkyYj6cZFs3zEIyPb/MNRkeQd2IysFE/epiUi8t6UdJ0JJoj+3qXuCNiyjjZ87EaIrnIboVy9n9RAbZYxqyBHEO2GA/avuZJx16y1xGaJ8FKTFFXQmOuIxlWIK5GIzaSK22y4C22AfriHgaZsBAnIIJhqu+tCiDzliKO3CJtPWA7HQTbOAII+LXAzF5RSQB6qBfBKmUKKEltJjQfAHHiLcdp8EE6gAAAABJRU5ErkJggg==" style="height:18px;width:18px" alt="" data-csiid="15" data-atf="4"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://images.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › documents › FY23-...</span></cite></div></div></div><span jscontroller="IX53Tb" jsaction="rcuQ6b:npT2md" style="display:none"></span></a></span><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd YmJh3d iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://images.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › documents › FY23-...</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-frm="true" data-isdesktop="true" jsdata="l7Bhpb;_;BY9TTs cECq7c;_;BY9TT8" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQ2esEegQIDhAM"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQh-4GegQIDhAN"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf lVm3ye r025kc hJNv6b Hdw6tb" style="-webkit-line-clamp:2"><span class="LEwnzc Sqrs4e"><span>May 5, 2023</span> — </span><span>Emissions from the first through the third quarter were calculated based on third-party invoices or <em>annual</em> summaries obtained from waste&nbsp;...</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>',
        'href_text': '', 'page_summary': '',
        'doc_name': 'httpsimagesnvidiacomaem-damsolutionsdocumentsfy23-pwc-assurance-report-management-assertion_signed-finalpdf',
        'entity_name': 'Nvidia Inc', 'keyphrase': 'annual reports'
    },
    {
        'href': 'https://images.nvidia.com/aem-dam/Solutions/documents/FY2022-NVIDIA-Corporate-Responsibility.pdf',
        'result_text': 'NVIDIA 2022 Corporate Responsibility Report\nnvidia.com\nhttps://images.nvidia.com › documents › FY20...\nPDF\nJul 7, 2022 — We ensure strong pay for all employees through an annual review of peer compensation practices in the markets we operate in and annual ...\n69 pages',
        'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_HIrkae"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><span jscontroller="msmzHf" jsaction="rcuQ6b:npT2md;PYDNKe:bLV6Bd;mLt3mc"><a jsname="UWckNb" href="https://images.nvidia.com/aem-dam/Solutions/documents/FY2022-NVIDIA-Corporate-Responsibility.pdf" data-jsarwt="1" data-usg="AOvVaw1I5RH4o64j2dwqYU0JETRR" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQFnoECBIQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">NVIDIA 2022 Corporate Responsibility Report</h3><div class="notranslate TbwUpd YmJh3d NJjxre iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAACLElEQVR4Ae3WA4wlQRSF4bVt27YRrG3btm3bRrC2bds2xjZ75k9yk9yM7Uq+h1vV1ee9ViVKaKFpCS2hTb6UaAJuRpA+YQmwDT4RZE5od54MG2EJZ3hHegAGlcAM3IQVRkg9BYqgJ47BJUID0NkQl+Apk9/ASrEdc9EUKWV8fmyEe7gCUCyMM/CGGSYiWyDngAETGZNC+qvgXagD8CUxhsMRBvYhk/SlQ3fswnhUQ2tshJ1M9gLFZXxaHIMRogCywUEY8MYEdZwnwkLq3zEKKVTw3DgPHxlXVZ2we2GEJMB1VZwrtVy4K7XXqIXE2IY/aOzn6ngoY82QXurJ8TEkAbaq4gCpDZHvXsjl9xyAHQojMRaqS3KpGjslpP9AIThL8R2SICMspTbEbwBxHofVCbkNicUMFcpL+oM8CeeqjsFSGwoDZsjnN4DihUUSPAW2qB3+QnWMhFlQAVLisXTYoCgSY6/U3qCEDiA+oZm6aT1Qfdf9HL7MWA1PHUCHKIj/6sTLiqRYKhs5Yz3WYAlaIDkyYj6cZFs3zEIyPb/MNRkeQd2IysFE/epiUi8t6UdJ0JJoj+3qXuCNiyjjZ87EaIrnIboVy9n9RAbZYxqyBHEO2GA/avuZJx16y1xGaJ8FKTFFXQmOuIxlWIK5GIzaSK22y4C22AfriHgaZsBAnIIJhqu+tCiDzliKO3CJtPWA7HQTbOAII+LXAzF5RSQB6qBfBKmUKKEltJjQfAHHiLcdp8EE6gAAAABJRU5ErkJggg==" style="height:18px;width:18px" alt="" data-csiid="16" data-atf="4"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://images.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › documents › FY20...</span></cite></div></div></div><span jscontroller="IX53Tb" jsaction="rcuQ6b:npT2md" style="display:none"></span></a></span><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd YmJh3d iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://images.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › documents › FY20...</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-frm="true" data-isdesktop="true" jsdata="l7Bhpb;_;BY9TTw cECq7c;_;BY9TUA" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQ2esEegQIEhAM"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQh-4GegQIEhAN"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf fS1kJf lVm3ye r025kc hJNv6b Hdw6tb" style="-webkit-line-clamp:2"><span class="LEwnzc Sqrs4e"><span>Jul 7, 2022</span> — </span><span>We ensure strong pay for all employees through an <em>annual</em> review of peer compensation practices in the markets we operate in and <em>annual</em>&nbsp;...</span></div><div class="LEwnzc Sqrs4e"><span>69 pages</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>',
        'href_text': '', 'page_summary': '',
        'doc_name': 'httpsimagesnvidiacomaem-damsolutionsdocumentsfy2022-nvidia-corporate-responsibilitypdf',
        'entity_name': 'Nvidia Inc', 'keyphrase': 'annual reports'
    },
    {
        'href': 'https://images.nvidia.com/aem-dam/Solutions/documents/FY2023-NVIDIA-Corporate-Responsibility-Report-1.pdf',
        'result_text': 'NVIDIA Corporate Responsibility Report Fiscal Year 2023\nnvidia.com\nhttps://images.nvidia.com › documents › FY20...\nPDF\nThe corporate responsibility reporting team updates the NCGC, and their feedback combined with executive input, helps to determine annual program direction. In ...\n59 pages',
        'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_muiOUe"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><span jscontroller="msmzHf" jsaction="rcuQ6b:npT2md;PYDNKe:bLV6Bd;mLt3mc"><a jsname="UWckNb" href="https://images.nvidia.com/aem-dam/Solutions/documents/FY2023-NVIDIA-Corporate-Responsibility-Report-1.pdf" data-jsarwt="1" data-usg="AOvVaw1IJC0AZft2l_-hiRjfv19l" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQFnoECCgQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">NVIDIA Corporate Responsibility Report Fiscal Year 2023</h3><div class="notranslate TbwUpd YmJh3d NJjxre iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAACLElEQVR4Ae3WA4wlQRSF4bVt27YRrG3btm3bRrC2bds2xjZ75k9yk9yM7Uq+h1vV1ee9ViVKaKFpCS2hTb6UaAJuRpA+YQmwDT4RZE5od54MG2EJZ3hHegAGlcAM3IQVRkg9BYqgJ47BJUID0NkQl+Apk9/ASrEdc9EUKWV8fmyEe7gCUCyMM/CGGSYiWyDngAETGZNC+qvgXagD8CUxhsMRBvYhk/SlQ3fswnhUQ2tshJ1M9gLFZXxaHIMRogCywUEY8MYEdZwnwkLq3zEKKVTw3DgPHxlXVZ2we2GEJMB1VZwrtVy4K7XXqIXE2IY/aOzn6ngoY82QXurJ8TEkAbaq4gCpDZHvXsjl9xyAHQojMRaqS3KpGjslpP9AIThL8R2SICMspTbEbwBxHofVCbkNicUMFcpL+oM8CeeqjsFSGwoDZsjnN4DihUUSPAW2qB3+QnWMhFlQAVLisXTYoCgSY6/U3qCEDiA+oZm6aT1Qfdf9HL7MWA1PHUCHKIj/6sTLiqRYKhs5Yz3WYAlaIDkyYj6cZFs3zEIyPb/MNRkeQd2IysFE/epiUi8t6UdJ0JJoj+3qXuCNiyjjZ87EaIrnIboVy9n9RAbZYxqyBHEO2GA/avuZJx16y1xGaJ8FKTFFXQmOuIxlWIK5GIzaSK22y4C22AfriHgaZsBAnIIJhqu+tCiDzliKO3CJtPWA7HQTbOAII+LXAzF5RSQB6qBfBKmUKKEltJjQfAHHiLcdp8EE6gAAAABJRU5ErkJggg==" style="height:18px;width:18px" alt="" data-csiid="18" data-atf="4"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://images.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › documents › FY20...</span></cite></div></div></div><span jscontroller="IX53Tb" jsaction="rcuQ6b:npT2md" style="display:none"></span></a></span><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd YmJh3d iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://images.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › documents › FY20...</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-frm="true" data-isdesktop="true" jsdata="l7Bhpb;_;BY9TUc cECq7c;_;BY9TUg" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQ2esEegQIKBAM"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQh-4GegQIKBAN"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf fS1kJf lVm3ye r025kc hJNv6b Hdw6tb" style="-webkit-line-clamp:2"><span>The corporate responsibility <em>reporting</em> team updates the NCGC, and their feedback combined with executive input, helps to determine <em>annual</em> program direction. In&nbsp;...</span></div><div class="LEwnzc Sqrs4e"><span>59 pages</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>',
        'href_text': '', 'page_summary': '',
        'doc_name': 'httpsimagesnvidiacomaem-damsolutionsdocumentsfy2023-nvidia-corporate-responsibility-report-1pdf',
        'entity_name': 'Nvidia Inc', 'keyphrase': 'annual reports'
    },
    {
        'href': 'https://www.nvidia.com/content/dam/en-us/Solutions/documents/Trucost-Assurance-statement-NVIDIA-FINAL.pdf',
        'result_text': "Assurance statement: AA1000\nnvidia.com\nhttps://www.nvidia.com › Solutions › documents\nPDF\nNVIDIA completes a comprehensive annual review to identify its key stakeholders. Key stakeholder groups are published annually in NVIDIA's annual CSR Report For ...\n3 pages",
        'result_html': '<div class="N54PNb BToiNc cvP2Ce" data-snc="ih6Jnb_FO3Rkb"><div class="kb0PBd cvP2Ce jGGQ5e" data-snf="x5WNvb" data-snhf="0"><div class="yuRUbf" style="white-space:nowrap"><div><span jscontroller="msmzHf" jsaction="rcuQ6b:npT2md;PYDNKe:bLV6Bd;mLt3mc"><a jsname="UWckNb" href="https://www.nvidia.com/content/dam/en-us/Solutions/documents/Trucost-Assurance-statement-NVIDIA-FINAL.pdf" data-jsarwt="1" data-usg="AOvVaw2bA5tIvF1oeUrhECRd4o23" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQFnoECCwQAQ"><br><h3 class="LC20lb MBeuO DKV0Md">Assurance statement: AA1000</h3><div class="notranslate TbwUpd YmJh3d NJjxre iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd Vwoesf" aria-hidden="true"><img class="XNo5Ab" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAACLElEQVR4Ae3WA4wlQRSF4bVt27YRrG3btm3bRrC2bds2xjZ75k9yk9yM7Uq+h1vV1ee9ViVKaKFpCS2hTb6UaAJuRpA+YQmwDT4RZE5od54MG2EJZ3hHegAGlcAM3IQVRkg9BYqgJ47BJUID0NkQl+Apk9/ASrEdc9EUKWV8fmyEe7gCUCyMM/CGGSYiWyDngAETGZNC+qvgXagD8CUxhsMRBvYhk/SlQ3fswnhUQ2tshJ1M9gLFZXxaHIMRogCywUEY8MYEdZwnwkLq3zEKKVTw3DgPHxlXVZ2we2GEJMB1VZwrtVy4K7XXqIXE2IY/aOzn6ngoY82QXurJ8TEkAbaq4gCpDZHvXsjl9xyAHQojMRaqS3KpGjslpP9AIThL8R2SICMspTbEbwBxHofVCbkNicUMFcpL+oM8CeeqjsFSGwoDZsjnN4DihUUSPAW2qB3+QnWMhFlQAVLisXTYoCgSY6/U3qCEDiA+oZm6aT1Qfdf9HL7MWA1PHUCHKIj/6sTLiqRYKhs5Yz3WYAlaIDkyYj6cZFs3zEIyPb/MNRkeQd2IysFE/epiUi8t6UdJ0JJoj+3qXuCNiyjjZ87EaIrnIboVy9n9RAbZYxqyBHEO2GA/avuZJx16y1xGaJ8FKTFFXQmOuIxlWIK5GIzaSK22y4C22AfriHgaZsBAnIIJhqu+tCiDzliKO3CJtPWA7HQTbOAII+LXAzF5RSQB6qBfBKmUKKEltJjQfAHHiLcdp8EE6gAAAABJRU5ErkJggg==" style="height:18px;width:18px" alt="" data-csiid="19" data-atf="4"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://www.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › Solutions › documents</span></cite></div></div></div><span jscontroller="IX53Tb" jsaction="rcuQ6b:npT2md" style="display:none"></span></a></span><div class="B6fmyf byrV5b Mg1HEd"><div class="TbwUpd YmJh3d iUh30 ojE3Fb"><span class="oPfux" style="background:linear-gradient(120.09deg,rgba(255,255,255,0.1) 0.06%, #9aa0a6 102.36%)"></span><span class="H9lube" style="background:linear-gradient(rgba(255,255,255,0.25), rgba(255,255,255,0.25)), #fff padding-box"><div class="eqA2re NjwKYd" style="height:18px;width:18px"></div></span><div class="GTRloc"><span class="VuuXrf">nvidia.com</span><div class="byrV5b"><cite class="qLRx3b tjvcx GvPZzd cHaqb" role="text">https://www.nvidia.com<span class="ylgVCe ob9lvb" role="text"> › Solutions › documents</span></cite><div class="eFM0qc BCF2pd iUh30"><span class="ZGwO7 s4H5Cf C0kchf NaCKVc yUTMj VDgVie">PDF</span></div></div></div></div><div class="csDOgf BCF2pd L48a4c"><div jscontroller="exgaYe" data-bsextraheight="0" data-frm="true" data-isdesktop="true" jsdata="l7Bhpb;_;BY9TUk cECq7c;_;BY9TUo" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQ2esEegQILBAM"><div role="button" tabindex="0" jsaction="RvIhPd" jsname="I3kE2c" class="iTPLzd rNSxBe lUn2nc" style="position:absolute" aria-label="About this result"><span jsname="czHhOd" class="D6lY4c mBswFe"><span jsname="Bil8Ae" class="xTFaxe z1asCe" style="height:18px;line-height:18px;width:18px"><svg focusable="false" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"></path></svg></span></span></div><span jsname="zOVa8" data-ved="2ahUKEwjc7ujYxLaDAxWeEVkFHU35A_EQh-4GegQILBAN"></span></div></div></div></div></div></div><div class="kb0PBd cvP2Ce" data-sncf="1" data-snf="nke7rc"><div class="VwiC3b yXK7lf fS1kJf lVm3ye r025kc hJNv6b Hdw6tb" style="-webkit-line-clamp:2"><span>NVIDIA completes a comprehensive <em>annual</em> review to identify its key stakeholders. Key stakeholder groups are published annually in NVIDIA\'s <em>annual</em> CSR <em>Report</em> For&nbsp;...</span></div><div class="LEwnzc Sqrs4e"><span>3 pages</span></div></div><div class="kb0PBd cvP2Ce" data-sncf="2" data-snf="mCCBcf"><div class="fG8Fp uo4vr"></div></div></div>',
        'href_text': '', 'page_summary': '',
        'doc_name': 'httpswwwnvidiacomcontentdamen-ussolutionsdocumentstrucost-assurance-statement-nvidia-finalpdf',
        'entity_name': 'Nvidia Inc', 'keyphrase': 'annual reports'
    }
]
"""

# ## Select a document to process
doc_name = 'httpss201q4cdncom141608511filesdoc_downloads2021042021-annual-reviewpdf'

# ## Extract data from a document
text_extraction_payload = bg.create_api_payload(
    func='extract_text_pipeline',
    args={
        'doc_name': doc_name,
    },
    task_mode='async',
)
text_extraction_resp = bg.call_api(
    payload=text_extraction_payload,
)
"""
<p>
Notice that we have set the `task_mode='async'` for text extraction. 
Because text extraction will take some time to run, `async` execution model 
will allow the run the task in the background, to avoid the risk of task breaking 
if our API call times out.
</p>
"""

# ## Check available output for the document
"""
<p>
We can check the status of available output by listing files of interest. 
For example, `extract_text_pipeline` will generate a number of files including 
- page images of the original file;
- OCR output files from page images;
- Extracted text segment files;
- Extracted table files;
- Page data files, which combine all the text and tables extracted from a page in a single JSON object; 
</p>
"""

# ### Page image files
"""
<p>
Since, page image files will be generated first, let's check if these files are available
</p>
"""
list_images_payload = bg.create_api_payload(
    func='list_files',
    args={
        'doc_name': doc_name,
        'file_pattern': 'data_type=unstructured/**/variable_desc=page-img/**.png',
    },
    task_mode='sync',
)
list_images_resp = bg.call_api(
    payload=text_extraction_payload,
)

# ### Page data files
"""
<p>
Since page data files will be generated at the end, let's check if these files are available. 
If so, that means that `extract_text_pipeline` output is complete, and we can move on to downstream tasks. 
</p>
"""
list_page_data_payload = bg.create_api_payload(
    func='list_files',
    args={
        'doc_name': doc_name,
        'file_pattern': 'data_type=semi-structured/**/variable_desc=page-data/**.pickle',
    },
    task_mode='sync',
)
list_page_data_resp = bg.call_api(
    payload=text_extraction_payload,
)

# ## Financial data structuring
"""
<p>
Now that the base data extraction from the annual report is complete, we can move on to downstream tasks. 
We will start by identifying financial statement pages within the annual report, 
and structuring the reported metrics to a more useful format. 
</p>
"""

# ### Identify financial statement pages

identify_statement_pages_payload = bg.create_api_payload(
    func='identify_financial_statement_pages',
    args={
        'doc_name': doc_name,
    },
    task_mode='sync',
)
identify_statement_pages_resp = bg.call_api(
    payload=identify_statement_pages_payload,
)
statement_pages = identify_statement_pages_resp.get_output()
"""
The statement pages found are, `statement_pages`

"""

# ### Structure reported metrics
structure_metrics_payload = bg.create_api_payload(
    func='structure_reported_financial_metrics',
    args={
        'doc_name': doc_name,
    },
    task_mode='sync',
)
structure_metrics_resp = bg.call_api(
    payload=structure_metrics_payload,
)

# ## Map structured metrics to a financial taxonomy
"""
<p>
Now that we have all reported metrics extracted from a few pages, we can map them onto a financial taxonomy, 
to store this data in a more standardised manner, which will make it considerably easier to perform any downstream 
analytics, or even to query relevant data. 
</p>
"""
taxonomise_data_payload = bg.create_api_payload(
    func='taxonomise_data',
    args={
        'doc_name': doc_name,
    },
    task_mode='sync',
)
taxonomise_data_resp = bg.call_api(
    payload=taxonomise_data_payload,
)