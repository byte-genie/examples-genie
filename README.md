# ByteGenie Examples
Usage examples for byte-genie API.

Examples are categorised by use-cases, including company research, question-answering on data, model training/fine-tuning, etc.

## Generate API Key
To run these examples, you will need a ByteGenie API key. To generate an API key, sing up on [ESGnie](https://app.esgnie.com/home), and verify your account, by clicking on the activation URL sent via email. The activation email will contain an API key, which can be used once the account is verified.

In case of any issues with generating an API key, contact us at info@esgnie.org 

## Set up credentials
Once you have an API key, save it in a secrets.json file inside the code repo, along with your username. Some examples may require an OpenAI API key as well, which can also be set in secrets.json. Here is an example of secrets.json 

{
    "username": "...",
    "BYTE_GENIE_KEY": "...",
    "OPENAI_KEY": "..."
}

Once secrets.json is setup, you can run any of the python scripts or jupyter notebooks to get started.

## Code structure
The code repo includes examples for different tasks and use-cases.

### web_search
This folder contains examples for:
* Finding an organisation's home page given its name;
* Searching and downloading documents (pdf files) related to a keyphrase on the organisation's homepage.
### company_research
This folder contains examples for
* Downloading company disclosures from company's homepage;
* Processing documents to extract and structure all qualitative and quantitative information from documents;
* Ranking structured data extracted from documents by relevance to input keyphrases;
* Creating custom datasets out of ranked data;
* Standardising data.
### document_processing
This folder contains examples related to PDF document processing, including:
* short-form document, such as invoices, utility bills, processing;
* Long-form document, such as company annual and sustainability reports, processing.
### data_management
This folder contains examples related to data management, including:
* Uploading local files;
* Accessing data extracted from processed documents.
### data_qa
This folder contains examples for
* Generating meta-data for a given data set;
* Filtering out relevant data based on an input natural laguage query;
* Answering questions based on the extracted tabular data with or without LangChain. 

Stay tuned for many more examples to come!

**Last updated: 2023-09-11**