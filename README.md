# ByteGenie Examples
Usage examples for byte-genie API.

Examples are categorised by use-cases, including company research, question-answering on data, model training/fine-tuning, etc.

To run these examples, you will need a ByteGenie API key. Contact info@esgnie.com to get your API key.

Once you have an API key, save it in a secrets.json file inside the code repo, along with your username. Some examples may require an OpenAI API key as well, which can also be set in secrets.json. Here is an example of secrets.json 

{
    "username": "...",
    "BYTE_GENIE_KEY": "...",
    "OPENAI_KEY": "..."
}

Once secrets.json is setup, you can run any of the python scripts or jupyter notebooks to get started.

The code repo includes examples for:
* Finding an organisation's home page given its name;
* Searching and downloading documents (pdf files) related to a keyphrase on the organisation's homepage;
* Processing documents to extract and structure all qualitative and quantitative information from documents;
* Ranking structured data extracted from documents by relevance to input keyphrases;
* Creating custom datasets out of ranked data;
* Standardising data;
* Generating meta-data for a given data set;
* Filtering out relevant data based on an input natural laguage query;
* Answering questions based on the extracted tabular data with or without LangChain. 

Stay tuned for many more examples to come!

**Last updated: 2023-08-23**