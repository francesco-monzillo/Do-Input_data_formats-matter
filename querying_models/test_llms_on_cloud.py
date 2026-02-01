from platform import processor
from openai import OpenAI
import os
import time
#import chunking
from nltk.tokenize import sent_tokenize
import json
from langchain_community.vectorstores import FAISS
import torch
import sys
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig, AutoConfig
from litai import LLM

import nltk
nltk.download('punkt_tab')

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

client = OpenAI(
    #base_url="http://localhost:8000/v1",
    #base_url = "https://lightning.ai/api/v1/",
    #api_key = os.environ["Lightning_AI_LLMS_API_KEY"]+"/namespaceName/name_of_project"  # vLLM requires a key but doesn't validate it by default
)

# llm_client = LLM(model="openai/gpt-5-mini", api_key="f7***c60")

# Llama 3.2 1B Instruct GGUF model
#llama_client = lms.llm("qwen3_vl_2b", config={"contextLength": 130000, "seed": 42})
def returnLocalVectorStoreObject(file_path, embedding_model = "GEMINI"):
    vectorstore_wrapper = []

    if not os.path.exists(file_path):
        print(f"Error: The specified vectorstore file {file_path} does not exist.")
        sys.exit(1)

    embedding_model = None

    if embedding_model == "OPENAI":
        from langchain_openai import OpenAIEmbeddings
        embedding_model = OpenAIEmbeddings(model="text-embedding-3-small")
    else:
        import langchain_google_genai.embeddings as genai_embeddings
        embedding_model = genai_embeddings.GoogleGenerativeAIEmbeddings(model="text-embedding-004")

    vectorstore_wrapper = FAISS.load_local(file_path, embedding_model, allow_dangerous_deserialization = True)

    return vectorstore_wrapper


representations_list = [None, None, None, None, None, None, None, None]

documentation_path = "documentation_and_hierarchy"

sub_folder = "better_embeddings"

with open("./questions/questions.tsv", "r") as f:
    text_file = f.read()

    for i, line in enumerate(text_file.split("\n")):

        if(i == 0):
            continue

        if line.strip():  # Ensure the line is not empty
        
            if(line[0] == "#"):
                continue

            sentences = sent_tokenize(line)

            print(sentences[0])

            requests_for_llm = [sentences[1], sentences[2]]

            for request in requests_for_llm:

                request_string = ""

                if request == sentences[1]:
                    request_string = 1
                else:
                    request_string = 2
    
                print("Request: " + request)

                for i, hypergraph_representation in enumerate(representations_list):
                    good_ending = False

                    while not good_ending:
                        try:
                            
                            to_open_directory = ""
                            internal_folder = ""

                            if i == 0:
                                internal_folder = "Normal"
                            elif i == 1:
                                internal_folder = "Time-labeled"
                            elif i == 2:
                                internal_folder = "Measure-labeled"
                            elif i == 3:
                                internal_folder = "KG-labeled"
                            elif i == 4:
                                internal_folder = "Measure-value-labeled"
                            elif i == 5:
                                internal_folder = "CSV"
                            elif i == 6:
                                internal_folder = "KG"
                            elif i == 7:
                                internal_folder = "Metadata"

                            sub_directory_in_relevant_chunks = ""

                            if i >= 5 and i <=7:
                                sub_directory_in_relevant_chunks = "baselines/"


                            relevant_chunks_file_path = f"./RELEVANT_CHUNKS_FOR_EACH_QUESTION/{sub_directory_in_relevant_chunks}{documentation_path}/{sub_folder}/{internal_folder}/relevant_chunks_{i}_question_{sentences[0][0:len(sentences[0]) - 1]}_request_{request_string}.txt"

                            relevant_chunks = None
                            
                            with open(relevant_chunks_file_path, "r", encoding="utf-8") as rel_chunks_file:
                                relevant_chunks = rel_chunks_file.read()

                            response = client.chat.completions.create(
                                model="your_favourite_model",
                                messages=[
                                    {
                                        "role": "user", 
                                        "content": relevant_chunks + "\n" + request,
                                    }
                                ],
                                temperature=0.0, # Low temperature is better for factual QA
                                top_p=1
                            )

                            final_response = response.choices[0].message.content

                            print("Response:\n" + final_response)

                            response_path = f"./responses/your_model_name/{sub_directory_in_relevant_chunks}with_{documentation_path}/{internal_folder}_hg_responses.txt"

                            with open(response_path, "a", encoding="utf-8") as response_file:
                                response_file.write(f"Question: {sentences[0]}\nRequest: {request}\nExpected Response: {sentences[3]}\nResponse:\n{final_response}\n\n")
                                response_file.write("Vote:\n--------------------------------------------------\n")
                            
                            good_ending = True

                        except Exception as e:
                            print("timeout...Retry")
                            print(e)

                            time.sleep(20)