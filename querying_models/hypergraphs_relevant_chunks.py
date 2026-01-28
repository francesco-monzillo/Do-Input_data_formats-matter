import ast
import csv
from google import genai
import os
import json
import time
import nltk
import sys
from google.genai import types
#nltk.download('punkt_tab')
import tiktoken
from nltk.tokenize import sent_tokenize
import chunking
import re
import numpy as np
import langchain_google_genai.embeddings as genai_embeddings

import nltk
nltk.download('punkt_tab')

embedding_model = genai_embeddings.GoogleGenerativeAIEmbeddings(model="text-embedding-004")

def safe_filename(s: str) -> str:
    # Replace illegal or unsafe characters with underscores
    return re.sub(r'[^a-zA-Z0-9._-]', '_', s)

documentation_path = "documentation_and_hierarchy"

hypergraph_representation_list = [None, None, None, None, None]


# Open and read the file
with open("../generated_hypergraphs_json/"+ documentation_path +"/test_4_uniform_hypergraph.json", "r") as f:
    hypergraph = json.load(f)
    hypergraph_representation_list[0] = json.dumps(hypergraph, separators=(',', ':'))
    hypergraph_representation_list[0] = hypergraph

with open("../generated_hypergraphs_json/"+ documentation_path +"/test_time_labeled_hypergraph.json", "r") as f:
    hypergraph = json.load(f)
    hypergraph_representation_list[1] = json.dumps(hypergraph, separators=(',', ':'))
    hypergraph_representation_list[1] = hypergraph

with open("../generated_hypergraphs_json/"+ documentation_path +"/test_measure_labeled_hypergraph.json", "r") as f:
    hypergraph = json.load(f)
    hypergraph_representation_list[2] = json.dumps(hypergraph, separators=(',', ':'))
    hypergraph_representation_list[2] = hypergraph

with open("../generated_hypergraphs_json/"+ documentation_path +"/test_kg_labeled_hypergraph.json", "r") as f:
    hypergraph = json.load(f)
    hypergraph_representation_list[3] = json.dumps(hypergraph, separators=(',', ':'))
    hypergraph_representation_list[3] = hypergraph

with open("../generated_hypergraphs_json/" + documentation_path + "/test_value_labeled_hypergraph.json", "r") as f:
    hypergraph = json.load(f)
    hypergraph_representation_list[4] = hypergraph


chunks_for_each_repr = [_ for _ in range(len(hypergraph_representation_list))]


for i, representation_path in enumerate(hypergraph_representation_list):

    nolabel = False

    # Because normal hypergraph does not have univocal identifiers for edges (first presented in this list)
    if(i == 0):
         nolabel = True


    chunks = chunking.create_chunks_from_edges_and_nodes(representation_path["nodes_contained_for_each_edge"], representation_path["node-data"], nolabel = True if i == 0 else False)

    chunks_for_each_repr[i] = chunks
    




vectorstore_wrappers = [[] for _ in range(len(hypergraph_representation_list))]

relevant_chunks = [_ for _ in range(len(hypergraph_representation_list))]

sub_folder = "better_embeddings"

# Save vector stores locally
# for i, wrapper in enumerate(vectorstore_wrappers):

#     if (sub_folder != ""):
#         vectorstore_wrappers[i] = chunking.buildVectorStoreObject(chunks_for_each_repr[i], embeddings= "GEMINI", maximum_tokens_for_embedding_request = 30000, Structure = i, CSV = False, KG = False, Metadata = False)
#     else:
#         vectorstore_wrappers[i] = chunking.buildVectorStoreObject(chunks_for_each_repr[i], embeddings= "GEMINI")
#     #for j, vectorstore in enumerate(vectorstore_wrappers[i]):
#     if (sub_folder != ""):
#         vectorstore_wrappers[i].save_local(f"./vectorstores/{documentation_path}/{sub_folder}/vectorstore_{i}/part_1")
#     else:
#         vectorstore_wrappers[i].save_local(f"./vectorstores/{documentation_path}/weakened_embeddings/vectorstore_{i}/part_1")


# Load vector stores from local directory
for i, wrapper in enumerate(vectorstore_wrappers):

    repr_parent = None

    if (sub_folder != ""):
        repr_parent = f"./vectorstores/{documentation_path}/{sub_folder}/vectorstore_{i}"
    else:
        repr_parent = f"./vectorstores/{documentation_path}/weakened_embeddings/vectorstore_{i}"
    
    for name in os.listdir(repr_parent):
        path = os.path.join(repr_parent, name)

        if os.path.isdir(path):
            print("Folder:", path)
            vectorstore_wrappers[i] = chunking.returnLocalVectorStoreObject(f"{path}")



# Compare each chunk embedding with the question embedding through cosine similarity. Then, save the top k relevant chunks to include in the prompt (until a certain token limit is reached).
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

            requests = [sentences[1], sentences[2]]

            for request in requests:

                request_string = ""
                            
                if request == sentences[1]:
                    request_string = 1
                else:
                    request_string = 2
    
                print("Request: " + request)

                for i, representation in enumerate(hypergraph_representation_list):
                    good_ending = False

                    while not good_ending:
                        try:

                            to_open_directory = ""
                            internal_folder = ""


                            if i == 0:
                                internal_folder = "4_uniform"
                            elif i == 1:
                                internal_folder = "Time-labeled"
                            elif i == 2:
                                internal_folder = "Measure-labeled"
                            elif i == 3:
                                internal_folder = "KG-labeled"
                            elif i == 4:
                                internal_folder = "Value-labeled"
                            
                            number_of_relevant_chunks = 300
                            relevant_chunks[i] = chunking.getRelevantChunks(vectorstore_wrappers[i], request, k = number_of_relevant_chunks)

                            maximum_tokens = 83000

                            chunks_addition = 50

                            stop = False

                            while not stop:

                                number_of_relevant_chunks += chunks_addition

                                if(chunking.count_tokens_in_chunks(relevant_chunks[i]) < maximum_tokens):

                                    futureRelevantChunks = chunking.getRelevantChunks(vectorstore_wrappers[i], request, k = number_of_relevant_chunks)

                                    if(chunking.count_tokens_in_chunks(futureRelevantChunks) >= maximum_tokens):
                                        stop = True
                                    else:
                                        relevant_chunks[i] = futureRelevantChunks
                                else:
                                    stop = True
                                
                            if(sub_folder != ""):
                                to_open_directory = f"./RELEVANT_CHUNKS_FOR_EACH_QUESTION/{documentation_path}/{sub_folder}/{internal_folder}/relevant_chunks_{i}_question_{sentences[0][0:len(sentences[0]) - 1]}_request_{request_string}.txt"
                            else:
                                to_open_directory = f"./RELEVANT_CHUNKS_FOR_EACH_QUESTION/{documentation_path}/weakened_embeddings/{internal_folder}/relevant_chunks_{i}_question_{sentences[0][0:len(sentences[0]) - 1]}_request_{request_string}.txt"


                            with open(to_open_directory, "w", encoding='utf-8') as rc_f:

                                for chunk in relevant_chunks[i]:
                                    rc_f.write(f"{chunk}")

                            good_ending = True

                        except Exception as e:
                            print("timeout...Retry")
                            print(e)

                            time.sleep(20)
            