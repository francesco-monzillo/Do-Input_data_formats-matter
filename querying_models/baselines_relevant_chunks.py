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

CSV_HEADER = "['KG id', 'KG name', 'Accessibility_Availability_Sparql endpoint', 'Representational_Versatility_SPARQL endpoint URL', 'Accessibility_Availability_Availability of RDF dump (metadata)', 'Accessibility_Availability_Availability of RDF dump (query)', 'Representational_Versatility_URL for download the dataset', 'Dataset dynamicity_Currency_Age of data', 'Dataset dynamicity_Currency_Modification date', 'Dataset dynamicity_Currency_Time elapsed since last modification', 'Representational_Versatility_Languages (query)', 'Representational_Versatility_Languages (metadata)', 'Representational_Versatility_Serialization formats', 'Accessibility_Availability_Availability for download (query)', 'Accessibility_Availability_Availability for download (metadata)', 'Accessibility_Security_Use HTTPS', 'Accessibility_Security_Requires authentication', 'Representational_Representational conciseness_Average length of URIs (subject)', 'Representational_Representational conciseness_Average length of URIs (predicate)', 'Representational_Representational conciseness_Average length of URIs (object)', 'Representational_Representational conciseness_Use RDF structures', 'Accessibility_Licensing_License machine redeable (metadata)', 'Accessibility_Licensing_License machine redeable (query)', 'Accessibility_Licensing_License human redeable', 'Accessibility_Performance_Median latency', 'Accessibility_Performance_Median throughput', 'Contextual_Amount of data_Number of triples (metadata)', 'Contextual_Amount of data_Number of triples (query)', 'Contextual_Amount of data_Number of entities', 'Contextual_Amount of data_Number of entities counted with regex', 'Contextual_Amount of data_Number of property', 'Dataset dynamicity_Timeliness_Dataset update frequency', 'Accessibility_Interlinking_Degree of connection', 'Accessibility_Interlinking_Clustering coefficient', 'Accessibility_Interlinking_Centrality', 'Accessibility_Interlinking_Number of samAs chains', 'Trust_Reputation_PageRank', 'Trust_Believability_Description', 'Trust_Believability_Dataset URL', 'Trust_Believability_Is on a trusted provider list', 'Trust_Believability_Trust value', 'Representational_Understandability_Vocabularies', 'Trust_Verifiability_Author (query)', 'Trust_Verifiability_Author (metadata)', 'Trust_Verifiability_Contributor', 'Trust_Verifiability_Publisher', 'Trust_Verifiability_Sources', 'Trust_Verifiability_Signed', 'Contextual_Completeness_Interlinking completeness', 'Representational_Interoperability_New vocabularies defined in the dataset', 'Representational_Interoperability_New terms defined in the dataset', 'Representational_Understandability_Number of labels/comments present on the data', 'Representational_Understandability_Regex uri', 'Representational_Understandability_Presence of example', 'Representational_Interpretability_Number of blank nodes', 'Intrinsic_Consistency_Deprecated classes/properties used', 'Intrinsic_Consistency_Entities as member of disjoint class', 'Intrinsic_Consistency_Triples with misplaced property problem', 'Intrinsic_Consistency_Triples with misplaced class problem', 'Intrinsic_Consistency_Ontology Hijacking problem', 'Intrinsic_Consistency_Undefined class used without declaration', 'Intrinsic_Consistency_Undefined properties used without declaration', 'Intrinsic_Conciseness_Extensional conciseness', 'Intrinsic_Conciseness_Intensional conciseness', 'Intrinsic_Accuracy_Triples with empty annotation problem', 'Intrinsic_Accuracy_Triples with white space in annotation(at the beginning or at the end)', 'Intrinsic_Accuracy_Triples with malformed data type literals problem', 'Intrinsic_Accuracy_Functional properties with inconsistent values', 'Intrinsic_Accuracy_Invalid usage of inverse-functional properties', 'Unknown_Unknown_Score', 'Unknown_Unknown_Normalized score', 'Unknown_Accessibility_URIs Deferenceability', 'Accessibility_Availability_Availability score', 'Accessibility_Licensing_Licensing score', 'Accessibility_Interlinking_Interlinking score', 'Accessibility_Performance_Performance score', 'Intrinsic_Accuracy_Accuracy score', 'Intrinsic_Consistency_Consistency score', 'Intrinsic_Conciseness_Conciseness score', 'Trust_Verifiability_Verifiability score', 'Trust_Reputation_Reputation score', 'Trust_Believability_Believability score', 'Dataset dynamicity_Currency_Currency score', 'Dataset dynamicity_Timeliness_Timeliness score', 'Contextual_Completeness_Completeness score', 'Contextual_Amount of data_Amount of data score', 'Representational_Interoperability_Interoperability score', 'Unknown_Unknown_Representational-Conciseness score', 'Representational_Understandability_Understandability score', 'Representational_Interpretability_Interpretability score', 'Representational_Versatility_Versatility score', 'Accessibility_Security_Security score', 'Accessibility_Interlinking_SKOS mapping properties', 'Representational_Versatility_metadata-media-type', 'Representational_Versatility_Availability of a common accepted Media Type', 'Unknown_FAIR_F1-M Unique and persistent ID', 'Unknown_FAIR_F1-D URIs dereferenceability', 'Unknown_FAIR_F2a-M - Metadata availability via standard primary sources', 'Unknown_FAIR_F2b-M Metadata availability for all the attributes covered in the FAIR score computation', 'Unknown_FAIR_F3-M Data referrable via a DOI', 'Unknown_FAIR_F4-M Metadata registered in a searchable engine', 'Unknown_FAIR_F score', 'Unknown_FAIR_A1-D Working access point(s)', 'Unknown_FAIR_A1-M Metadata availability via working primary sources', 'Unknown_FAIR_A1.2 Authentication & HTTPS support', 'Unknown_FAIR_A2-M Registered in search engines', 'Unknown_FAIR_A score', 'Unknown_FAIR_R1.1 Machine- or human-readable license retrievable via any primary source', 'Unknown_FAIR_R1.2 Publisher information such as authors-contributors-publishers and sources', 'Unknown_FAIR_R1.3-D Data organized in a standardized way', 'Unknown_FAIR_R1.3-M Metadata are described with VoID/DCAT predicates', 'Unknown_FAIR_R score', 'Unknown_FAIR_I1-D Standard & open representation format', 'Unknown_FAIR_I1-M Metadata are described with VoID/DCAT predicates', 'Unknown_FAIR_I2 Use of FAIR vocabularies', 'Unknown_FAIR_I3-D Degree of connection', 'Unknown_FAIR_I score', 'Unknown_FAIR_FAIR score', 'Analysis date']"
KG_PREFIX = "@prefix dct: <http://purl.org/dc/terms/> . @prefix dqv: <http://www.w3.org/ns/dqv#> . @prefix ex: <http://example.org/vocab/> . @prefix prov: <http://www.w3.org/ns/prov#> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix rml: <http://w3id.org/rml/> . @prefix void: <http://rdfs.org/ns/void#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> ."

import nltk
nltk.download('punkt_tab')

embedding_model = genai_embeddings.GoogleGenerativeAIEmbeddings(model="text-embedding-004")

def safe_filename(s: str) -> str:
    # Replace illegal or unsafe characters with underscores
    return re.sub(r'[^a-zA-Z0-9._-]', '_', s)

documentation_path = "documentation_and_hierarchy"

representations_list = ["../baselines_representations/"+ documentation_path +"/csv_repr.csv", "../baselines_representations/"+ documentation_path +"/kg_repr.ttl", "../baselines_representations/"+ documentation_path +"/metadata_repr.md"]


chunks_for_each_repr = [_ for _ in range(len(representations_list))]

for i, representation_path in enumerate(representations_list):

    chunks = None


    if(i == 0):
        chunks = chunking.create_chunks_from_csv_representation(representation_path, "../baselines_representations/"+ documentation_path +"/metrics_doc.json")
    elif(i == 1):
        chunks = chunking.create_chunks_from_kg_representation(representation_path)
    elif(i == 2):
        chunks = chunking.create_chunks_from_metadata_representation(representation_path, "../baselines_representations/"+ documentation_path +"/verbalized_doc.md")


    # Remove new lines from each chunk
    for j, chunk in enumerate(chunks):
        chunks[j] = chunk.replace("\n", " ").strip().replace("\t","")


    chunks_for_each_repr[i] = chunks
    




vectorstore_wrappers = [[] for _ in range(len(representations_list))]

relevant_chunks = [_ for _ in range(len(representations_list))]

sub_folder = "better_embeddings"

# Save vector stores locally
for i, wrapper in enumerate(vectorstore_wrappers):

    if (sub_folder != ""):
        vectorstore_wrappers[i] = chunking.buildVectorStoreObject(chunks_for_each_repr[i], embeddings= "GEMINI", maximum_tokens_for_embedding_request = 30000, Structure = i, CSV = True if i == 0 else False, KG = True if i == 1 else False, Metadata = True if i == 2 else False)
    else:
        vectorstore_wrappers[i] = chunking.buildVectorStoreObject(chunks_for_each_repr[i], embeddings= "GEMINI")
    #for j, vectorstore in enumerate(vectorstore_wrappers[i]):
    if (sub_folder != ""):
        vectorstore_wrappers[i].save_local(f"./baselines_vectorstores/{documentation_path}/{sub_folder}/vectorstore_{i}/part_1")
    else:
        vectorstore_wrappers[i].save_local(f"./baselines_vectorstores/{documentation_path}/weakened_embeddings/vectorstore_{i}/part_1")


# Load vector stores from local directory
for i, wrapper in enumerate(vectorstore_wrappers):

    repr_parent = None

    if (sub_folder != ""):
        repr_parent = f"./baselines_vectorstores/{documentation_path}/{sub_folder}/vectorstore_{i}"
    else:
        repr_parent = f"./baselines_vectorstores/{documentation_path}/weakened_embeddings/vectorstore_{i}"
    
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

                for i, representation in enumerate(representations_list):
                    good_ending = False

                    while not good_ending:
                        try:

                            to_open_directory = ""
                            internal_folder = ""

                            if i == 0:
                                internal_folder = "CSV"
                            elif i == 1:
                                internal_folder = "KG"
                            elif i == 2:
                                internal_folder = "Metadata"
                            
                            number_of_relevant_chunks = 10
                            relevant_chunks[i] = chunking.getRelevantChunks(vectorstore_wrappers[i], request, k = number_of_relevant_chunks)

                            maximum_tokens = None

                            chunks_addition = 1

                            if i == 0 or i == 1:
                                maximum_tokens = 75000
                                chunks_addition = 5
                            else:
                                maximum_tokens = 85000
                                chunks_addition = 1

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
                                to_open_directory = f"./RELEVANT_CHUNKS_FOR_EACH_QUESTION/baselines/{documentation_path}/{sub_folder}/{internal_folder}/relevant_chunks_{i}_question_{sentences[0][0:len(sentences[0]) - 1]}_request_{request_string}.txt"
                            else:
                                to_open_directory = f"./RELEVANT_CHUNKS_FOR_EACH_QUESTION/baselines/{documentation_path}/weakened_embeddings/{internal_folder}/relevant_chunks_{i}_question_{sentences[0][0:len(sentences[0]) - 1]}_request_{request_string}.txt"

                            with open(to_open_directory, "w", encoding='utf-8') as rc_f:

                                if i == 0:
                                    rc_f.write(f"{CSV_HEADER}")

                                
                                if i == 1:
                                    rc_f.write(f"{KG_PREFIX} ")


                                for chunk in relevant_chunks[i]:
                                    rc_f.write(f"{chunk}")

                            good_ending = True

                        except Exception as e:
                            print("timeout...Retry")
                            print(e)

                            time.sleep(20)
            