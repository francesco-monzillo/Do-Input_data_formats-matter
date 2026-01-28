import ast
import csv
import json
import os
import time
from google import genai
from langchain_community.vectorstores import FAISS
import sys
import faiss
import uuid
from sympy import re
import tiktoken
import numpy as np
import re

CSV_HEADER = "['KG id', 'KG name', 'Accessibility_Availability_Sparql endpoint', 'Representational_Versatility_SPARQL endpoint URL', 'Accessibility_Availability_Availability of RDF dump (metadata)', 'Accessibility_Availability_Availability of RDF dump (query)', 'Representational_Versatility_URL for download the dataset', 'Dataset dynamicity_Currency_Age of data', 'Dataset dynamicity_Currency_Modification date', 'Dataset dynamicity_Currency_Time elapsed since last modification', 'Representational_Versatility_Languages (query)', 'Representational_Versatility_Languages (metadata)', 'Representational_Versatility_Serialization formats', 'Accessibility_Availability_Availability for download (query)', 'Accessibility_Availability_Availability for download (metadata)', 'Accessibility_Security_Use HTTPS', 'Accessibility_Security_Requires authentication', 'Representational_Representational conciseness_Average length of URIs (subject)', 'Representational_Representational conciseness_Average length of URIs (predicate)', 'Representational_Representational conciseness_Average length of URIs (object)', 'Representational_Representational conciseness_Use RDF structures', 'Accessibility_Licensing_License machine redeable (metadata)', 'Accessibility_Licensing_License machine redeable (query)', 'Accessibility_Licensing_License human redeable', 'Accessibility_Performance_Median latency', 'Accessibility_Performance_Median throughput', 'Contextual_Amount of data_Number of triples (metadata)', 'Contextual_Amount of data_Number of triples (query)', 'Contextual_Amount of data_Number of entities', 'Contextual_Amount of data_Number of entities counted with regex', 'Contextual_Amount of data_Number of property', 'Dataset dynamicity_Timeliness_Dataset update frequency', 'Accessibility_Interlinking_Degree of connection', 'Accessibility_Interlinking_Clustering coefficient', 'Accessibility_Interlinking_Centrality', 'Accessibility_Interlinking_Number of samAs chains', 'Trust_Reputation_PageRank', 'Trust_Believability_Description', 'Trust_Believability_Dataset URL', 'Trust_Believability_Is on a trusted provider list', 'Trust_Believability_Trust value', 'Representational_Understandability_Vocabularies', 'Trust_Verifiability_Author (query)', 'Trust_Verifiability_Author (metadata)', 'Trust_Verifiability_Contributor', 'Trust_Verifiability_Publisher', 'Trust_Verifiability_Sources', 'Trust_Verifiability_Signed', 'Contextual_Completeness_Interlinking completeness', 'Representational_Interoperability_New vocabularies defined in the dataset', 'Representational_Interoperability_New terms defined in the dataset', 'Representational_Understandability_Number of labels/comments present on the data', 'Representational_Understandability_Regex uri', 'Representational_Understandability_Presence of example', 'Representational_Interpretability_Number of blank nodes', 'Intrinsic_Consistency_Deprecated classes/properties used', 'Intrinsic_Consistency_Entities as member of disjoint class', 'Intrinsic_Consistency_Triples with misplaced property problem', 'Intrinsic_Consistency_Triples with misplaced class problem', 'Intrinsic_Consistency_Ontology Hijacking problem', 'Intrinsic_Consistency_Undefined class used without declaration', 'Intrinsic_Consistency_Undefined properties used without declaration', 'Intrinsic_Conciseness_Extensional conciseness', 'Intrinsic_Conciseness_Intensional conciseness', 'Intrinsic_Accuracy_Triples with empty annotation problem', 'Intrinsic_Accuracy_Triples with white space in annotation(at the beginning or at the end)', 'Intrinsic_Accuracy_Triples with malformed data type literals problem', 'Intrinsic_Accuracy_Functional properties with inconsistent values', 'Intrinsic_Accuracy_Invalid usage of inverse-functional properties', 'Unknown_Unknown_Score', 'Unknown_Unknown_Normalized score', 'Unknown_Accessibility_URIs Deferenceability', 'Accessibility_Availability_Availability score', 'Accessibility_Licensing_Licensing score', 'Accessibility_Interlinking_Interlinking score', 'Accessibility_Performance_Performance score', 'Intrinsic_Accuracy_Accuracy score', 'Intrinsic_Consistency_Consistency score', 'Intrinsic_Conciseness_Conciseness score', 'Trust_Verifiability_Verifiability score', 'Trust_Reputation_Reputation score', 'Trust_Believability_Believability score', 'Dataset dynamicity_Currency_Currency score', 'Dataset dynamicity_Timeliness_Timeliness score', 'Contextual_Completeness_Completeness score', 'Contextual_Amount of data_Amount of data score', 'Representational_Interoperability_Interoperability score', 'Unknown_Unknown_Representational-Conciseness score', 'Representational_Understandability_Understandability score', 'Representational_Interpretability_Interpretability score', 'Representational_Versatility_Versatility score', 'Accessibility_Security_Security score', 'Accessibility_Interlinking_SKOS mapping properties', 'Representational_Versatility_metadata-media-type', 'Representational_Versatility_Availability of a common accepted Media Type', 'Unknown_FAIR_F1-M Unique and persistent ID', 'Unknown_FAIR_F1-D URIs dereferenceability', 'Unknown_FAIR_F2a-M - Metadata availability via standard primary sources', 'Unknown_FAIR_F2b-M Metadata availability for all the attributes covered in the FAIR score computation', 'Unknown_FAIR_F3-M Data referrable via a DOI', 'Unknown_FAIR_F4-M Metadata registered in a searchable engine', 'Unknown_FAIR_F score', 'Unknown_FAIR_A1-D Working access point(s)', 'Unknown_FAIR_A1-M Metadata availability via working primary sources', 'Unknown_FAIR_A1.2 Authentication & HTTPS support', 'Unknown_FAIR_A2-M Registered in search engines', 'Unknown_FAIR_A score', 'Unknown_FAIR_R1.1 Machine- or human-readable license retrievable via any primary source', 'Unknown_FAIR_R1.2 Publisher information such as authors-contributors-publishers and sources', 'Unknown_FAIR_R1.3-D Data organized in a standardized way', 'Unknown_FAIR_R1.3-M Metadata are described with VoID/DCAT predicates', 'Unknown_FAIR_R score', 'Unknown_FAIR_I1-D Standard & open representation format', 'Unknown_FAIR_I1-M Metadata are described with VoID/DCAT predicates', 'Unknown_FAIR_I2 Use of FAIR vocabularies', 'Unknown_FAIR_I3-D Degree of connection', 'Unknown_FAIR_I score', 'Unknown_FAIR_FAIR score', 'Analysis date']".split(", ")


def count_tokens_in_chunks(chunks):
    enc = tiktoken.get_encoding("cl100k_base")  # Similar to Gemini tokenizer
    total_tokens = 0
    for chunk in chunks:
        total_tokens += len(enc.encode(chunk))
    return total_tokens

def create_chunks_from_edges_and_nodes(edge_list, node_dict, nolabel = False):
    chunks = []

    for edge_dict in edge_list:
        for edge_id, nodes in edge_dict.items():
            edge = None
            #Not using univocal identifier for edges in case of normal hypergraph
            if(nolabel):
                edge = nodes
            else:
                edge = {edge_id: nodes}

            chunks.append(json.dumps(edge, separators=(',', ':')))

    for node_id, properties in node_dict.items():
        node = {node_id: properties}
        chunks.append(json.dumps(node, separators=(',', ':')))

    return chunks

def create_chunks_from_csv_representation(csv_representation_path, documentation_path):
    chunks = []

    #read row by row the csv file with the csv extension
    with open(csv_representation_path, mode ="r", encoding='utf-8-sig', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        counter = 0
        for row in reader:

            if(counter == 0):
                counter += 1
                continue

            chunks.append(json.dumps(row, separators=(',', ':')))

            counter += 1

    #Also add the documentation as chunks
    with open(documentation_path, "r", encoding='utf-8') as f:

        metrics_description_dict = json.load(f)

        for metric, dict_of_properties in metrics_description_dict.items():
            chunks.append(json.dumps({metric: dict_of_properties}, separators=(',', ':'), indent=None))

    return chunks


def create_chunks_from_kg_representation(kg_representation_path):
    with open(kg_representation_path, "r", encoding='utf-8') as f:
        text = f.read()

    chunks = []
    remaining_lines = []


    # Extract lines starting with "@"
    # Split the text into individual lines first
    lines = text.splitlines()
    for line in lines:
        if line.strip().startswith("@"): 
            chunks.append(line)
        else:
            # Keep this line for the next pass
            remaining_lines.append(line)
    
    # Reconstruct the text without the @ lines to process blocks next
    cleaned_text = "\n".join(remaining_lines)

    chunks.extend(cleaned_text.split("\n\n"))

    #print(chunks[len(chunks)-1])
    
    return chunks

def create_chunks_from_metadata_representation(metadata_representation_path, metadata_documentation_path):
    with open(metadata_representation_path, "r", encoding='utf-8') as f:
        text = f.read()
    chunks = text.split("toremove ")
    
    with open(metadata_documentation_path, "r", encoding='utf-8') as f:
        documentation_text = f.read()
    
    chunks = chunks + documentation_text.split("toremove ")

    #Removing initial empty chunk
    return chunks


def create_chunks_from_edges(edge_list):
    chunks = []

    for edge_dict in edge_list:
        for edge_id, nodes in edge_dict.items():
            edge = {edge_id: nodes}
            chunks.append(json.dumps(edge, separators=(',', ':')))

    return chunks

def create_chunks_from_nodes(node_dict):
    chunks = []

    for node_id, properties in node_dict.items():
        node = {node_id: properties}
        chunks.append(json.dumps(node, separators=(',', ':')))

    return chunks

def return_chunks_elements(text, Structure = -1, CSV = False, KG = False, METADATA = False):

    if CSV == False and KG == False and METADATA == False and Structure != None:

        try:
            #print(text[:150])

            if Structure == 0:

                text_list = ast.literal_eval(text)

                if(type(text_list) is dict):
                    text_list = json.loads(text)

                    text_dict_keys = list(text_list.keys())

                    text_dict_label = text_dict_keys[0]

                    text_list = text_list[text_dict_label]

                    if("dimension" in text_list):

                        dim_value = text_list["dimension"]

                        #Remove dimension to avoid repetition
                        del text_list["dimension"]

                        if("score" not in text_dict_label):
                            text = {"Metric: " + text_dict_label + " (dimension: " + dim_value + " (score)) in the dataset": text_list}
                        else:
                            text = {"dimension (in the dataset): " + text_dict_label : text_list}

                        print(text)
                    
                        return [str(text)]
                
                return [str(text_list)]
            
            elif Structure == 1:
                text_dict = json.loads(text)
                text_dict_keys = list(text_dict.keys())
                
                text_dict_label = text_dict_keys[0]

                text_list = text_dict[text_dict_label]

                if("dimension" in text_list):

                    dim_value = text_list["dimension"]

                    #Remove dimension to avoid repetition
                    del text_list["dimension"]

                    if("score" not in text_dict_label):
                        text = {"Metric: " + text_dict_label + " (dimension: " + dim_value + " (score)) in the dataset": text_list}
                    else:
                        text = {"dimension (in the dataset): " + text_dict_label : text_list}
                        
                    print(text)
                    return [str(text)]

                if(len(text_list) <= 1 or "description" in text_list or "dimension" in text_list):
                    return [text]

                final_chunk = {text_dict_label: text_list}

                return [str(final_chunk)]

            elif Structure == 2:
                text_dict = json.loads(text)
                text_dict_keys = list(text_dict.keys())
                
                text_dict_label = text_dict_keys[0]

                text_list = text_dict[text_dict_label]

                if("dimension" in text_list):

                    dim_value = text_list["dimension"]

                    #Remove dimension to avoid repetition
                    del text_list["dimension"]

                    if("score" not in text_dict_label):
                        text = {"Metric: " + text_dict_label + " (dimension: " + dim_value + " (score)) in the dataset": text_list}
                    else:
                        text = {"dimension (in the dataset): " + text_dict_label : text_list}

                    print(text)
                    return [str(text)]

                if(len(text_list) <= 1 or "description" in text_list or "dimension" in text_list):
                    return [text]

                final_chunk = {text_dict_label: text_list}

                return [str(final_chunk)]
            
            elif Structure == 3:
                text_dict = json.loads(text)
                text_dict_keys = list(text_dict.keys())
                
                text_dict_label = text_dict_keys[0]

                text_list = text_dict[text_dict_label]

                if("dimension" in text_list):

                    dim_value = text_list["dimension"]

                    #Remove dimension to avoid repetition
                    del text_list["dimension"]

                    if("score" not in text_dict_label):
                        text = {"Metric: " + text_dict_label + " (dimension: " + dim_value + " (score)) in the dataset": text_list}
                    else:
                        text = {"dimension (in the dataset): " + text_dict_label : text_list}

                    print(text)
                    return [str(text)]

                if(len(text_list) <= 1 or "description" in text_list or "dimension" in text_list):
                    return [text]

                final_chunk = {text_dict_label: text_list}

                return [str(final_chunk)]
            
            elif Structure == 4:
                text_dict = json.loads(text)
                text_dict_keys = list(text_dict.keys())
                
                text_dict_label = text_dict_keys[0]

                text_list = text_dict[text_dict_label]

                if("dimension" in text_list):

                    dim_value = text_list["dimension"]

                    #Remove dimension to avoid repetition
                    del text_list["dimension"]

                    if("score" not in text_dict_label):
                        text = {"Metric: " + text_dict_label + " (dimension: " + dim_value + " (score)) in the dataset": text_list}
                    else:
                        text = {"dimension (in the dataset): " + text_dict_label : text_list}

                    print(text)
                    return [str(text)]

                if(len(text_list) <= 1 or "description" in text_list or "dimension" in text_list):
                    return [text]

                final_chunk = {text_dict_label: text_list}

                return [str(final_chunk)]
        except Exception as e:
            #print(f" ERROR while reformatting chunk for structure {Structure}. You can simply ignore this message if the chunks are not structured. Exception: {type(e)}")

            return [text]
        
        if Structure == -1:
            print(" ERROR: No structure specified for chunk elements extraction.")
            return [text]

    elif CSV == True:
        row = ast.literal_eval(text)

        if(type(row) is not list):
            print("WARNING: The provided chunk is not a CSV row representation.")
            return [text]

        for i in range(len(row)):
            # Take off the brackets and append the actual value
            row[i] = CSV_HEADER[i][1:len(CSV_HEADER[i])-1] + ": " + row[i]

        text = str(row)

        return [text]
    
    elif KG == True:
        # Understand why some chunks starting with ## are not retrieved in some questions

        m = re.search(r'(dqv:inDimension <http://example.org/Quality-Dimension/.+>)', text)

        if m:
            value = m.group(1)

            value = value.replace('dqv:inDimension <http://example.org/Quality-Dimension/','').replace('>','')

            text = re.sub(r'(dqv:inDimension <http://example.org/Quality-Dimension/[\s\S]+>)', '', text) #' (dimension/score: '+ value + ')', text)

            text = text.replace("Metric;   ex:InputToCompute", "; dimension: " + value + " (score); ex:InputToCompute")

            text =  "Metric: " + text[0: text.find("; dimension:")-1] + "; dimension: " + value + " (score)."

        m = re.search("a dqv:Dimension;", text)

        if m:
            print("Dimension chunk found.")

            dim_beginning, dim_end = text.find("\""), text.rfind("\"")

            text = text[dim_beginning + 1: dim_end] + " is a dimension in this dataset."



        text = text.replace("a dqv", "").replace('<http://www.w3.org/2004/02/skos/core#prefLabel>', 'description: ').replace("<http://example.org/Quality_Dimension/", "").replace('<http://example.org/dataset/','Dataset: ').replace("<http://example.org/Quality-Metric/", "").replace('<http://example.org/measurement/','Measurement: ').replace('dqv:inCategory', 'Inside Category: ').replace('a dqv:', '').replace('dqv:', '').replace('prov:', '').replace('%20', ' ').replace('<','').replace('>','').replace('%28', '(').replace('%29', ')').replace('http://example.org/Quality-Category/', 'Category: ').replace('ex:','').replace('@en','').replace(";    .",".")


        if(m):
            print(text)

        return [text]
    
    elif METADATA == True:

        if(text.startswith("## Table of Contents") or text.startswith("# Knowledge Graph Quality Assessment") or text.startswith("# Overall Quality Scores") or text.endswith("category")):
            return [text]

        if(text.startswith("# ")):

            text = text.replace("# ", "")

            text = "Dataset: " + text

            end_line_marker = text.find("This is a detailed quality analysis")

            text_before_marker = text[:end_line_marker]

            last_open = text_before_marker.rfind("(")
            last_closed = text_before_marker.rfind(")")

            text = text[:last_open] + "date of analysis: " + text[last_open:last_closed+1]

        elif(text.startswith("## ")):

            # Understand why some chunks starting with ## are not retrieved in some questions

            # Remove leading ##
            text = text[3:]

            dimension = text[0: text.find("###")]

            metrics = text.split("### ")

            print(dimension)

            text = "This is a dimension in this dataset. dimension: " + text[0: len(dimension)].strip() + "(score). Metrics involved: "# + text[len(dimension):]

            # name of dimension string

            # Remove first section, which is not a metric
            metrics = metrics[1:]

            #print(metrics)

            to_add_text = ""

            for i, metric in enumerate(metrics):

                if (i == 0):
                    comma = ""
                else:
                    comma = ", "

                metric = metric[0: metric.find("- **")]
                to_add_text += comma + str(metric)

            
            text = text + to_add_text
            

            #text = text.replace(" - **", " (dimension: " + dimension + ") - **")

            print(text[:150])
         
        return [text]

def add_embedding_for_chunks(embedding_model, chunk_lists, Structure = -1, CSV = False, KG = False, Metadata = False):
    embeddings = []
    array_of_elements = []
    for chunk_list in chunk_lists:
        array_of_elements.append(return_chunks_elements(chunk_list, Structure = Structure, CSV = CSV, KG = KG, METADATA = Metadata))
        

    chunks_embeddings = None
    while chunks_embeddings is None:
        try:
            chunks_embeddings = np.array(embedding_model.embed_documents([element for array in array_of_elements for element in array]))
        except Exception as e:
            print(f"Error while embedding chunks: {e}")
            time.sleep(20)

    # Aggregate the embeddings (e.g., by averaging)
    # Also, converting to float32 as FAISS requires that datatype
    for array in array_of_elements:

        #Retrieve only the embeddings related to the current chunk
        interested_embeddings = chunks_embeddings[:len(array)]
        #Remove the used embeddings from the main array
        chunks_embeddings = chunks_embeddings[len(array):]

        aggregate_embedding = np.mean(interested_embeddings, axis=0).astype(np.float32)

        # Normalize the aggregate embedding
        # when using L2 distance, as it makes it equivalent to cosine similarity.
        faiss.normalize_L2(aggregate_embedding.reshape(1, -1))

        #Reshaping to a matrix with one row and one column for each dimension (FOR FAISS)
        embeddings.append(aggregate_embedding)

    return embeddings


def buildFromEmbeddingsAndDocuments(embeddings, documents, embedding_model = None):

    if embedding_model is None:
        import langchain_google_genai.embeddings as genai_embeddings
        embedding_model = genai_embeddings.GoogleGenerativeAIEmbeddings(model="text-embedding-004")

    return FAISS.from_embeddings([(document, embedding) for document, embedding in zip(documents, embeddings)], embedding_model)


inserted_docs = dict()


def incorporate_texts_in_vectorstore(vectorstore_wrapper, texts, embedding_for_each_document, embedding_model):

    if vectorstore_wrapper is None:
        
        vectorstore_wrapper = FAISS.from_embeddings([(text, embedding) for text, embedding in zip(texts, embedding_for_each_document)], embedding_model)

    else:
        for t in texts:
            if not t or not t.strip():
                continue
        
        vectorstore_wrapper.add_embeddings([(text, embedding) for text, embedding in zip(texts, embedding_for_each_document)])

    return vectorstore_wrapper


def buildVectorStoreObject(chunks, embeddings = "GEMINI", maximum_tokens_for_embedding_request = None, Structure = -1, CSV = False, KG = False, Metadata = False):
    
    vectorstore_wrapper = None

    if embeddings == "OPENAI":
        from langchain_openai import OpenAIEmbeddings
        embedding_model = OpenAIEmbeddings(model="text-embedding-3-small")
        if maximum_tokens_for_embedding_request is None:
            maximum_tokens_for_embedding_request = 8191
    else:
        import langchain_google_genai.embeddings as genai_embeddings

        embedding_model = genai_embeddings.GoogleGenerativeAIEmbeddings(model="text-embedding-004")
        
        if maximum_tokens_for_embedding_request is None:
            maximum_tokens_for_embedding_request = 30000

    number_of_tokens = 0

    enc = tiktoken.get_encoding("cl100k_base")  # Similar to Gemini tokenizer

    for chunk in chunks:
        text = chunk
        number_of_tokens += len(enc.encode(text))

    print(f"Number of tokens: {number_of_tokens}")

    if(number_of_tokens > maximum_tokens_for_embedding_request):
        print("Warning: The total number of tokens in chunks exceeds the maximum allowed for the embedding model... Chunking on the chunks is required.")

        chunks_per_request = []
        embeddings_for_chunks = []

        segmented_chunks = []

        current_token_count = 0

        #Counter
        i = 0

        while i < len(chunks):

            chunk = chunks[i]
            chunk_token_count = len(enc.encode(chunk))
            if current_token_count + chunk_token_count <= maximum_tokens_for_embedding_request:
                chunks_per_request.append(chunk)

                current_token_count += chunk_token_count
            else:
                current_token_count = 0
                            
                segmented_chunks.append(chunks_per_request)
                
                print(f"Created vectorstore with {len(chunks_per_request)} chunks.")

                chunks_per_request = [chunk]
                current_token_count  = chunk_token_count

                
                if(len(enc.encode(chunk)) > maximum_tokens_for_embedding_request):
                    print("Error: A single chunk exceeds the absolute maximum token limit for embeddings.")
                    decomposition = []
                    for offset in range(0, len(chunk), maximum_tokens_for_embedding_request):
                        #Advance by maximum_tokens_for_embedding_request tokens
                        to_add = maximum_tokens_for_embedding_request

                        if(offset + maximum_tokens_for_embedding_request > len(chunk)):
                            to_add = len(chunk) - offset

                        decomposition.append(chunk[offset : offset + to_add])
                    
                    for j, chunk in enumerate(decomposition):
                        if (j == 0):
                            continue

                        chunks.insert(i + j, chunk)

                    #Add the first section of the decomposed chunk    
                    chunks_per_request = [decomposition[0]]

                    segmented_chunks.append(chunks_per_request)

                    chunks_per_request = []
                    embeddings_for_chunks = []
                    current_token_count = 0

                #In this case, reset counter and empty list 'cause I just directly inserted the new vectorstore in the wrapper list                 

            i += 1
            
        if(len(chunks_per_request) > 0):
            #In this case, the remaining chunk can be added to the vectorstore through the aggregated embedding (otherwise it would have been added already in the else case above)
            segmented_chunks.append(chunks_per_request)

            print(f"Created vectorstore with {len(chunks_per_request)} chunks.")

        total_length = len(segmented_chunks)

        # Reset counter
        i = 0
        while i < len(segmented_chunks):

            chunks_per_request = []

            j = 0

            while j < 9 and (i + j) < len(segmented_chunks):
                if(i + j < len(segmented_chunks)):
                    chunks_per_request += segmented_chunks[i + j]

                j += 1

            embeddings_for_chunks = add_embedding_for_chunks(embedding_model, chunks_per_request, Structure = Structure, CSV = CSV, KG = KG, Metadata = Metadata)
            
            vectorstore_wrapper = incorporate_texts_in_vectorstore(vectorstore_wrapper, chunks_per_request, embeddings_for_chunks, embedding_model)

            i = i + j

            print(f"Combined vectorstore with chunks from segmented chunk sets. Progress: {i}/{total_length}")


    return vectorstore_wrapper


def getRelevantChunks(wrappers, question, k=1500):

    relevant_chunks = []
    import math

    relevant_chunks = wrappers.similarity_search(question, k = k)   

    return [chunk.page_content for chunk in relevant_chunks]

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
