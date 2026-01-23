import csv
import json
import utils as utime4
import os
from datetime import datetime
import xgi

#Increase max size for each field (10 MB)
csv.field_size_limit(1000000000)

def safe_float(cell):
    if cell == "None" or cell == "none" or cell == "" or cell == "N/A" or cell == "n/a" or cell == "NaN" or cell == "nan" or cell == None:
        return cell 
    try:
        # First, replace comma with dot if present
        cell = cell.replace(',', '.')
        return str(float(cell))
    except:
        return cell

def mean_value(sum, numbers):
    return round(sum/numbers, 6)

def add_properties_of_metrics(hypergraph, to_build_hierarchy = False):

    # Metric Properties
    with open("./documentation/metrics_documentation/metrics_doc_with_strings.json", "r", encoding="utf-8") as f:
        metrics_description_dict = json.load(f)

        for metric, dict_of_properties in metrics_description_dict.items():
            if metric not in hypergraph.nodes:
                if to_build_hierarchy:
                    hypergraph.add_node(metric)
                    hypergraph.nodes[metric]["type"] = "measure"
                else:
                    continue
            for property in dict_of_properties:
                hypergraph.nodes[metric][property] = dict_of_properties[property]

    
    # Metrics dimension mapping
    with open("./documentation/metrics_hierarchy/dimension_category_mapping.json", "r", encoding="utf-8") as f:
        metrics_dimension_mapping_dict = json.load(f)

        for metric, dimension in metrics_dimension_mapping_dict.items():
            if metric not in hypergraph.nodes:
                if to_build_hierarchy:
                    hypergraph.add_node(metric)
                    hypergraph.nodes[metric]["type"] = "measure"
                else:
                    continue

            hypergraph.nodes[metric]["dimension"] = dimension

    return

def gen_hypegraph_json(hypergraph, structure, path):

    xgi.write_json(hypergraph, path)

    with open(path, "r") as f:
        hypergraph_json = json.load(f)

    del hypergraph_json["edge-data"]

    node_dict = hypergraph_json["node-data"]

    edge_dict = hypergraph_json["edge-dict"]

    edge_list = []

    if(structure != "normal"):
        for edge in edge_dict:

            edge_dict[edge] = list(edge_dict[edge])


            ####################
            # Reordering nodes based on their type
            nodes_positions_array = [_ for _ in range(len(edge_dict[edge]))] 

            for i, cell in enumerate(nodes_positions_array):
                i_th_node_type = node_dict[edge_dict[edge][i]]["type"]
            
                if(structure == "time_labeled"):

                    structure = "Timestamp"

                    if(i_th_node_type == "dataset"):
                        nodes_positions_array[i] = 0
                    elif(i_th_node_type == "measure"):
                        nodes_positions_array[i] = 1
                    elif(i_th_node_type == "measure_value"):
                        nodes_positions_array[i] = 2

                elif(structure == "measure_labeled"):

                    structure = "Measure"

                    if(i_th_node_type == "dataset"):
                        nodes_positions_array[i] = 0
                    elif(i_th_node_type == "measure_value"):
                        nodes_positions_array[i] = 1
                    elif(i_th_node_type == "timestamp"):
                        nodes_positions_array[i] = 2

                elif(structure == "kg_labeled"):

                    structure = "Dataset"

                    if(i_th_node_type == "measure"):
                        nodes_positions_array[i] = 0
                    elif(i_th_node_type == "measure_value"):
                        nodes_positions_array[i] = 1
                    elif(i_th_node_type == "timestamp"):
                        nodes_positions_array[i] = 2
                
                elif(structure == "measure_value_labeled"):

                    structure = "Measure Value"

                    if(i_th_node_type == "dataset"):
                        nodes_positions_array[i] = 0
                    elif(i_th_node_type == "measure"):
                        nodes_positions_array[i] = 1
                    elif(i_th_node_type == "timestamp"):
                        nodes_positions_array[i] = 2


            ordered_nodes_list = [_ for _ in range(len(edge_dict[edge]))]

            for i, position in enumerate(nodes_positions_array):
                current_node_to_insert__type = node_dict[edge_dict[edge][i]]["type"]

                if(current_node_to_insert__type == "measure_value"):
                    current_node_to_insert__type = "measure value"

                ordered_nodes_list[position] = current_node_to_insert__type + ": " + edge_dict[edge][i]
            

            curr_edge_dict = {"(" + structure + ")" + " Label: " + hypergraph.edges[int(edge)]["label"] : ordered_nodes_list}

            edge_list.append(curr_edge_dict)

    else:
        for edge in edge_dict:
            edge_dict[edge] = list(edge_dict[edge])

            ####################
            # Reordering nodes based on their type
            nodes_positions_array = [_ for _ in range(len(edge_dict[edge]))] 

            for i, cell in enumerate(nodes_positions_array):
                i_th_node_type = node_dict[edge_dict[edge][i]]["type"]
            
                if(i_th_node_type == "dataset"):
                    nodes_positions_array[i] = 0
                elif(i_th_node_type == "measure"):
                    nodes_positions_array[i] = 1
                elif(i_th_node_type == "measure_value"):
                    nodes_positions_array[i] = 2
                elif(i_th_node_type == "timestamp"):
                    nodes_positions_array[i] = 3


            ordered_nodes_list = [_ for _ in range(len(edge_dict[edge]))]

            for i, position in enumerate(nodes_positions_array):
                current_node_to_insert__type = node_dict[edge_dict[edge][i]]["type"]

                if(current_node_to_insert__type == "measure_value"):
                    current_node_to_insert__type = "measure value"

                ordered_nodes_list[position] = current_node_to_insert__type + ": " + edge_dict[edge][i]

            curr_edge_dict = {edge:ordered_nodes_list}

            edge_list.append(curr_edge_dict)

    
    hypergraph_json["nodes_contained_for_each_edge"] = edge_list

    node_dict = hypergraph_json["node-data"]

    # Removing type information from nodes
    for node in node_dict:
        del node_dict[node]["type"]

    del hypergraph_json["edge-dict"]

    with open(path, "w") as f:
        json.dump(hypergraph_json, f, indent=2)


from datetime import datetime

def is_date(string, format="%Y-%m-%d"):
    try:
        datetime.strptime(string, format)
        return True
    except ValueError:
        return False

def process_file_unique_hypergraph(file_paths, metrics_for_each_dataset, stop = 0, timestep = 1):

        #Must process at least one file
        if(stop < 0):
            stop = 0

        if(timestep < 1):
            timestep = 1

        # Counter to scan intervals through multiple timestamps
        countdown = 0

        # Counter until stop (if needed)
        count = 0

        #Boolean to consider skipped files
        skipped = False

        #array containing all the 5 different configurations of hyperedges
        hyperedges = [{}, {}, {}, {}, {}]

        for file_path in file_paths:

            # Countdown for skipping files
            if(countdown > 1):
                skip = True
                countdown = countdown - 1
                if(skipped):
                    countdown = countdown + 1
            else:
                skip = False
                countdown = timestep

            if(skip == False):
                if(stop == 0 or count < stop):
                    filename = os.path.basename(file_path)

                    #Check if at least one row will be processed
                    flag = False
                

                    if os.path.isfile(file_path):
                        with open(file_path, mode ="r", encoding='utf-8-sig', newline='') as csvfile:
                            reader = csv.reader(csvfile, delimiter=',')
                            counter = 0
                            row_containing_headers = None

                            for row in reader:
                                if counter == 0:
                                    row_containing_headers = row
                                    counter = counter + 1
                                    continue

                                #Convert decimal numbers from comma to dot
                                try:
                                    row = [safe_float(cell) for cell in row]

                                    #Building Hypergraph based on this Knowledge Graphs
                                    if not (row[0] == "NoiPA" or row[1] == "NoiPA" or row[0] == "dblp Knowledge Graph" or row[1] == "dblp Knowledge Graph" or row[0] == "BBC Programmes" or row[1] == "BBC Programmes" or row[0] == "bpr" or row[1] == "bpr" or row[0] == "Allie Abbreviation And Long Form Database in Life Science" or row[1] == "Allie Abbreviation And Long Form Database in Life Science" or row[0] == "w3c-wordnet" or row[1] == "w3c-wordnet" or row[0] == "LemmaBank" or row[1] == "LemmaBank" or row[0] == "micro-coronavirus" or row[1] == "micro-coronavirus" or row[0] == "CIDOC-CRM" or row[1] == "CIDOC-CRM" or row[0] == "environment-agency-bathing-water-quality" or row[1] == "environment-agency-bathing-water-quality"):
                                        continue


                                    flag = True
                                except ValueError as e:
                                    print("Error parsing row:", e)
                                    continue

                                filename = os.path.splitext(filename)[0]

                                for i in range(5):
                                    new_edges = utime4.add_KG_data(row_containing_headers, row, filename, metrics_for_each_dataset, i)

                                    to_add = {}

                                    for edge in new_edges:
                                        to_add.update({edge: new_edges[edge]})

                                    if(len(to_add) > 0):
                                        counter = counter + 1
                                        hyperedges[i].update(to_add)

                            #In case of no row has been processed, we will re-increment countdown variable to reset previous state
                            if(flag == False):
                                skipped = True

                        #Referring to overall count of processed files
                        count = count + 1

        
        print("Finished reading files")
        print("Total number of hyperedges: ", len(hyperedges[0]))

        #Construction of the Hypergraph with xgi
        import xgi

        #Normal Hypergraph
        H_normal = xgi.Hypergraph()

        for edge in hyperedges[0]:
            first_Node = hyperedges[0][edge][0]
            second_Node = hyperedges[0][edge][1]
            third_Node = hyperedges[0][edge][2]
            fourth_Node = hyperedges[0][edge][3]

            H_normal.add_node(first_Node, type="dataset")
            H_normal.nodes[first_Node]["type"] = "dataset"

            H_normal.add_node(second_Node, type="measure")
            H_normal.nodes[second_Node]["type"] = "measure"

            H_normal.add_node(third_Node, type="measure_value")
            H_normal.nodes[third_Node]["type"] = "measure_value"

            H_normal.add_node(fourth_Node, type="timestamp")
            H_normal.nodes[fourth_Node]["type"] = "timestamp"

            #distinguishing identical nodes by adding a space at the end of the string (in order to correctly have the same number of nodes for any edge)
            if first_Node == third_Node:
                #Previous add didn't work because of identical strings
                H_normal.add_node(third_Node + " ", type="measure_value")
                H_normal.nodes[third_Node]["type"] = "measure_value"

                hyperedges[0][edge][2] = third_Node + " "



            H_normal.add_edge(hyperedges[0][edge], edge)


        

        if(len(H_normal.nodes) == 0 or len(H_normal.edges) == 0):
            print("Empty hypergraph created")
            return [H_normal]
        
        H_time_label = xgi.Hypergraph()
        H_measure_label = xgi.Hypergraph() 
        H_kg_label = xgi.Hypergraph()
        H_measure_value_label = xgi.Hypergraph()

        xgiHGS = [H_time_label, H_measure_label, H_kg_label, H_measure_value_label]

        couples = []
        for i, el in enumerate(xgiHGS):
            #From H_time_label to H_measure_value_label, leaving out H_normal since it has already been created
            couples.append((hyperedges[i+1], el, i+1))

        for edges, hypergraph, index in couples:
            for i, edge in enumerate(edges):
                
                first_Node = hyperedges[index][edge][0]
                second_Node = hyperedges[index][edge][1]
                third_Node = hyperedges[index][edge][2]

                #Adding nodes with different types
                hypergraph.add_node(first_Node)
                hypergraph.add_node(second_Node)
                hypergraph.add_node(third_Node)


                #Binding types to nodes based on the custom hyperedge reordering
                if(index == 1):
                    #distinguishing identical nodes by adding a space at the end of the string (in order to correctly have the same number of nodes for any edge)
                    if first_Node == third_Node:
                        third_Node = third_Node + " "
                        hypergraph.add_node(third_Node)

                        #Updating edge information
                        edges[edge][2] = third_Node

                    hypergraph.nodes[first_Node]["type"] = "dataset"
                    hypergraph.nodes[second_Node]["type"] = "measure"
                    hypergraph.nodes[third_Node]["type"] = "measure_value"

                elif(index == 2):
                    if first_Node == second_Node:
                        second_Node = second_Node + " "
                        hypergraph.add_node(second_Node)

                        #Updating edge information
                        edges[edge][1] = second_Node

                    hypergraph.nodes[first_Node]["type"] = "dataset"
                    hypergraph.nodes[second_Node]["type"] = "measure_value"
                    hypergraph.nodes[third_Node]["type"] = "timestamp"
                elif(index == 3):

                    #Here, nodes can't be identical (this was a problem with (dataset, measure) nodes couples)
                    hypergraph.nodes[first_Node]["type"] = "measure"
                    hypergraph.nodes[second_Node]["type"] = "measure_value"
                    hypergraph.nodes[third_Node]["type"] = "timestamp"
                elif(index == 4):

                    #Here, nodes can't be identical (this was a problem with (dataset, measure) nodes couples)
                    hypergraph.nodes[first_Node]["type"] = "timestamp"
                    hypergraph.nodes[second_Node]["type"] = "dataset"
                    hypergraph.nodes[third_Node]["type"] = "measure"


                label = edges[edge][-1]

                #removing last element (that represents the label)
                correct_edge = edges[edge][:-1]

                hypergraph.add_edge(correct_edge, edge, label=label)

        print("Hypergraph created")

        for i, hypergraph in enumerate([H_normal, H_time_label, H_measure_label, H_kg_label, H_measure_value_label]):
            if(i == 2):
                add_properties_of_metrics(hypergraph, to_build_hierarchy=True)
            else:
                add_properties_of_metrics(hypergraph)
                
            print("Number of nodes in hypergraph ", i, ": ", len(hypergraph.nodes), " Number of edges: ", len(hypergraph.edges))

        return [H_normal, H_time_label, H_measure_label, H_kg_label, H_measure_value_label]

    
if __name__ == "__main__":    


    directory = './Weekly_Data'


    #Accessing to them in an ascending sorted order
    files = sorted(os.listdir(directory))
    file_paths = [directory + "/" + f for f in files if os.path.isfile(os.path.join(directory, f))]

    metrics_for_each_dataset = {}

    stop = 0 #Set to 0 to process all files
    timestep = 1 #Set to 1 to process all files

    #Hypergraph
    H_list = process_file_unique_hypergraph(file_paths, metrics_for_each_dataset, stop, timestep) #For example, if timestep = 10, that means that we will take one file every 10 files (weeks)

    print("Metrics for each dataset built")

    documentation_path = "documentation_and_hierarchy"

    # Export to JSON file
    for i, hypergraph in enumerate(H_list):
        if(i == 0):
            #xgi.write_hif(hypergraph, "../generated_hypergraphs_json/test_normal_hypergraph.json")
            gen_hypegraph_json(hypergraph, "normal", "../generated_hypergraphs_json/" + documentation_path + "/test_4_uniform_hypergraph.json")
        elif(i == 1):
            #xgi.write_hif(hypergraph, "../generated_hypergraphs_json/test_time_labeled_hypergraph.json")
            gen_hypegraph_json(hypergraph, "time_labeled", "../generated_hypergraphs_json/" + documentation_path + "/test_time_labeled_hypergraph.json")
        elif(i == 2):
            #xgi.write_hif(hypergraph, "../generated_hypergraphs_json/test_measure_labeled_hypergraph.json")
            gen_hypegraph_json(hypergraph, "measure_labeled", "../generated_hypergraphs_json/" + documentation_path + "/test_measure_labeled_hypergraph.json")
        elif(i == 3):
            #xgi.write_hif(hypergraph, "../generated_hypergraphs_json/test_kg_labeled_hypergraph.json")
            gen_hypegraph_json(hypergraph, "kg_labeled", "../generated_hypergraphs_json/" + documentation_path + "/test_kg_labeled_hypergraph.json")
        elif(i == 4):
            #xgi.write_hif(hypergraph, "../generated_hypergraphs_json/test_measure_value_labeled_hypergraph.json")
            gen_hypegraph_json(hypergraph, "measure_value_labeled", "../generated_hypergraphs_json/" + documentation_path + "/test_value_labeled_hypergraph.json")
