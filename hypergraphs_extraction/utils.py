import json
import re

# Global counter for edge identifiers
counter = 0

def returning_number_of_triples(row):

    number_of_triples = None

    try:
        #trying with metadata column first
        number_of_triples = float(row[60])
    except (ValueError, TypeError):
        pass

    if(number_of_triples == None):
        try:
            #trying with VoID description second
            number_of_triples = float(row[59])
        except (ValueError, TypeError):
            pass


    return number_of_triples

def return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, list):

    global counter

    #increasing counter for edge identifier
    counter = counter + 1

    if(list[1] not in metrics_for_each_dataset[row[0]]):
        metrics_for_each_dataset[row[0]][list[1]] = []

    value = None

    try:
        value = float(list[2])
        metrics_for_each_dataset[row[0]][list[1]].append(value)
    except (ValueError, TypeError):
        pass



    if(structure_type == 0 or structure_type == 1):
        return [list[0], list[1], list[2], list[3]]
    elif(structure_type == 2):
        return [list[0], list[2], list[3], list[1]]
    elif(structure_type == 3):
        return [list[1], list[2], list[3], list[0]]
    elif(structure_type == 4):
        return [list[3], list[0], list[1], list[2]]
    


def add_availability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    
    availability_edges = {}

    #Accessibility SPARQL metric
    accessibility_value = row[2]
    accessibility_string = row_containing_headers[2]



    availability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], accessibility_string, (str(accessibility_value)), timestamp])

    accessibility_value = None
    accessibility_string = row_containing_headers[4]
    try:
        accessibility_value = round(float(row[4]), 1)
    except (ValueError, TypeError):
        pass

    availability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], accessibility_string, (str(accessibility_value) if accessibility_value == None else f"{accessibility_value:.6f}"), timestamp])

    deferenceability_value = None
    dereferenceability_string = row_containing_headers[118]
    try:
        deferenceability_value = round(float(row[118]), 1)
    except (ValueError, TypeError):
        pass

    availability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], dereferenceability_string, (str(deferenceability_value) if deferenceability_value == None else f"{deferenceability_value:.6f}"), timestamp])

    return availability_edges


def add_licensing(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):

    licensing_edges = {}

    #Machine readable license (metadata) metric
    machine_readable_license_value = row[42]
    machine_readable_license_string = row_containing_headers[42]

    null_value = False

    if(machine_readable_license_value == "FALSE" or machine_readable_license_value == "False" or machine_readable_license_value == "[]" or machine_readable_license_value == "-" or machine_readable_license_value == "endpoint offline" or machine_readable_license_value == "endpoint absent"):
        null_value = True

    licensing_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], machine_readable_license_string, (str(machine_readable_license_value)), timestamp])
    
    if(null_value == True):
        machine_readable_license_value = 0
    else:
        machine_readable_license_value = 1

    
    #Machine readable license (query) metric
    machine_readable_license_value = row[43]
    machine_readable_license_string = row_containing_headers[43]

    null_value = False

    if(machine_readable_license_value == "FALSE" or machine_readable_license_value == "False" or machine_readable_license_value == "[]" or machine_readable_license_value == "-" or machine_readable_license_value == "endpoint offline" or machine_readable_license_value == "endpoint absent"):
        null_value = True

    licensing_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], machine_readable_license_string, (str(machine_readable_license_value)), timestamp])
    
    if(null_value == True):
        machine_readable_license_value = 0
    else:
        machine_readable_license_value = 1


    human_readable_license_value = row[43]
    human_readable_license_value_string = row_containing_headers[43]

    null_value = False

    if(human_readable_license_value == "FALSE" or human_readable_license_value == "False" or human_readable_license_value == "[]" or human_readable_license_value == "-" or human_readable_license_value == "endpoint offline" or human_readable_license_value == "endpoint absent"):
        null_value = True
        # human_readable_license_value = None

    licensing_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], human_readable_license_value_string, (str(human_readable_license_value)), timestamp])
    

    if(null_value == True):
        human_readable_license_value = 0
    else:
        human_readable_license_value = 1


    human_readable_license_value = row[44]
    human_readable_license_value_string = row_containing_headers[44]

    if(human_readable_license_value == "FALSE" or human_readable_license_value == "False" or human_readable_license_value == "[]" or human_readable_license_value == "-" or human_readable_license_value == "endpoint offline" or human_readable_license_value == "endpoint absent"):
        null_value = True

    licensing_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], human_readable_license_value_string, (str(human_readable_license_value)), timestamp])

    if(null_value == True):
        human_readable_license_value = 0
    else:
        human_readable_license_value = 1

    
    return licensing_edges


def add_interlinking(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):

    interlinking_edges = {}

    degree_connection_value = row[65]
    degree_connection_value_string = row_containing_headers[65]

    interlinking_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], degree_connection_value_string, str(degree_connection_value), timestamp])
    
    clustering_coefficient_value = None
    clustering_coefficient_value_string = row_containing_headers[66]

    try:
        clustering_coefficient_value = round(float(row[66]), 1)
    except (ValueError, TypeError):
        pass

    interlinking_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], clustering_coefficient_value_string, (str(clustering_coefficient_value) if clustering_coefficient_value == None else f"{clustering_coefficient_value:.6f}"), timestamp])
      
    centrality_value = None
    centrality_value_string = row_containing_headers[67]

    try:
        centrality_value = round(float(row[67]), 1)
    except (ValueError, TypeError):
        pass

    interlinking_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], centrality_value_string, (str(centrality_value) if centrality_value == None else f"{centrality_value:.6f}"), timestamp])
        
    sameAsChainsNumber = None
    sameAsChainsNumber_string = row_containing_headers[68]
    
    try:
        sameAsChainsNumber = float(row[68])
    except (ValueError, TypeError):
        pass

    numberOfTriples = returning_number_of_triples(row)
    sameAs_chains_value = None

    if(sameAsChainsNumber != None and numberOfTriples != None):
        sameAs_chains_value = round(sameAsChainsNumber / numberOfTriples, 1)

    interlinking_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], sameAsChainsNumber_string, (str(sameAsChainsNumber) if sameAsChainsNumber == None else f"{sameAsChainsNumber:.6f}"), timestamp])
    
    skos_mapping_count = None
    skos_mapping_count_string = row_containing_headers[139]
    
    try:
        skos_mapping_count= float(row[139])
    except (ValueError, TypeError, IndexError):
        pass

    skos_mapping_frequency_value = None

    if(skos_mapping_count != None and numberOfTriples != None):
        skos_mapping_frequency_value = round(skos_mapping_count / numberOfTriples, 1)

    interlinking_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], skos_mapping_count_string, (str(skos_mapping_count) if skos_mapping_count == None else f"{skos_mapping_count:.6f}"), timestamp])
    
    return interlinking_edges


def add_security(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):

    security_edges = {}


    authentication_required_value = row[19]
    authentication_required_value_string = row_containing_headers[19]

    security_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], authentication_required_value_string, (str(authentication_required_value)), timestamp])
    
    https_used_value = row[18]
    https_used_value_string = row_containing_headers[18]

    
    security_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], https_used_value_string, (str(https_used_value)), timestamp])
    
    return security_edges

def add_performance(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):

    performance_edges = {}

    average_latency = None
    average_latency_string = row_containing_headers[50]

    try:
        average_latency = round(float(row[50]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], average_latency_string, (str(average_latency) if average_latency == None else f"{average_latency:.6f}"), timestamp])
    
    low_latency_value = None
    if(average_latency != None):
        if(average_latency < 1):
            low_latency_value = 1
        else:
            low_latency_value = round(average_latency / 1000, 1)

    maximum_latency = None
    maximum_latency_string = row_containing_headers[49]

    try:
        maximum_latency = round(float(row[49]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], maximum_latency_string, (str(maximum_latency) if maximum_latency == None else f"{maximum_latency:.6f}"), timestamp])

    minimum_latency = None
    minimum_latency_string = row_containing_headers[45]
    try:
        minimum_latency = round(float(row[45]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], minimum_latency_string, (str(minimum_latency) if minimum_latency == None else f"{minimum_latency:.6f}"), timestamp])

    _25th_percentile_latency = None
    _25th_percentile_latency_string = row_containing_headers[46]

    try:
        _25th_percentile_latency = round(float(row[46]), 1)
    except (ValueError, TypeError):
        pass
    
    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _25th_percentile_latency_string, (str(_25th_percentile_latency) if _25th_percentile_latency == None else f"{_25th_percentile_latency:.6f}"), timestamp])

    median_latency = None
    median_latency_string = row_containing_headers[47]

    try:
        median_latency = round(float(row[47]), 1)
    except (ValueError, TypeError):
        pass
    
    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], median_latency_string, (str(median_latency) if median_latency == None else f"{median_latency:.6f}"), timestamp])

    _75th_percentile_latency = None
    _75th_percentile_latency_string = row_containing_headers[48]
    
    try:
        _75th_percentile_latency = round(float(row[48]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _75th_percentile_latency_string, (str(_75th_percentile_latency) if _75th_percentile_latency == None else f"{_75th_percentile_latency:.6f}"), timestamp])

    standard_deviation_latency = None
    standard_deviation_latency_string = row_containing_headers[51]

    try:
        standard_deviation_latency = round(float(row[51]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], standard_deviation_latency_string, (str(standard_deviation_latency) if standard_deviation_latency == None else f"{standard_deviation_latency:.6f}"), timestamp])
    
    average_throughput = None
    average_throughput_string = row_containing_headers[57]

    try:
        average_throughput = round(float(row[57]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], average_throughput_string, (str(average_throughput) if average_throughput == None else f"{average_throughput:.6f}"), timestamp])

    minimum_throughput = None
    minimum_throughput_string = row_containing_headers[52]

    try:
        minimum_throughput = round(float(row[52]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], minimum_throughput_string, (str(minimum_throughput) if minimum_throughput == None else f"{minimum_throughput:.6f}"), timestamp])

    _25th_percentile_throughput = None
    _25th_percentile_throughput_string = row_containing_headers[53]

    try:
        _25th_percentile_throughput = round(float(row[53]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _25th_percentile_throughput_string, (str(_25th_percentile_throughput) if _25th_percentile_throughput == None else f"{_25th_percentile_throughput:.6f}"), timestamp])

    median_throughput = None
    median_throughput_string = row_containing_headers[54]

    try:
        median_throughput = round(float(row[54]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], median_throughput_string, (str(median_throughput) if median_throughput == None else f"{median_throughput:.6f}"), timestamp])

    _75th_percentile_throughput = None
    _75th_percentile_throughput_string = row_containing_headers[55]

    try:
        _75th_percentile_throughput = round(float(row[55]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _75th_percentile_throughput_string, (str(_75th_percentile_throughput) if _75th_percentile_throughput == None else f"{_75th_percentile_throughput:.6f}"), timestamp])

    maximum_throughput = None
    maximum_throughput_string = row_containing_headers[56]

    try:
        maximum_throughput = round(float(row[56]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], maximum_throughput_string, (str(maximum_throughput) if maximum_throughput == None else f"{maximum_throughput:.6f}"), timestamp])

    standard_deviation_throughput = None
    standard_deviation_throughput_string = row_containing_headers[58]

    try:
        standard_deviation_throughput = round(float(row[58]), 1)
    except (ValueError, TypeError):
        pass

    performance_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], standard_deviation_throughput_string, (str(standard_deviation_throughput) if standard_deviation_throughput == None else f"{standard_deviation_throughput:.6f}"), timestamp])

    high_throughput_value = None

    if(average_throughput != None):
        if(average_throughput > 5):
            high_throughput_value = 1
        else:
            high_throughput_value = round(average_throughput / 200, 1)

    return performance_edges


def add_semantic_accuracy(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    semantic_accuracy_edges = {}

    triples_with_empty_annotation = None
    triples_with_empty_annotation_string = row_containing_headers[102]

    try:
        triples_with_empty_annotation = float(row[102])
    except (ValueError, TypeError):
        pass
    
    semantic_accuracy_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_empty_annotation_string, (str(triples_with_empty_annotation) if triples_with_empty_annotation == None else f"{triples_with_empty_annotation:.6f}"), timestamp])
       
    triples_with_white_spaces = None
    triples_with_white_spaces_string = row_containing_headers[103]

    try:
        triples_with_white_spaces = float(row[103])
    except (ValueError, TypeError):
        pass

    semantic_accuracy_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_white_spaces_string, (str(triples_with_white_spaces) if triples_with_white_spaces == None else f"{triples_with_white_spaces:.6f}"), timestamp])
    
    triples_with_data_type_problem = None
    triples_with_data_type_problem_string = row_containing_headers[104]

    try:
        triples_with_data_type_problem = round(float(row[104]), 1)
    except (ValueError, TypeError):
        pass

    semantic_accuracy_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_data_type_problem_string, (str(triples_with_data_type_problem) if triples_with_data_type_problem == None else f"{triples_with_data_type_problem:.6f}"), timestamp])
    
    triples_inconsistent_with_functional_property = None
    triples_inconsistent_with_functional_property_string = row_containing_headers[105]
    
    try:
        triples_inconsistent_with_functional_property = round(float(row[105]), 1)
    except (ValueError, TypeError):
        pass

    semantic_accuracy_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_inconsistent_with_functional_property_string, (str(triples_inconsistent_with_functional_property) if triples_inconsistent_with_functional_property == None else f"{triples_inconsistent_with_functional_property:.6f}"), timestamp])
        
    triples_with_invalid_inverse_functional_properties = None
    triples_with_invalid_inverse_functional_properties_string = row_containing_headers[106]

    try:
        triples_with_invalid_inverse_functional_properties = round(float(row[106]), 1)
    except (ValueError, TypeError):
        pass

    semantic_accuracy_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_invalid_inverse_functional_properties_string, (str(triples_with_invalid_inverse_functional_properties) if triples_with_invalid_inverse_functional_properties == None else f"{triples_with_invalid_inverse_functional_properties:.6f}"), timestamp])
    
    return semantic_accuracy_edges


def add_consistency(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    consistency_edges ={}

    entities_with_disjoint_classes = None
    entities_with_disjoint_classes_string = row_containing_headers[94]

    try:
        entities_with_disjoint_classes = round(float(row[94]), 1)
    except (ValueError, TypeError):
        pass

    consistency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], entities_with_disjoint_classes_string, (str(entities_with_disjoint_classes) if entities_with_disjoint_classes == None else f"{entities_with_disjoint_classes:.6f}"), timestamp])
    
    triples_misplaces_classes = None
    triples_misplaces_classes_string = row_containing_headers[96]

    try:
        triples_misplaces_classes = round(float(row[96]), 1) 
    except (ValueError, TypeError):
        pass

    consistency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_misplaces_classes_string, (str(triples_misplaces_classes) if triples_misplaces_classes == None else f"{triples_misplaces_classes:.6f}"), timestamp])
    
    triples_misplaces_properties = None
    triples_misplaces_properties_string = row_containing_headers[95]

    try:
        triples_misplaces_properties = round(float(row[95]), 1)
    except (ValueError, TypeError):
        pass

    consistency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_misplaces_properties_string, (str(triples_misplaces_properties) if triples_misplaces_properties == None else f"{triples_misplaces_properties:.6f}"), timestamp])
    
    triples_with_undefined_classes = None
    triples_with_undefined_classes_string = row_containing_headers[98]
    
    try:
        triples_with_undefined_classes = round(float(row[98]), 1)
    except (ValueError, TypeError):
        pass

    consistency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_undefined_classes_string, (str(triples_with_undefined_classes) if triples_with_undefined_classes == None else f"{triples_with_undefined_classes:.6f}"), timestamp])
    
    triples_with_undefined_properties = None
    triples_with_undefined_properties_string = row_containing_headers[99]
    
    try:
        triples_with_undefined_properties = round(float(row[99]), 1)
    except (ValueError, TypeError):
        pass

    consistency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_undefined_properties_string, (str(triples_with_undefined_properties) if triples_with_undefined_properties == None else f"{triples_with_undefined_properties:.6f}"), timestamp])
    
    ontology_hijacking_value = row[97]
    ontology_hijacking_value_string = row_containing_headers[97]

    consistency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], ontology_hijacking_value_string, (str(ontology_hijacking_value)), timestamp])
       
    return consistency_edges


def add_conciseness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    conciseness_edges = {}

    triples_with_intensional_conciseness = None
    triples_with_intensional_conciseness_string = row_containing_headers[101]
    
    try:
        #matches
        match = re.search(r"\d+(?:\.\d+)?", row[101])
        triples_with_intensional_conciseness = round(float(match.group()), 1)
    except (ValueError, TypeError, AttributeError, IndexError):
        pass

    conciseness_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_intensional_conciseness_string, (str(triples_with_intensional_conciseness) if triples_with_intensional_conciseness == None else f"{triples_with_intensional_conciseness:.6f}"), timestamp])
    
    triples_with_extensional_conciseness = None
    triples_with_extensional_conciseness_string = row_containing_headers[100]
    
    try:
        match = re.search(r"\d+(?:\.\d+)?", row[100])
        triples_with_extensional_conciseness = round(float(match.group()), 1)
    except (ValueError, TypeError, AttributeError, IndexError):
        pass

    conciseness_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], triples_with_extensional_conciseness_string, (str(triples_with_extensional_conciseness) if triples_with_extensional_conciseness == None else f"{triples_with_extensional_conciseness:.6f}"), timestamp])
        
    return conciseness_edges


def add_reputation(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    reputation_edges = {}

    pageRank = None
    pageRank_string = row_containing_headers[70]
    try:
        pageRank = round(float(row[70]), 6)
    except (ValueError, TypeError):
        pass

    reputation_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], pageRank_string, (str(pageRank) if pageRank == None else f"{pageRank:.6f}"), timestamp])
        
    return reputation_edges


def add_believability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):

    believability_edges = {}


    believability_value = None
    believability_value_string = row_containing_headers[128]
    try:
        believability_value = round(float(row[128]), 1)
    except (ValueError, TypeError, IndexError):
        pass

    believability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], believability_value_string, (str(believability_value) if believability_value == None else f"{believability_value:.6f}"), timestamp])
        
    return believability_edges


def add_verifiability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    verifiability_edges = {}

    authors_value = row[77]
    authors_value_string = row_containing_headers[77]
    null_value = False
    found = False

    if(authors_value == "-" or authors_value == "FALSE" or authors_value == "False" or authors_value == "[]" or authors_value == "endpoint absent" or authors_value == "endpoint offline"):
        null_value = True
        # authors_value = None
    else:
        found = True


    verifiability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], authors_value_string, (str(authors_value)), timestamp])

    
    authors_value = row[76]
    authors_value_string = row_containing_headers[76]

    if(authors_value == "-" or authors_value == "FALSE" or authors_value == "False" or authors_value == "[]" or authors_value == "endpoint absent" or authors_value == "endpoint offline"):
        null_value = True
    else:
        found = True

    verifiability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], authors_value_string, (str(authors_value)), timestamp])

    if(not found):
        authors_value = 0
    else:
        authors_value = 1

    contributors_value = row[78]
    contributors_value_string = row_containing_headers[78]

    null_value = False

    if(contributors_value == "-" or contributors_value == "FALSE" or contributors_value == "False" or contributors_value == "[]" or contributors_value == "endpoint absent" or contributors_value == "endpoint offline"):
        null_value = True

    verifiability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], contributors_value_string, (str(contributors_value)), timestamp])

    if(null_value):
        contributors_value = 0
    else:
        contributors_value = 1
   
    publishers_value = row[79]
    publishers_value_string = row_containing_headers[79]

    null_value = False

    if(publishers_value == "-" or publishers_value == "FALSE" or publishers_value == "False" or publishers_value == "[]" or publishers_value == "endpoint absent" or publishers_value == "endpoint offline"):
        null_value = True

    verifiability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], publishers_value_string, (str(publishers_value)), timestamp])

    if(null_value):
        publishers_value = 0
    else:
        publishers_value = 1
   
    sources_value = row[80]
    sources_value_string = row_containing_headers[80]

    null_value = False

    if(sources_value == "-" or sources_value == "FALSE" or sources_value == "False" or sources_value == "[]" or sources_value == "endpoint absent" or sources_value == "endpoint offline"):
        null_value = True

    verifiability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], sources_value_string, (str(sources_value)), timestamp])

    if(null_value):
        sources_value = 0
    else:
        sources_value = 1
    
    signed_value = row[81]
    signed_value_string = row_containing_headers[81]

    null_value = False

    if(signed_value == "-" or signed_value == "FALSE" or signed_value == "[]" or signed_value == "endpoint absent" or signed_value == "endpoint offline"):
        null_value = True
  
    verifiability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], signed_value_string, (str(signed_value)), timestamp])

    if(null_value):
        signed_value = 0
    else:
        signed_value = 1

    return verifiability_edges


def add_currency(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    currency_edges = {}

    age_of_data = row[8]
    age_of_data_string = row_containing_headers[8]

    null_value = False

    if(age_of_data == "-" or age_of_data == "FALSE" or age_of_data == "False" or age_of_data == "[]" or age_of_data == "endpoint absent" or age_of_data == "endpoint offline" or age_of_data == "insufficient data" or age_of_data == "Could not process formulated query on indicated endpoint"):
        null_value = True
    
    currency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], age_of_data_string, (str(age_of_data)), timestamp])

    if(null_value):
        age_of_data = 0
    else:
        age_of_data = 1

    modification_date_of_statements = row[9]
    modification_date_of_statements_string = row_containing_headers[9]

    null_value = False

    if(modification_date_of_statements == "-" or modification_date_of_statements == "FALSE" or modification_date_of_statements == "False" or modification_date_of_statements == "[]" or modification_date_of_statements == "endpoint absent" or modification_date_of_statements == "endpoint offline" or modification_date_of_statements == "insufficient data" or modification_date_of_statements == "Could not process formulated query on indicated endpoint"):
        null_value = True

    currency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], modification_date_of_statements_string, (str(modification_date_of_statements)), timestamp])

    if(null_value):
        modification_date_of_statements = 0
    else:
        modification_date_of_statements = 1

    time_elapsed_since_last_modification = row[11]
    time_elapsed_since_last_modification_string = row_containing_headers[11]

    null_value = False

    if(time_elapsed_since_last_modification == "-" or time_elapsed_since_last_modification == "FALSE" or time_elapsed_since_last_modification == "False" or time_elapsed_since_last_modification == "[]" or time_elapsed_since_last_modification == "endpoint absent" or time_elapsed_since_last_modification == "endpoint offline" or time_elapsed_since_last_modification == "insufficient data" or time_elapsed_since_last_modification == "Could not process formulated query on indicated endpoint"):
        null_value = True
    
    currency_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], time_elapsed_since_last_modification_string, (str(time_elapsed_since_last_modification)), timestamp])

    if(null_value):
        time_elapsed_since_last_modification = 0
    else:
        time_elapsed_since_last_modification = 1

    history_of_updates = row[12]
    history_of_updates_string = row_containing_headers[12]

    null_value = False

    if(history_of_updates == "-" or history_of_updates == "FALSE" or history_of_updates == "False" or history_of_updates == "[]" or history_of_updates == "endpoint absent" or history_of_updates == "endpoint offline" or history_of_updates == "insufficient data" or history_of_updates == "Could not process formulated query on indicated endpoint"):
        null_value = True
    
    currency_edges[ counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], history_of_updates_string, (str(history_of_updates)), timestamp])

    if(null_value):
        history_of_updates = 0
    else:
        history_of_updates = 1

    return currency_edges


def add_timeliness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    timeliness_edges = {}

    #Considered as validation
    update_frequency = row[64]
    update_frequency_string = row_containing_headers[64]

    null_value = False

    if(update_frequency == "-" or update_frequency == "FALSE" or update_frequency == "False" or update_frequency =="endpoint absent" or update_frequency == "endpoint offline" or update_frequency == "absent"):
        null_value = True
    
    timeliness_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], update_frequency_string, (str(update_frequency)), timestamp])

    if(null_value):
        update_frequency = 0
    else:
        update_frequency = 1

    return timeliness_edges


def add_completeness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    completeness_edges = {}

    interlinking_completeness = None
    interlinking_completeness_string = row_containing_headers[84]

    try:
        interlinking_completeness = round(float(row[84]), 1)
    except (ValueError, TypeError):
        pass

    completeness_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], interlinking_completeness_string, (str(interlinking_completeness) if interlinking_completeness == None else f"{interlinking_completeness:.6f}"), timestamp])
    
    return completeness_edges

def add_amount_of_data(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    amount_of_data_edges = {}

    number_of_triples = None

    number_of_triples_metadata = None
    number_of_triples_metadata_string = row_containing_headers[60]
    try:
        number_of_triples_metadata = float(row[60])
        number_of_triples = number_of_triples_metadata
    except (ValueError, TypeError):
        pass
    
    amount_of_data_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], number_of_triples_metadata_string, (str(number_of_triples_metadata) if number_of_triples_metadata == None else f"{number_of_triples_metadata:.6f}"), timestamp])

    number_of_triples_void = None
    number_of_triples_void_string = row_containing_headers[59]

    try:
        number_of_triples_void = float(row[59])
        number_of_triples = number_of_triples_void
    except (ValueError, TypeError):
        pass

    amount_of_data_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], number_of_triples_void_string, (str(number_of_triples_void) if number_of_triples_void == None else f"{number_of_triples_void:.6f}"), timestamp])

    if(number_of_triples == None):
        number_of_triples = 1
    else:
        number_of_triples = 0

    level_of_detail = row[63]
    level_of_detail_string = row_containing_headers[63]

    null_value = False

    if(level_of_detail == "-" or level_of_detail.strip() == "" or level_of_detail == "False" or level_of_detail == "FALSE" or level_of_detail == "endpoint absent" or level_of_detail == "endpoint offline"):
        null_value = True

    try:
        level_of_detail = f"{float(level_of_detail):.6f}"
    except (ValueError, TypeError):
        pass

    amount_of_data_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], level_of_detail_string, (str(level_of_detail)), timestamp])

    if(null_value):
        level_of_detail = 0
    else:
        level_of_detail = 1

    number_of_entities = None
    number_of_entities_string = row_containing_headers[61]
    
    try:
        number_of_entities = float(row[61])
    except (ValueError, TypeError):
        pass
    
    number_of_entities_with_regex = row[62]
    number_of_entities_with_regex_string = row_containing_headers[62]


    amount_of_data_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], number_of_entities_string, (str(number_of_entities) if number_of_entities == None else f"{number_of_entities:.6f}"), timestamp])

    null_value = False

    if(number_of_entities_with_regex == "-" or number_of_entities_with_regex == "FALSE" or number_of_entities_with_regex == "False" or number_of_entities_with_regex.strip() == "" or number_of_entities_with_regex == "False"):
        null_value = True


    amount_of_data_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], number_of_entities_with_regex_string, (str(number_of_entities_with_regex)), timestamp])

    scope_retrievable = None

    if((number_of_entities == None and number_of_entities_with_regex == None)):
        scope_retrievable = 0
    else:
        scope_retrievable = 1

    return amount_of_data_edges


def add_representational_conciseness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    representational_conciseness_edges = {}

    #there is no "keeping URIs short" metric identifiable in our data
    return representational_conciseness_edges


def add_interoperability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    interoperability_edges = {}

    new_vocabolaries = row[85]
    new_vocabolaries_string = row_containing_headers[85]

    null_value = False

    reuse_value = None
    if(new_vocabolaries == "-" or new_vocabolaries == "False" or new_vocabolaries.strip() == "" or new_vocabolaries == "FALSE" or new_vocabolaries == "[]"):
        null_value = True

    interoperability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], new_vocabolaries_string, (str(new_vocabolaries)), timestamp])

    if(null_value):
        reuse_value = 1
    else:
        reuse_value = 0

    new_terms = row[86]
    new_terms_string = row_containing_headers[86]

    null_value = False

    reuse_value = None
    if(new_terms == "-" or new_terms == "FALSE" or new_terms == "False" or new_terms == "[]"):
        null_value = True

    interoperability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], new_terms_string, (str(new_terms)), timestamp])

    if(null_value):
        reuse_value = 1
    else:
        reuse_value = 0

    return interoperability_edges


def add_understandability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    understandability_edges = {}

    num_of_labels = None
    num_of_labels_string = row_containing_headers[87]
    
    try:
        num_of_labels = float(row[87])
    except (ValueError, TypeError):
        pass
    
    num_of_triples = returning_number_of_triples(row)

    understandability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], num_of_labels_string, (str(num_of_labels) if num_of_labels == None else f"{num_of_labels:.6f}"), timestamp])

    human_readable_labelling_value = None

    if(num_of_labels != None and num_of_triples != None):
        try:
            human_readable_labelling_value = round(float(num_of_labels / num_of_triples * 100), 1)
        except (ValueError, TypeError):
            pass

    presence_of_examples = row[90]
    presence_of_examples_string = row_containing_headers[90]

    understandability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], presence_of_examples_string, (str(presence_of_examples)), timestamp])
    
    regex_uri_value = row[89]
    regex_uri_value_string = row_containing_headers[89]

    null_value = False

    if(regex_uri_value == "-" or regex_uri_value == "False" or regex_uri_value == "FALSE" or regex_uri_value == "[]"):
        null_value = True

    understandability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], regex_uri_value_string, (str(regex_uri_value)), timestamp])

    if(null_value):
        regex_uri_value = 0
    else:
        regex_uri_value = 1

    understandability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], "_existent_regex_of_the_uris", (str(regex_uri_value) if regex_uri_value == None else f"{regex_uri_value:.6f}"), timestamp])

    title = row[1]
    title_string = row_containing_headers[1]

    understandability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], title_string, (str(title)), timestamp])

    description = row[71]
    description_string = row_containing_headers[71]

    understandability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], description_string, (str(description)), timestamp])

    sources = row[80]

    indication_of_metadata_value = None

    return understandability_edges


def add_interpretability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    interpretability_edges = {}

    use_of_rdf_structures = row[41]
    use_of_rdf_structures_string = row_containing_headers[41]

    interpretability_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], use_of_rdf_structures_string, (str(use_of_rdf_structures)), timestamp])
    
    return interpretability_edges


def add_versatility(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):
    versatility_edges = {}

    languages = None
    languages_metadata = row[13]
    languages_metadata_string = row_containing_headers[13]


    if(languages_metadata == "-" or languages_metadata == f"{{}}" or languages_metadata == "[]" or languages_metadata == "False" or languages_metadata == "FALSE"):
        pass
    else:
        languages = languages_metadata

    versatility_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], languages_metadata_string, (str(languages_metadata)), timestamp])

    languages_query = row[14]
    languages_query_string = row_containing_headers[14]

    if(languages_query == "-" or languages_query == f"{{}}" or languages_query == "[]" or languages_query == "False" or languages_query == "FALSE"):
        pass
    else:
        languages = languages_query

    versatility_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], languages_query_string, (str(languages_query)), timestamp])

    if(languages == None):
        languages = 0
    else:
        languages = 1


    serialization_formats = row[15]
    serialization_formats_string = row_containing_headers[15]

    not_specified = False
    if(serialization_formats == "-" or serialization_formats == "[]" or serialization_formats == "FALSE" or serialization_formats == "False" or serialization_formats.strip() == "" or serialization_formats == "endpoint absent" or serialization_formats == "endpoint offline"):
        not_specified = True

    versatility_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], serialization_formats_string, (str(serialization_formats)), timestamp])

    if(not_specified):
        serialization_formats = 0
    else:
        serialization_formats = 1

    availability_of_sparql_endpoint = row[2]
    
    availability_of_rdf_dump = row[4]

    accessing_of_data_in_different_ways = None

    if(availability_of_sparql_endpoint == "Available" and availability_of_rdf_dump == "1"):
        accessing_of_data_in_different_ways = 1
    else:
        accessing_of_data_in_different_ways = 0

    return versatility_edges

def add_residual_fields(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset):

    residual_edges = {}

    sparql_endpoint_url = row[3]
    sparql_endpoint_url_string = row_containing_headers[3]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], sparql_endpoint_url_string, (str(sparql_endpoint_url)), timestamp])

    rdf_dump_query = row[5]
    rdf_dump_query_string = row_containing_headers[5]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], rdf_dump_query_string, (str(rdf_dump_query)), timestamp])

    url_to_download_dataset = row[6]
    url_to_download_dataset_string = row_containing_headers[6]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], url_to_download_dataset_string, (str(url_to_download_dataset)), timestamp])

    inactive_links = row[7]
    inactive_links_string = row_containing_headers[7]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], inactive_links_string, (str(inactive_links)), timestamp])

    percentage_of_data_updated = row[10]
    percentage_of_data_updated_string = row_containing_headers[10]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], percentage_of_data_updated_string, (str(percentage_of_data_updated)), timestamp])

    availability_for_download_query = row[16]
    availability_for_download_query_string = row_containing_headers[16]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], availability_for_download_query_string, (str(availability_for_download_query)), timestamp])
    
    availability_for_download_metadata = None
    availability_for_download_metadata_string = row_containing_headers[17]

    try:
        availability_for_download_metadata = float(row[17])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], availability_for_download_metadata_string, (str(availability_for_download_metadata) if availability_for_download_metadata == None else f"{availability_for_download_metadata:.6f}"), timestamp])

    average_length_of_subjects_URIs = None
    average_length_of_subjects_URIs_string = row_containing_headers[20]
    
    try:
        average_length_of_subjects_URIs = float(row[20])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], average_length_of_subjects_URIs_string, (str(average_length_of_subjects_URIs) if average_length_of_subjects_URIs == None else f"{average_length_of_subjects_URIs:.6f}"), timestamp])

    standard_deviation_length_of_subjects_URIs = None
    standard_deviation_length_of_subjects_URIs_string = row_containing_headers[21]
    
    try:
        standard_deviation_length_of_subjects_URIs = float(row[21])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], standard_deviation_length_of_subjects_URIs_string, (str(standard_deviation_length_of_subjects_URIs) if standard_deviation_length_of_subjects_URIs == None else f"{standard_deviation_length_of_subjects_URIs:.6f}"), timestamp])

    minimum_length_of_subjects_URIs = None
    minimum_length_of_subjects_URIs_string = row_containing_headers[22]
    
    try:
        minimum_length_of_subjects_URIs = float(row[22])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], minimum_length_of_subjects_URIs_string, (str(minimum_length_of_subjects_URIs) if minimum_length_of_subjects_URIs == None else f"{minimum_length_of_subjects_URIs:.6f}"), timestamp])

    _25th_percentile_length_of_subjects_URIs = None
    _25th_percentile_length_of_subjects_URIs_string = row_containing_headers[23]
    
    try:
        _25th_percentile_length_of_subjects_URIs = float(row[23])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _25th_percentile_length_of_subjects_URIs_string, (str(_25th_percentile_length_of_subjects_URIs) if _25th_percentile_length_of_subjects_URIs == None else f"{_25th_percentile_length_of_subjects_URIs:.6f}"), timestamp])

    median_length_of_subjects_URIs = None
    median_length_of_subjects_URIs_string = row_containing_headers[24]
    
    try:
        median_length_of_subjects_URIs = float(row[24])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], median_length_of_subjects_URIs_string, (str(median_length_of_subjects_URIs) if median_length_of_subjects_URIs == None else f"{median_length_of_subjects_URIs:.6f}"), timestamp])

    _75th_percentile_length_of_subjects_URIs = None
    _75th_percentile_length_of_subjects_URIs_string = row_containing_headers[25]
    
    try:
        _75th_percentile_length_of_subjects_URIs = float(row[25])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _75th_percentile_length_of_subjects_URIs_string, (str(_75th_percentile_length_of_subjects_URIs) if _75th_percentile_length_of_subjects_URIs == None else f"{_75th_percentile_length_of_subjects_URIs:.6f}"), timestamp])

    maximum_length_of_subjects_URIs = None
    maximum_length_of_subjects_URIs_string = row_containing_headers[26]
    try:
        maximum_length_of_subjects_URIs = float(row[26])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], maximum_length_of_subjects_URIs_string, (str(maximum_length_of_subjects_URIs) if maximum_length_of_subjects_URIs == None else f"{maximum_length_of_subjects_URIs:.6f}"), timestamp])

    average_length_of_predicates_URIs = None
    average_length_of_predicates_URIs_string = row_containing_headers[27]
    
    try:
        average_length_of_predicates_URIs = float(row[27])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], average_length_of_predicates_URIs_string, (str(average_length_of_predicates_URIs) if average_length_of_predicates_URIs == None else f"{average_length_of_predicates_URIs:.6f}"), timestamp])

    standard_deviation_length_of_predicates_URIs = None
    standard_deviation_length_of_predicates_URIs_string = row_containing_headers[28]
    
    try:
        standard_deviation_length_of_predicates_URIs = float(row[28])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], standard_deviation_length_of_predicates_URIs_string, (str(standard_deviation_length_of_predicates_URIs) if standard_deviation_length_of_predicates_URIs == None else f"{standard_deviation_length_of_predicates_URIs:.6f}"), timestamp])

    minimum_length_of_predicates_URIs = None
    minimum_length_of_predicates_URIs_string = row_containing_headers[29]
    
    try:
        minimum_length_of_predicates_URIs = float(row[29])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], minimum_length_of_predicates_URIs_string, (str(minimum_length_of_predicates_URIs) if minimum_length_of_predicates_URIs == None else f"{minimum_length_of_predicates_URIs:.6f}"), timestamp])

    _25th_percentile_length_of_predicates_URIs = None
    _25th_percentile_length_of_predicates_URIs_string = row_containing_headers[30]
    
    try:
        _25th_percentile_length_of_predicates_URIs = float(row[30])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _25th_percentile_length_of_predicates_URIs_string, (str(_25th_percentile_length_of_predicates_URIs) if _25th_percentile_length_of_predicates_URIs == None else f"{_25th_percentile_length_of_predicates_URIs:.6f}"), timestamp])

    median_length_of_predicates_URIs = None
    median_length_of_predicates_URIs_string = row_containing_headers[31]
    
    try:
        median_length_of_predicates_URIs = float(row[31])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], median_length_of_predicates_URIs_string, (str(median_length_of_predicates_URIs) if median_length_of_predicates_URIs == None else f"{median_length_of_predicates_URIs:.6f}"), timestamp])

    _75th_percentile_length_of_predicates_URIs = None
    _75th_percentile_length_of_predicates_URIs_string = row_containing_headers[32]
    
    try:
        _75th_percentile_length_of_predicates_URIs = float(row[32])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _75th_percentile_length_of_predicates_URIs_string, (str(_75th_percentile_length_of_predicates_URIs) if _75th_percentile_length_of_predicates_URIs == None else f"{_75th_percentile_length_of_predicates_URIs:.6f}"), timestamp])

    maximum_length_of_predicates_URIs = None
    maximum_length_of_predicates_URIs_string = row_containing_headers[33]

    try:
        maximum_length_of_predicates_URIs = float(row[33])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], maximum_length_of_predicates_URIs_string, (str(maximum_length_of_predicates_URIs) if maximum_length_of_predicates_URIs == None else f"{maximum_length_of_predicates_URIs:.6f}"), timestamp])

    average_length_of_objects_URIs = None
    average_length_of_objects_URIs_string = row_containing_headers[34]
    
    try:
        average_length_of_objects_URIs = float(row[34])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], average_length_of_objects_URIs_string, (str(average_length_of_objects_URIs) if average_length_of_objects_URIs == None else f"{average_length_of_objects_URIs:.6f}"), timestamp])

    standard_deviation_length_of_objects_URIs = None
    standard_deviation_length_of_objects_URIs_string = row_containing_headers[35]
    
    try:
        standard_deviation_length_of_objects_URIs = float(row[35])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], standard_deviation_length_of_objects_URIs_string, (str(standard_deviation_length_of_objects_URIs) if standard_deviation_length_of_objects_URIs == None else f"{standard_deviation_length_of_objects_URIs:.6f}"), timestamp])

    minimum_length_of_objects_URIs = None
    minimum_length_of_objects_URIs_string = row_containing_headers[36]
    
    try:
        minimum_length_of_objects_URIs = float(row[36])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], minimum_length_of_objects_URIs_string, (str(minimum_length_of_objects_URIs) if minimum_length_of_objects_URIs == None else f"{minimum_length_of_objects_URIs:.6f}"), timestamp])

    _25th_percentile_length_of_objects_URIs = None
    _25th_percentile_length_of_objects_URIs_string = row_containing_headers[37]
    
    try:
        _25th_percentile_length_of_objects_URIs = float(row[37])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _25th_percentile_length_of_objects_URIs_string, (str(_25th_percentile_length_of_objects_URIs) if _25th_percentile_length_of_objects_URIs == None else f"{_25th_percentile_length_of_objects_URIs:.6f}"), timestamp])

    median_length_of_objects_URIs = None
    median_length_of_objects_URIs_string = row_containing_headers[38]
    
    try:
        median_length_of_objects_URIs = float(row[38])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], median_length_of_objects_URIs_string, (str(median_length_of_objects_URIs) if median_length_of_objects_URIs == None else f"{median_length_of_objects_URIs:.6f}"), timestamp])

    _75th_percentile_length_of_objects_URIs = None
    _75th_percentile_length_of_objects_URIs_string = row_containing_headers[39]
    
    try:
        _75th_percentile_length_of_objects_URIs = float(row[39])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], _75th_percentile_length_of_objects_URIs_string, (str(_75th_percentile_length_of_objects_URIs) if _75th_percentile_length_of_objects_URIs == None else f"{_75th_percentile_length_of_objects_URIs:.6f}"), timestamp])

    maximum_length_of_objects_URIs = None
    maximum_length_of_objects_URIs_string = row_containing_headers[40]
    
    try:
        maximum_length_of_objects_URIs = float(row[40])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], maximum_length_of_objects_URIs_string, (str(maximum_length_of_objects_URIs) if maximum_length_of_objects_URIs == None else f"{maximum_length_of_objects_URIs:.6f}"), timestamp])

    external_links = row[69]
    external_links_string = row_containing_headers[69]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], external_links_string, str(external_links), timestamp])

    dataset_url = row[72]
    dataset_url_string = row_containing_headers[72]
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], dataset_url_string, (str(dataset_url)), timestamp])

    is_on_trusted_providers_list = row[73]
    is_on_trusted_providers_list_string = row_containing_headers[73]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], is_on_trusted_providers_list_string, (str(is_on_trusted_providers_list)), timestamp])

    trust = None
    trust_string = row_containing_headers[74]

    try:
        trust = float(row[74])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], trust_string, (str(trust) if trust == None else f"{trust:.6f}"), timestamp])

    used_vocabolaries = row[75]
    used_vocabolaries_string = row_containing_headers[75]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], used_vocabolaries_string, (str(used_vocabolaries)), timestamp])

    number_of_triples_linked = None
    number_of_triples_linked_string = row_containing_headers[83]
    
    try:
        number_of_triples_linked = float(row[83])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], number_of_triples_linked_string, (str(number_of_triples_linked) if number_of_triples_linked == None else f"{number_of_triples_linked:.6f}"), timestamp])
    
    percentage_of_triples_with_labels = None
    percentage_of_triples_with_labels_string = row_containing_headers[88]
    
    try:
        percentage_of_triples_with_labels = float(row[88])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], percentage_of_triples_with_labels_string, (str(percentage_of_triples_with_labels) if percentage_of_triples_with_labels == None else f"{percentage_of_triples_with_labels:.6f}"), timestamp])

    number_of_blanks_nodes = None
    number_of_blanks_nodes_string = row_containing_headers[91]
    try:
        number_of_blanks_nodes = float(row[91])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], number_of_blanks_nodes_string, (str(number_of_blanks_nodes) if number_of_blanks_nodes == None else f"{number_of_blanks_nodes:.6f}"), timestamp])

    use_of_RDF_structures = row[92]
    use_of_RDF_structures_string = row_containing_headers[92]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], use_of_RDF_structures_string, (str(use_of_RDF_structures)), timestamp])

    deprecated_classes_or_properties_used = None
    deprecated_classes_or_properties_used_string = row_containing_headers[93]

    try:
        deprecated_classes_or_properties_used = float(row[93])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], deprecated_classes_or_properties_used_string, (str(deprecated_classes_or_properties_used) if deprecated_classes_or_properties_used == None else f"{deprecated_classes_or_properties_used:.6f}"), timestamp])

    score = None
    score_string = row_containing_headers[108]

    try:
        score = float(row[108])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], score_string, (str(score) if score == None else f"{score:.6f}"), timestamp])
    
    normalized_score = None
    normalized_score_string = row_containing_headers[109]
    
    try:
        normalized_score = float(row[109])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], normalized_score_string, (str(normalized_score) if normalized_score == None else f"{normalized_score:.6f}"), timestamp])
    
    limited = row[110]
    limited_string = row_containing_headers[110]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], limited_string, (str(limited)), timestamp])

    offline_dumps = row[111]
    offline_dumps_string = row_containing_headers[111]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], offline_dumps_string, (str(offline_dumps)), timestamp])

    url_file_VOID = row[112]
    url_file_VOID_string = row_containing_headers[112]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], url_file_VOID_string, (str(url_file_VOID)), timestamp])

    availability_VOID_file = row[113]
    availability_VOID_file_string = row_containing_headers[113]

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], availability_VOID_file_string, (str(availability_VOID_file)), timestamp])
    
    minTPnoOff = None
    minTPnoOff_string = row_containing_headers[114]

    try:
        minTPnoOff = float(row[114])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], minTPnoOff_string, (str(minTPnoOff) if minTPnoOff == None else f"{minTPnoOff:.6f}"), timestamp])

    meanTPnoOff = None
    meanTPnoOff_string = row_containing_headers[115]
    
    try:
        meanTPnoOff = float(row[115])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], meanTPnoOff_string, (str(meanTPnoOff) if meanTPnoOff == None else f"{meanTPnoOff:.6f}"), timestamp])

    maxTPnoOff = None
    maxTPnoOff_string = row_containing_headers[116]
    
    try:
        maxTPnoOff = float(row[116])
    except (ValueError, TypeError):
        pass
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], maxTPnoOff_string, (str(maxTPnoOff) if maxTPnoOff == None else f"{maxTPnoOff:.6f}"), timestamp])

    sdTPnoOff = None
    sdTPnoOff_string = row_containing_headers[117]

    try:
        sdTPnoOff = float(row[117])
    except (ValueError, TypeError):
        pass

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], sdTPnoOff_string, (str(sdTPnoOff) if sdTPnoOff == None else f"{sdTPnoOff:.6f}"), timestamp])

    availability_score = None
    availability_score_string = row_containing_headers[119]

    try:
        availability_score = float(row[119])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], availability_score_string, (str(availability_score) if availability_score == None else f"{availability_score:.6f}"), timestamp])

    licensing_score = None
    licensing_score_string = row_containing_headers[120]

    try:
        licensing_score = float(row[120])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], licensing_score_string, (str(licensing_score) if licensing_score == None else f"{licensing_score:.6f}"), timestamp])

    interlinking_score = None
    interlinking_score_string = row_containing_headers[121]

    try:
        interlinking_score = float(row[121])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], interlinking_score_string, (str(interlinking_score) if interlinking_score == None else f"{interlinking_score:.6f}"), timestamp])

    performance_score = None
    performance_score_string = row_containing_headers[122]

    try:
        performance_score = float(row[122])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], performance_score_string, (str(performance_score) if performance_score == None else f"{performance_score:.6f}"), timestamp])

    accuracy_score = None
    accuracy_score_string = row_containing_headers[123]
    
    try:
        accuracy_score = float(row[123])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], accuracy_score_string, (str(accuracy_score) if accuracy_score == None else f"{accuracy_score:.6f}"), timestamp])

    consistency_score = None
    consistency_score_string = row_containing_headers[124]
    
    try:
        consistency_score = float(row[124])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], consistency_score_string, (str(consistency_score) if consistency_score == None else f"{consistency_score:.6f}"), timestamp])

    conciseness_score = None
    conciseness_score_string = row_containing_headers[125]

    try:
        conciseness_score = float(row[125])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], conciseness_score_string, (str(conciseness_score) if conciseness_score == None else f"{conciseness_score:.6f}"), timestamp])

    verifiability_score = None
    verifiability_score_string = row_containing_headers[126]
    
    try:
        verifiability_score = float(row[126])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], verifiability_score_string, (str(verifiability_score) if verifiability_score == None else f"{verifiability_score:.6f}"), timestamp])

    reputation_score = None
    reputation_score_string = row_containing_headers[127]

    try:
        reputation_score = float(row[127])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], reputation_score_string, (str(reputation_score) if reputation_score == None else f"{reputation_score:.6f}"), timestamp])

    currency_score = None
    currency_score_string = row_containing_headers[129]

    try:
        currency_score = float(row[129])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], currency_score_string, (str(currency_score) if currency_score == None else f"{currency_score:.6f}"), timestamp])

    volatility_score = None
    volatility_score_string = row_containing_headers[130]

    try:
        volatility_score = float(row[130])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], volatility_score_string, (str(volatility_score) if volatility_score == None else f"{volatility_score:.6f}"), timestamp])

    completeness_score = None
    completeness_score_string = row_containing_headers[131]

    try:
        completeness_score = float(row[131])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], completeness_score_string, (str(completeness_score) if completeness_score == None else f"{completeness_score:.6f}"), timestamp])

    amount_of_data_score = None
    amount_of_data_score_string = row_containing_headers[132]

    try:
        amount_of_data_score = float(row[132])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], amount_of_data_score_string, (str(amount_of_data_score) if amount_of_data_score == None else f"{amount_of_data_score:.6f}"), timestamp])

    representational_consistency_score = None
    representational_consistency_score_string = row_containing_headers[133]

    try:
        representational_consistency_score = float(row[133])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], representational_consistency_score_string, (str(representational_consistency_score) if representational_consistency_score == None else f"{representational_consistency_score:.6f}"), timestamp])

    representational_conciseness_score = None
    representational_conciseness_score_string = row_containing_headers[134]

    try:
        representational_conciseness_score = float(row[134])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], representational_conciseness_score_string, (str(representational_conciseness_score) if representational_conciseness_score == None else f"{representational_conciseness_score:.6f}"), timestamp])

    understandability_score = None
    understandability_score_string = row_containing_headers[135]

    try:
        understandability_score = float(row[135])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], understandability_score_string, (str(understandability_score) if understandability_score == None else f"{understandability_score:.6f}"), timestamp])

    interpretability_score = None
    interpretability_score_string = row_containing_headers[136]

    try:
        interpretability_score = float(row[136])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], interpretability_score_string, (str(interpretability_score) if interpretability_score == None else f"{interpretability_score:.6f}"), timestamp])

    versatility_score = None
    versatility_score_string = row_containing_headers[137]

    try:
        versatility_score = float(row[137])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], versatility_score_string, (str(versatility_score) if versatility_score == None else f"{versatility_score:.6f}"), timestamp])

    security_score = None
    security_score_string = row_containing_headers[138]

    try:
        security_score = float(row[138])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], security_score_string, (str(security_score) if security_score == None else f"{security_score:.6f}"), timestamp])

    u1_value = None
    u1_value_string = row_containing_headers[140]

    try:
        u1_value = float(row[140])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], u1_value_string, (str(u1_value) if u1_value == None else f"{u1_value:.6f}"), timestamp])

    cs2_value = None
    cs2_value_string = row_containing_headers[141]

    try:
        cs2_value = float(row[141])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], cs2_value_string, (str(cs2_value) if cs2_value == None else f"{cs2_value:.6f}"), timestamp])

    IN3_value = None
    IN3_value_string = row_containing_headers[142]
    
    try:
        IN3_value = float(row[142])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], IN3_value_string, (str(IN3_value) if IN3_value == None else f"{IN3_value:.6f}"), timestamp])

    RC1_value = None
    RC1_value_string = row_containing_headers[143]

    try:
        RC1_value = float(row[143])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], RC1_value_string, (str(RC1_value) if RC1_value == None else f"{RC1_value:.6f}"), timestamp])

    RC2_value = None
    RC2_value_string = row_containing_headers[144]

    try:
        RC2_value = float(row[144])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], RC2_value_string, (str(RC2_value) if RC2_value == None else f"{RC2_value:.6f}"), timestamp])

    IN4_value = None
    IN4_value_string = row_containing_headers[145]

    try:
        IN4_value = float(row[145])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], IN4_value_string, (str(IN4_value) if IN4_value == None else f"{IN4_value:.6f}"), timestamp])

    metadata_media_type = None
    metadata_media_type_string = row_containing_headers[146]

    try:
        metadata_media_type = row[146]
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], metadata_media_type_string, (str(metadata_media_type)), timestamp])

    common_accepted_media_type_availability = None
    common_accepted_media_type_availability_string = row_containing_headers[147]

    try:
        common_accepted_media_type_availability = row[147]
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], common_accepted_media_type_availability_string, (str(common_accepted_media_type_availability)), timestamp])

    U5_value = None
    U5_value_string = row_containing_headers[148]

    try:
        U5_value = float(row[148])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], U5_value_string, (str(U5_value) if U5_value == None else f"{U5_value:.6f}"), timestamp])

    PE2_value = None
    PE2_value_string = row_containing_headers[149]

    try:
        PE2_value = float(row[149])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], PE2_value_string, (str(PE2_value) if PE2_value == None else f"{PE2_value:.6f}"), timestamp])

    PE3_value = None
    PE3_value_string = row_containing_headers[150]

    try:
        PE3_value = float(row[150])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], PE3_value_string, (str(PE3_value) if PE3_value == None else f"{PE3_value:.6f}"), timestamp])

    F1_M_unique_and_persistend_Id = None
    F1_M_unique_and_persistend_Id_string = row_containing_headers[151]

    try:
        F1_M_unique_and_persistend_Id = float(row[151])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F1_M_unique_and_persistend_Id_string, (str(F1_M_unique_and_persistend_Id) if F1_M_unique_and_persistend_Id == None else f"{F1_M_unique_and_persistend_Id:.6f}"), timestamp])

    F1_D_URIs_dereferenceability = None
    F1_D_URIs_dereferenceability_string = row_containing_headers[152]

    try:
        F1_D_URIs_dereferenceability = float(row[152])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F1_D_URIs_dereferenceability_string, (str(F1_D_URIs_dereferenceability) if F1_D_URIs_dereferenceability == None else f"{F1_D_URIs_dereferenceability:.6f}"), timestamp])

    F2a_M_Metadata_availability_via_standard_primary_sources = None
    F2a_M_Metadata_availability_via_standard_primary_sources_string = row_containing_headers[153]

    try:
        F2a_M_Metadata_availability_via_standard_primary_sources = float(row[153])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F2a_M_Metadata_availability_via_standard_primary_sources_string, (str(F2a_M_Metadata_availability_via_standard_primary_sources) if F2a_M_Metadata_availability_via_standard_primary_sources == None else f"{F2a_M_Metadata_availability_via_standard_primary_sources:.6f}"), timestamp])

    F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score = None
    F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score_string = row_containing_headers[154]

    try:
        F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score = float(row[154])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score_string, (str(F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score) if F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score == None else f"{F2b_M_Metadata_availability_attributes_covered_in_FAIR_Score:.6f}"), timestamp])

    F3_M_data_deferrable_via_DOI = None
    F3_M_data_deferrable_via_DOI_string = row_containing_headers[155]

    try:
        F3_M_data_deferrable_via_DOI = float(row[155])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F3_M_data_deferrable_via_DOI_string, (str(F3_M_data_deferrable_via_DOI) if F3_M_data_deferrable_via_DOI == None else f"{F3_M_data_deferrable_via_DOI:.6f}"), timestamp])

    F4_M_metadata_registered_In_searchable_engine = None
    F4_M_metadata_registered_In_searchable_engine_string = row_containing_headers[156]

    try:
        F4_M_metadata_registered_In_searchable_engine = float(row[156])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F4_M_metadata_registered_In_searchable_engine_string, (str(F4_M_metadata_registered_In_searchable_engine) if F4_M_metadata_registered_In_searchable_engine == None else f"{F4_M_metadata_registered_In_searchable_engine:.6f}"), timestamp])

    F_score = None
    F_score_string = row_containing_headers[157]

    try:
        F_score = float(row[157])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], F_score_string, (str(F_score) if F_score == None else f"{F_score:.6f}"), timestamp])

    A1_D_working_access_points = None
    A1_D_working_access_points_string = row_containing_headers[158]

    try:
        A1_D_working_access_points = float(row[158])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges
    
    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], A1_D_working_access_points_string, (str(A1_D_working_access_points) if A1_D_working_access_points == None else f"{A1_D_working_access_points:.6f}"), timestamp])

    A1_M_Metadata_available_via_working_primary_sources = None
    A1_M_Metadata_available_via_working_primary_sources_string = row_containing_headers[159]

    try:
        A1_M_Metadata_available_via_working_primary_sources = float(row[159])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], A1_M_Metadata_available_via_working_primary_sources_string, (str(A1_M_Metadata_available_via_working_primary_sources) if A1_M_Metadata_available_via_working_primary_sources == None else f"{A1_M_Metadata_available_via_working_primary_sources:.6f}"), timestamp])

    A1_2_authentication_and_HTTPS_support = None
    A1_2_authentication_and_HTTPS_support_string = row_containing_headers[160]

    try:
        A1_2_authentication_and_HTTPS_support = float(row[160])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], A1_2_authentication_and_HTTPS_support_string, (str(A1_2_authentication_and_HTTPS_support) if A1_2_authentication_and_HTTPS_support == None else f"{A1_2_authentication_and_HTTPS_support:.6f}"), timestamp])

    A2_M_registered_in_search_engines = None
    A2_M_registered_in_search_engines_string = row_containing_headers[161]

    try:
        A2_M_registered_in_search_engines = float(row[161])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], A2_M_registered_in_search_engines_string, (str(A2_M_registered_in_search_engines) if A2_M_registered_in_search_engines == None else f"{A2_M_registered_in_search_engines:.6f}"), timestamp])

    A_score = None
    A_score_string = row_containing_headers[162]

    try:
        A_score = float(row[162])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], A_score_string, (str(A_score) if A_score == None else f"{A_score:.6f}"), timestamp])

    R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source = None
    R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source_string = row_containing_headers[163]

    try:
        R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source = float(row[163])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source_string, (str(R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source) if R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source == None else f"{R1_1_machine_or_human_readable_license_retrievable_via__any_primary_source:.6f}"), timestamp])

    R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources = None
    R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources_string = row_containing_headers[164]

    try:
        R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources = float(row[164])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources_string, (str(R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources) if R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources == None else f"{R1_2_publisher_information_such_as_authors_contributors_publishers_and_sources:.6f}"), timestamp])

    R1_3_D_data_organized_in_standardized_way = None
    R1_3_D_data_organized_in_standardized_way_string = row_containing_headers[165]

    try:
        R1_3_D_data_organized_in_standardized_way = float(row[165])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], R1_3_D_data_organized_in_standardized_way_string, (str(R1_3_D_data_organized_in_standardized_way) if R1_3_D_data_organized_in_standardized_way == None else f"{R1_3_D_data_organized_in_standardized_way:.6f}"), timestamp])

    R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties = None
    R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties_string = row_containing_headers[166]

    try:
        R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties = float(row[166])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties_string, (str(R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties) if R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties == None else f"{R1_3_M_Metadata_description_thorugh_VOID_or_DCAT_properties:.6f}"), timestamp])

    R_score = None
    R_score_string = row_containing_headers[167]

    try:
        R_score = float(row[167])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], R_score_string, (str(R_score) if R_score == None else f"{R_score:.6f}"), timestamp])

    I1_D_standard_and_open_representation_format = None
    I1_D_standard_and_open_representation_format_string = row_containing_headers[168]

    try:
        I1_D_standard_and_open_representation_format = float(row[168])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], I1_D_standard_and_open_representation_format_string, (str(I1_D_standard_and_open_representation_format) if I1_D_standard_and_open_representation_format == None else f"{I1_D_standard_and_open_representation_format:.6f}"), timestamp])

    I1_M_Metadata_description_through_VOID_or_DCAT_properties = None
    I1_M_Metadata_description_through_VOID_or_DCAT_properties_string = row_containing_headers[169]

    try:
        I1_M_Metadata_description_through_VOID_or_DCAT_properties = float(row[169])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], I1_M_Metadata_description_through_VOID_or_DCAT_properties_string, (str(I1_M_Metadata_description_through_VOID_or_DCAT_properties) if I1_M_Metadata_description_through_VOID_or_DCAT_properties == None else f"{I1_M_Metadata_description_through_VOID_or_DCAT_properties:.6f}"), timestamp])

    I2_use_of_FAIR_vocabularies = None
    I2_use_of_FAIR_vocabularies_string = row_containing_headers[170]

    try:
        I2_use_of_FAIR_vocabularies = float(row[170])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], I2_use_of_FAIR_vocabularies_string, (str(I2_use_of_FAIR_vocabularies) if I2_use_of_FAIR_vocabularies == None else f"{I2_use_of_FAIR_vocabularies:.6f}"), timestamp])

    I3_D_degree_of_connection = None
    I3_D_degree_of_connection_string = row_containing_headers[171]

    try:
        I3_D_degree_of_connection = float(row[171])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], I3_D_degree_of_connection_string, (str(I3_D_degree_of_connection) if I3_D_degree_of_connection == None else f"{I3_D_degree_of_connection:.6f}"), timestamp])

    I_score = None
    I_score_string = row_containing_headers[172]

    try:
        I_score = float(row[172])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], I_score_string, (str(I_score) if I_score == None else f"{I_score:.6f}"), timestamp])

    FAIR_score = None
    FAIR_score_string = row_containing_headers[173]

    try:
        FAIR_score = float(row[173])
    except (ValueError, TypeError):
        pass
    except (IndexError):
        # It means that this field is not present in most ancient CSV
        return residual_edges

    residual_edges[counter] = return_correct_structure_ordering(row, metrics_for_each_dataset, structure_type, [row[0], FAIR_score_string, (str(FAIR_score) if FAIR_score == None else f"{FAIR_score:.6f}"), timestamp])

    #END OF THE RESIDUAL FIELDS

    return residual_edges


def add_KG_data(row_containing_headers, row, timestamp,  metrics_for_each_dataset, structure_type = 0):

    if row[1] not in metrics_for_each_dataset:
        metrics_for_each_dataset[row[0]] = {}
    
    print(row[1])

    edges = {}

    #Availability Metrics
    edges.update(add_availability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))
    
    #Licensing Metrics
    edges.update(add_licensing(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Interlinking Metrics
    edges.update(add_interlinking(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Security Metrics
    edges.update(add_security(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Performance Metrics
    edges.update(add_performance(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Semantic Accuracy Metrics
    edges.update(add_semantic_accuracy(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Consistency Metrics
    edges.update(add_consistency(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Conciseness Metrics
    edges.update(add_conciseness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Reputation Metrics
    edges.update(add_reputation(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Believability Metrics
    edges.update(add_believability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Verifiability Metrics
    edges.update(add_verifiability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Currency Metrics
    edges.update(add_currency(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Timeliness Metrics
    edges.update(add_timeliness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Completeness Metrics
    edges.update(add_completeness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Amount of data Metrics
    edges.update(add_amount_of_data(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Representational Conciseness Metrics
    edges.update(add_representational_conciseness(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Interoperability Metrics
    edges.update(add_interoperability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Understandability Metrics
    edges.update(add_understandability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Interpretability Metrics
    edges.update(add_interpretability(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Versatility Metrics
    edges.update(add_versatility(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    #Residual Fields
    edges.update(add_residual_fields(row_containing_headers, row, timestamp, structure_type, metrics_for_each_dataset))

    return edges