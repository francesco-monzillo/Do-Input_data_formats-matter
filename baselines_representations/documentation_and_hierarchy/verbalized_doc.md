# Knowledge Graph Quality Assessment – Full Metrics Documentation

This document describes all metrics implemented in the Knowledge Graph Quality Assessment framework.  
Each metric includes its **description**, **input requirements**, and **possible outputs**.  

---

toremove ## Table of Contents - Quality dimensions
- [Availability](#availability)  
- [Security](#security)  
- [Licensing](#licensing)
- [Interlinking](#interlinking)  
- [Performance](#performance)  
- [Accuracy](#accuracy)  
- [Consistency](#consistency)  
- [Conciseness](#conciseness)  
- [Reputation](#reputation)  
- [Believability](#believability)  
- [Verifiability](#verifiability)   
- [Currency](#currency)
- [Timeliness](#timeliness)
- [Completeness](#completeness)
- [Amount of data](#amount-of-data)
- [Versatility](#versatility)
- [Representational conciseness](#representational-conciseness)
- [Understandability](#understandability)
- [Interoperability](#interoperability)
- [Interpretability](#interpretability)
- [Score](#score)  

---

toremove # Accessibility category

toremove ## Availability

### Sparql endpoint
- **Description:** Checking whether the server responds to a SPARQL query
- **Input:** Metadata
- **Output:** Offline: if the sparql endpoint is not online; Available: if SPARQL endpoint is online.; -: The SPARQL endpoint is missing.

### Availability of RDF dump (metadata)
- **Description:** Checking whether an RDF dump is provided and can be downloaded
- **Input:** Metadata or VoID file
- **Output:** 0: The RDF dump is offline; 1: The RDF dump is online. -1: The RDF dump is missing.

### Availability of RDF dump (query)
- **Description:** Checking whether an RDF dump is provided and can be downloaded
- **Input:** (working) SPARQL endpoint
- **Output:** False: The RDF dump is offline; True: The RDF dump is online. -: The RDF dump is missing.

### Availability for download (query)
- **Description:** Checking whether an RDF dump is provided and can be downloaded
- **Input:** (working) SPARQL endpoint
- **Output:** False: The RDF dump is offline; True: The RDF dump is online. -: The RDF dump is missing.

### Availability for download (metadata)
- **Description:** Checking whether an RDF dump is provided and can be downloaded
- **Input:** Metadata or VoID file
- **Output:** 0: The RDF dump is offline; 1: The RDF dump is online. -1: The RDF dump is missing.

### URIs Deferenceability
- **Description:** HTTP URIs should be dereferenceable, i.e. HTTP clients should be able to retrieve the resources identified by the URI
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Availability score
- **Description:** Overall score for the availability dimension computed as a linear combination of the individual availability metrics scores
- **Input:** Availability metrics scores
- **Output:** [0,1]. Best value: 1.

toremove ## Security

### Use HTTPS
- **Description:** HTTPS support
- **Input:** (working) SPARQL endpoint
- **Output:** False: Does not use HTTPS; True: Uses HTTPS; -: The SPARQL endpoint is missing.

### Requires authentication
- **Description:** Use of login credentials (or use of SSL or SSH)
- **Input:** (working) SPARQL endpoint
- **Output:** False: Authentication is not required; True: authentication required; -: The SPARQL endpoint is missing.

### Security score
- **Description:** Overall score for the security dimension computed as a linear combination of the individual security metrics scores
- **Input:** Security metrics scores
- **Output:** [0,1]. Best value: 1.

toremove ## Licensing

### License machine redeable (metadata)
- **Description:** Detection of the indication of a license in the VoID description or in the dataset itself
- **Input:** VoID file or metadata
- **Output:** string: the license if it can be recovered; False: the license is not indicated

### License machine redeable (query)
- **Description:** Detection of the indication of a license in the VoID description or in the dataset itself
- **Input:** (working) SPARQL endpoint
- **Output:** string: the license if it can be recovered; -: the license is not indicated or the SPARQL endpoint is missing.

### License human redeable
- **Description:** Detection of a license in the documentation of the dataset
- **Input:** (working) SPARQL endpoint
- **Output:** True: the human readable license is indicated; False: the human readable license is not indicated; -: The SPARQL endpoint is missing.

### Licensing score
- **Description:** Overall score for the licensing dimension computed as a linear combination of the individual licensing metrics scores
- **Input:** Licensing metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Performance

### Median latency
- **Description:** If an HTTP-request is not answered within an average time of one second, the latency of the data source is considered too low
- **Input:** (working) SPARQL endpoint
- **Output:** Median latency in milliseconds. Best value: <1000 ms

### Median throughput
- **Description:** No. of answered HTTP-requests per second
- **Input:** (working) SPARQL endpoint
- **Output:** # of requests answered per second. Best value: Higher is better

### Performance score
- **Description:** Overall score for the performance dimension computed as a linear combination of the individual performance metrics scores
- **Input:** Performance metrics scores
- **Output:** [0,1]. Best value: 1.

toremove ## Interlinking

### Degree of connection
- **Description:** Detection of (a) interlinking degree, (b) clustering coefficient, (c) centrality, (d) open sameAs chains and (e) description richness through sameAs by using network measures
- **Input:** Metadata
- **Output:** integer: the degree of connection; -: if the dataset is not interlinked.

### Clustering coefficient
- **Description:** Detection of (a) interlinking degree, (b) clustering coefficient, (c) centrality, (d) open sameAs chains and (e) description richness through sameAs by using network measures
- **Input:** Metadata
- **Output:** float: the clustering coefficient; {}: if the dataset is not interlinked.

### Centrality
- **Description:** Detection of (a) interlinking degree, (b) clustering coefficient, (c) centrality, (d) open sameAs chains and (e) description richness through sameAs by using network measures
- **Input:** Metadata
- **Output:** float: the centrality; empty: if the dataset is not interlinked.

### Number of samAs chains
- **Description:** Detection of (a) interlinking degree, (b) clustering coefficient, (c) centrality, (d) open sameAs chains and (e) description richness through sameAs by using network measures
- **Input:** Metadata
- **Output:** False: no sameAs chains in the dataset; -: the SPARQL endpoint is missing; integer: number of sameAs chains in the dataset.

### SKOS mapping properties
- **Description:** skos:closeMatch | skos:exactMatch | skos:broadMatch | skos:narrowMatch | skos:relatedMatch
- **Input:** (working) SPARQL endpoint
- **Output:** integer: the number of mapping properties; -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query. False: if no mapping property is present.

### Interlinking score
- **Description:** Overall score for the interlinking dimension computed as a linear combination of the individual interlinking metrics scores
- **Input:** Interlinking metrics scores
- **Output:** [0,1]. Best value: 1.

---


toremove # Intrinsic category

toremove ## Accuracy

### Triples with empty annotation problem
- **Description:** Labels, comments, notes which identifies triples whose property's object value is empty string
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Triples with white space in annotation (at the beginning or at the end)
- **Description:** Presence of white space in labels
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Triples with malformed data type literals problem
- **Description:** Incompatible with data type range
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Functional properties with inconsistent values
- **Description:** FP = 1 - num of triples with inconsistent values for functional properties / num of triples
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Invalid usage of inverse-functional properties
- **Description:** IFP = 1 - num of triples with inconsistent values for functional properties / num of triples
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.


### Accuracy score
- **Description:** Overall score for the accuracy dimension computed as a linear combination of the individual accuracy metrics scores
- **Input:** Accuracy metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Consistency

### Deprecated classes/properties used
- **Description:** Detection of use of OWL classes owl:DeprecatedClass and owl:DeprecatedProperty
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online.

### Entities as member of disjoint class
- **Description:** No. of entities described as members of disjoint classes / total no. of entities described in the dataset
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Triples with misplaced property problem
- **Description:** Detection of a URI defined as a class is used as a property or a URI defined as a property is used as a class
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query. insufficient data: if the property cannot be recovered.

### Triples with misplaced class problem
- **Description:** Detection of a URI defined as a class is used as a property or a URI defined as a property is used as a class
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query. insufficient data: if the class cannot be recovered.

### Ontology Hijacking problem
- **Description:** Detection of the redefinition by third parties of external classes/properties such that reasoning over data using those external terms is affected
- **Input:** (working) SPARQL endpoint
- **Output:** True: if ontology hijacking problem is detected; False: if ontology hijacking problem is not detected; -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Undefined class used without declaration
- **Description:** Detection of classes and properties used without any formal definition
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query. insufficient data: if the class cannot be recovered.

### Undefined properties used without declaration
- **Description:** Detection of classes and properties used without any formal definition
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query. insufficient data: if the property cannot be recovered.

### Consistency score
- **Description:** Overall score for the consistency dimension computed as a linear combination of the individual consistency metrics scores
- **Input:** Consistency metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Conciseness

### Extensional conciseness
- **Description:** Measure any redundancy between all the triples of the KG
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Intensional conciseness
- **Description:** Check if there are any duplicate properties within the dataset
- **Input:** (working) SPARQL endpoint
- **Output:** [0,1]. Best value: 1. -: if the SPARQL endpoint is missing or not online or error during the execution of the sparql query.

### Conciseness score
- **Description:** Overall score for the conciseness dimension computed as a linear combination of the individual conciseness metrics scores
- **Input:** Conciseness metrics scores
- **Output:** [0,1]. Best value: 1.
---


toremove # Trust category

toremove ## Reputation

### PageRank
- **Description:** Analyzing page rank of the dataset
- **Input:** Metadata
- **Output:** float: the page rank; empty: if the dataset is not interlinked.

### Reputation score
- **Description:** Overall score for the reputation dimension computed as a linear combination of the individual reputation metrics scores
- **Input:** Reputation metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Believability

### Trust value
- **Description:** Meta-information about the identity of information provider. It is calculated as a weighted average based on the presence of 4 key values for identifying provider information, these key values are KG name (from metadata, VoID file or SPARQL endpoint), Description (from metadata, VoID file or SPARQL endpoint), Provider URL (from metadata, VoID file or SPARQL endpoint), Check if a provider is in a list of reliable providers
- **Input:** Metadata
- **Output:** float: the trust value;

### Believability score
- **Description:** Overall score for the believability dimension computed as a linear combination of the individual believability metrics scores
- **Input:** Believability metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Verifiability

### Author (query)
- **Description:** Stating the author and his contributors, the publisher of the data and its sources
- **Input:** (working) SPARQL endpoint
- **Output:** []: if the author is not indicated; -: the SPARQL endpoint is missing; list of authors: if the author is indicated.

### Author (metadata)
- **Description:** Stating the author and his contributors, the publisher of the data and its sources
- **Input:** VoID file or metadata
- **Output:** False: if the author is not indicated; string with authors: if the author is indicated.

### Contributor
- **Description:** Stating the author and his contributors, the publisher of the data and its sources
- **Input:** (working) SPARQL endpoint or VoID file
- **Output:** []: if the contributors is not indicated; -: the SPARQL endpoint is missing; list of contributors: if the contributors is indicated.

### Publisher
- **Description:** Stating the author and his contributors, the publisher of the data and its sources
- **Input:** (working) SPARQL endpoint or VoID file
- **Output:** []: if the publisher is not indicated; -: the SPARQL endpoint is missing; list of publishers: if the publisher is indicated.

### Sources
- **Description:** Stating the author and his contributors, the publisher of the data and its sources
- **Input:** Metadata
- **Output:** string: the website, the name and the email of the source; False: if the sources are not indicated.

### Signed
- **Description:** Checking whether the dataset is signed
- **Input:** Metadata
- **Output:** boolean: true if the dataset is signed; false: if the dataset is not signed.


### Verifiability score
- **Description:** Overall score for the verifiability dimension computed as a linear combination of the individual verifiability metrics scores
- **Input:** Verifiability metrics scores
- **Output:** [0,1]. Best value: 1.

---

toremove # Dataset dynamicity category

toremove ## Currency

### Age of data
- **Description:** current time - created time
- **Input:** (working) SPARQL endpoint or VoID file
- **Output:** -: if the creation date can't be recovered; integer: if the creation date is correctly recovered

### Modification date
- **Description:** Use of dates as the point in time of the last verification of a statement represented by dcterms:modified
- **Input:** (working) SPARQL endpoint or VoID file
- **Output:** -: if the modification date can't be retrieved; date: if the modification date is correctly retrieved

### Time elapsed since last modification
- **Description:** Currency only measures the time since the last modification
- **Input:** (working) SPARQL endpoint or VoID file
- **Output:** -: if the modification date is not correctly retrieved or not indicated; an integer: if the modification date is correctly retrieved

### Currency score
- **Description:** Overall score for the currency dimension computed as a linear combination of the individual currency metrics scores
- **Input:** Currency metrics scores
- **Output:** [0,1]. Best value: 1.

toremove ## Timeliness

### Dataset update frequency
- **Description:** It corresponds to the 'stating the [...] frequency of data validation'
- **Input:** (working) SPARQL endpoint
- **Output:** []: the update frequency is not indicated; -: the SPARQL endpoint is missing; list of strings: the update frequency is indicated.

### Timeliness score
- **Description:** Overall score for the timeliness dimension computed as a linear combination of the individual timeliness metrics scores
- **Input:** Timeliness metrics scores
- **Output:** [0,1]. Best value: 1.

---

toremove # Contextual category

toremove ## Completeness

### Interlinking completeness
- **Description:** Degree to which interlinks are not missing
- **Input:** Metadata
- **Output:** [0,1]. Best value: 1. insufficient data: if the interlinking completeness can't be calculated.

### Completeness score
- **Description:** Overall score for the completeness dimension computed as a linear combination of the individual completeness metrics scores
- **Input:** Completeness metrics scores
- **Output:** [0,1]. Best value: 1.

toremove ## Amount of Data

### Number of triples (metadata)
- **Description:** Number of triples
- **Input:** VoID file or metadata
- **Output:** integer: the number of triples; -: if the number of triples can't be retrieved

### Number of triples (query)
- **Description:** Number of triples
- **Input:** (working) SPARQL endpoint
- **Output:** integer: the number of triples; -: The SPARQL endpoint is missing.

### Number of entities
- **Description:** Number of entities
- **Input:** VoID file or (working) SPARQL endpoint
- **Output:** integer: the number of entities; -: if the number of entities can't be retrieved or the SPARQL endpoint is missing.

### Number of entities counted with regex
- **Description:** Number of entities
- **Input:** VoID file or (working) SPARQL endpoint
- **Output:** number: the number of entities; -: if the number of entities can't be retrieved or the SPARQL endpoint is missing.

### Number of property
- **Description:** Number of properties
- **Input:** VoID file or (working) SPARQL endpoint
- **Output:** integer: the number of properties; -: if the number of properties can't be retrieved or the SPARQL endpoint is missing.

### Amount of data score
- **Description:** Overall score for the amount of data dimension computed as a linear combination of the individual amount of data metrics scores
- **Input:** Amount of data metrics scores
- **Output:** [0,1]. Best value: 1.

---

toremove # Representational category

toremove ## Versatility

### Languages (query)
- **Description:** Checking whether data is available in different languages
- **Input:** (working) SPARQL endpoint
- **Output:** False: if no lang tag is retrieved; list of langs: if at least a lang tag is retrieved; -: if the SPARQL endpoint is missing.

### Languages (metadata)
- **Description:** Checking whether metadata is available in different languages
- **Input:** VoID file or metadata
- **Output:** absent: if no lang is indicated in the metadata; list of langs: if at least a lang tag is retrieved

### Serialization formats
- **Description:** Checking whether data is available in different serialization formats
- **Input:** (working) SPARQL endpoint or Metadata or VoID file
- **Output:** []: if no serialization format is provided; list of serialization formats: if at least one serialization format is provided; -: if the SPARQL endpoint is missing.

### metadata-media-type
- **Description:** Checking whether data is available in different serialization formats
- **Input:** Metadata
- **Output:** []: if no serialization format is provided; list of serialization formats: if at least one serialization format is provided

### Availability of a common accepted Media Type
- **Description:** Checking whether data is available in a rdf serialization format that is commonly accepted
- **Input:** (working) SPARQL endpoint or Metadata or VoID file
- **Output:** True: if at least one commonly accepted serialization format is provided; False: if no commonly accepted serialization format is provided; No dump available: if no dump is available;

### SPARQL endpoint URL
- **Description:** Checking whether the data is available as a SPARQL endpoint and is available for download as an RDF dump
- **Input:** (working) SPARQL endpoint
- **Output:** url: if sparql endpoint url exists; -: if the sparql endpoint url does not exist

### URL for download the dataset
- **Description:** Checking whether the data is available as a SPARQL endpoint and is available for download as an RDF dump
- **Input:** (working) SPARQL endpoint or Metadata or VoID file
- **Output:** []: if the sparql endpoint or rdf dump is not online; list of links: if SPARQL endpoint and RDF dump are online.

### Versatility score
- **Description:** Overall score for the versatility dimension computed as a linear combination of the individual versatility metrics scores
- **Input:** Versatility metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Representational conciseness 

### Average length of URIs (subject)
- **Description:** Detection of long URIs or those that contain query parameters
- **Input:** (working) SPARQL endpoint
- **Output:** Average # of characters in URIs (subject). Best value: <80 characters

### Average length of URIs (predicate)
- **Description:** Detection of long URIs or those that contain query parameters
- **Input:** (working) SPARQL endpoint
- **Output:** Average # of characters in URIs (predicate). Best value: <80 characters

### Average length of URIs (object)
- **Description:** Detection of long URIs or those that contain query parameters
- **Input:** (working) SPARQL endpoint
- **Output:** Average # of characters in URIs (object). Best value: <80 characters

### Use RDF structures
- **Description:** Detection of the non-standard usage of collections, containers and reification
- **Input:** (working) SPARQL endpoint
- **Output:** True: if RDF structures are used; False: if RDF structures aren't used.; -: The SPARQL endpoint is missing.

toremove ## Understandability

### Vocabularies
- **Description:** Checking whether a list of vocabularies used in the dataset is provided
- **Input:** (working) SPARQL endpoint
- **Output:** []: if the vocabularies used are not indicated; -: the SPARQL endpoint is missing; list of vocabularies: if the vocabularies used are indicated.

### Number of labels/comments present on the data
- **Description:** No. of entities described by stating an rdfs:label or rdfs:comment in the dataset / total no. of entities described in the data.
- **Input:** (working) SPARQL endpoint
- **Output:** integer: the number of labels/comments; -: the SPARQL endpoint is missing; False: if no label/comment is present or error during the execution of the sparql query.

### Regex uri
- **Description:** Detecting whether a regular expression that matches the URIs is present
- **Input:** (working) SPARQL endpoint or VoID file
- **Output:** [] or empty value: no regex provided; -: if the SPARQL endpoint is missing; list of regex: if at least a regex is provided.

### Presence of example
- **Description:** Detecting whether examples of SPARQL queries are provided
- **Input:** Metadata
- **Output:** False: if examples are not provided; True: if examples are provided.

### Understandability score
- **Description:** Overall score for the understandability dimension computed as a linear combination of the individual understandability metrics scores
- **Input:** Understandability metrics scores
- **Output:** [0,1]. Best value: 1.

toremove ## Interoperability

### New vocabularies defined in the dataset
- **Description:** Usage of established vocabularies
- **Input:** (working) SPARQL endpoint
- **Output:** []: if no new vocabulary is defined; -: the SPARQL endpoint is missing; list of new vocabularies: if at least a new vocabulary is defined.

### New terms defined in the dataset
- **Description:** Detection of whether existing terms from all relevant vocabularies for that particular domain have been reused
- **Input:** (working) SPARQL endpoint
- **Output:** []: if no new term is defined; -: the SPARQL endpoint is missing; list of new terms: if at least a new term is defined.

### Interoperability score
- **Description:** Overall score for the interoperability dimension computed as a linear combination of the individual interoperability metrics scores
- **Input:** Interoperability metrics scores
- **Output:** [0,1]. Best value: 1.


toremove ## Interpretability

### Number of blank nodes
- **Description:** Detecting the use of blank nodes
- **Input:** (working) SPARQL endpoint
- **Output:** integer: the number of blank nodes; -: if the SPARQL endpoint is missing; False: if no blank node is present or error during the execution of the sparql query.

### Use RDF structures
- **Description:** Detection of the non-standard usage of collections, containers and reification
- **Input:** (working) SPARQL endpoint
- **Output:** True: if RDF structures are used; False: if RDF structures aren't used.; -: The SPARQL endpoint is missing.

### Interpretability score
- **Description:** Overall score for the interpretability dimension computed as a linear combination of the individual interpretability metrics scores
- **Input:** Interpretability metrics scores
- **Output:** [0,1]. Best value: 1.


---
toremove # Overall Quality Scores

### Score
- **Description:** Overall quality score computed as a linear combination of all scores of the individual quality dimensions
- **Input:** Quality dimensions scores
- **Output:** [0,1]. Best value: 1.

### Normalized score
- **Description:** Overall quality score computed as a linear combination of all scores of the individual quality dimensions, normalized in the range [0,100].
- **Input:** Quality dimensions scores
- **Output:** [0,100]. Best value: 100.

---