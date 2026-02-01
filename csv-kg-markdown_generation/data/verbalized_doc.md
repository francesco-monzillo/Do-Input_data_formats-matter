# Knowledge Graph Quality Assessment – Full Metrics Documentation

This document describes all metrics implemented in the Knowledge Graph Quality Assessment framework.  
Each metric includes its **description**, **input requirements**, and **possible outputs**.  

---

## Table of Contents  
- [Availability](#availability)  
- [Security](#security)  
- [Timeliness](#timeliness)
- [Versatility](#versatility)  
- [Representational Conciseness](#representational-conciseness)  
- [Licensing](#licensing-metrics)  
- [Performance](#performance-metrics)  
- [Amount of data](#amount-of-data)  
- [Interlinking](#interlinking-metrics)  
- [Trust Metrics](#trust-metrics)  
- [Believability](#provenance-metrics)    
- [FAIR](#fair-metrics)  
- [Score](#score-metrics)  

---

# Availability

### SPARQL endpoint  
- **Description**: Verifies whether a SPARQL endpoint is accessible and responsive to queries.  
- **Input**: Metadata.  
- **Output**:  
  - `Available`: endpoint is online.  
  - `Offline`: endpoint is unreachable.  
  - `-`: no endpoint specified.  

### SPARQL endpoint URL  
- **Description**: Checks if a SPARQL endpoint URL is provided and valid.  
- **Input**: A working SPARQL endpoint.  
- **Output**:  
  - `url`: the endpoint URL.  
  - `-`: not available.  

### Availability of RDF dump (metadata)  
- **Description**: Determines whether an RDF dump is declared in the metadata and available for download.  
- **Input**: Metadata or VoID file.  
- **Output**:  
  - `1`: RDF dump available.  
  - `0`: RDF dump offline.  
  - `-1`: RDF dump not declared.  

### Availability of RDF dump (query)  
- **Description**: Verifies RDF dump availability through a SPARQL endpoint.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `True`: available.  
  - `False`: offline.  
  - `-`: not declared.  

### URL for dataset download  
- **Description**: Retrieves dataset download URLs from endpoints or metadata.  
- **Input**: SPARQL endpoint or VoID file.  
- **Output**:  
  - `list of links`: valid download URLs.  
  - `[]`: none available.  


# Security

### Use HTTPS  
- **Description**: Checks whether the SPARQL endpoint uses HTTPS.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `True`: HTTPS used.  
  - `False`: not used.  
  - `-`: endpoint missing.  

### Requires authentication  
- **Description**: Detects whether authentication is required (e.g., login, SSL, SSH).  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `True`: authentication required.  
  - `False`: not required.  
  - `-`: endpoint missing.  

---

# Timeliness

### Age of data  
- **Description**: Calculates dataset age as difference between current date and creation date.  
- **Input**: SPARQL endpoint or VoID.  
- **Output**:  
  - `integer`: dataset age in days.  
  - `-`: unavailable.  

### Modification date  
- **Description**: Extracts last modification date (`dcterms:modified`).  
- **Input**: SPARQL endpoint or VoID.  
- **Output**:  
  - `date`: last modification date.  
  - `-`: unavailable.  

### Time elapsed since last modification  
- **Description**: Computes elapsed time since dataset was last updated.  
- **Input**: SPARQL endpoint or VoID.  
- **Output**:  
  - `integer`: time elapsed.  
  - `-`: unavailable.  

---

# Versatility  

### Languages (query)  
- **Description**: Identifies languages used in literals.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `list`: language codes.  
  - `False`: no language tags.  
  - `-`: endpoint missing.  

### Languages (metadata)  
- **Description**: Retrieves languages declared in metadata.  
- **Input**: VoID or metadata.  
- **Output**:  
  - `list`: languages.  
  - `absent`: not declared.  

### Serialization formats  
- **Description**: Lists supported serialization formats.  
- **Input**: SPARQL endpoint or metadata.  
- **Output**:  
  - `list`: available formats.  
  - `[]`: none.  

### Availability of a common accepted Media Type  
- **Description**: Validates whether dataset provides a widely supported RDF serialization format.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `True`: supported.  
  - `False`: unsupported.  
  - `No dump available`: none.  

---

# Representational conciseness

### Average length of URIs (subject, predicate, object)  
- **Description**: Computes readability of URIs.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `integer`: average URI length.  
  - Best value: `< 80 characters`.  

### Use RDF structures  
- **Description**: Detects RDF collections, containers, or reification.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `True`: structures present.  
  - `False`: absent.  
  - `-`: endpoint missing.  

---

# Licensing Metrics  

### License machine-readable (metadata/query)  
- **Description**: Retrieves license information in machine-readable form.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `string`: license URI.  
  - `False`: not provided.  
  - `-`: endpoint missing.  

### License human-readable  
- **Description**: Detects whether documentation includes a human-readable license.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `True`: indicated.  
  - `False`: missing.  
  - `-`: endpoint missing.  

---

# Performance Metrics  

### Median latency  
- **Description**: Measures query response time.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `ms`: average latency.  
  - Best: `< 1000 ms`.  

### Median throughput  
- **Description**: Counts HTTP requests answered per second.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `integer`: throughput.  
  - Higher is better.  

---

# Amount of data 

### Number of triples (metadata/query)  
- **Description**: Total count of triples in dataset.  
- **Input**: VoID or SPARQL endpoint.  
- **Output**:  
  - `integer`: number of triples.  
  - `-`: unavailable.  

### Number of entities  
- **Description**: Counts dataset entities.  
- **Input**: VoID or endpoint.  
- **Output**:  
  - `integer`: number.  
  - `-`: unavailable.  

### Number of properties  
- **Description**: Counts distinct properties.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `integer`: number.  
  - `-`: unavailable.  

---

# Interlinking Metrics  

### Number of distinct classes  
- **Description**: Counts unique RDF classes.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `integer`: number of classes.  

### Number of distinct external links  
- **Description**: Measures external interlinking across datasets.  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `integer`: count of links.  

### VoID linksets  
- **Description**: Retrieves declared linksets.  
- **Input**: VoID file.  
- **Output**:  
  - `list`: linksets.  
  - `[]`: none.  

---

# Believability  

### Trusted vocabulary usage  
- **Description**: Checks if dataset uses recognized vocabularies (e.g., FOAF, DC, SKOS).  
- **Input**: SPARQL endpoint.  
- **Output**:  
  - `True`: trusted vocabularies used.  
  - `False`: absent.  

---

# Provenance Metrics  

### Provenance metadata  
- **Description**: Detects provenance information such as creator, contributor, or publisher.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `True`: provenance present.  
  - `False`: absent.  

---

# FAIR Metrics  

### F1.1 – Persistent identifier  
- **Description**: Checks whether dataset provides globally unique and persistent identifiers.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `True`: identifiers are persistent.  
  - `False`: not guaranteed.  

### F2 – Rich metadata  
- **Description**: Verifies if metadata provides detailed descriptive information.  
- **Input**: Metadata or VoID file.  
- **Output**:  
  - `True`: rich metadata provided.  
  - `False`: missing.  

### A1 – Retrievability via protocol  
- **Description**: Evaluates if dataset is retrievable via a standardized protocol (HTTP, HTTPS, SPARQL).  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `True`: retrievable.  
  - `False`: not retrievable.  

### I1 – Use of standards  
- **Description**: Assesses adoption of standardized vocabularies and formats.  
- **Input**: Endpoint.  
- **Output**:  
  - `True`: standards used.  
  - `False`: absent.  

### R1.1 – License availability  
- **Description**: Validates whether usage license is available.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `True`: license provided.  
  - `False`: missing.  

---

# Score Metrics  

### Availability score  
- **Description**: Aggregated score of accessibility metrics (SPARQL endpoint, RDF dump, HTTPS, authentication).  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `float`: score between 0–1.  

### Interoperability score  
- **Description**: Aggregated score of serialization, vocabularies, and language metrics.  
- **Input**: Metadata or endpoint.  
- **Output**:  
  - `float`: score between 0–1.  

### FAIR score  
- **Description**: Composite measure of FAIR principles (Findable, Accessible, Interoperable, Reusable).  
- **Input**: Metadata and endpoint.  
- **Output**:  
  - `float`: score between 0–1.  
