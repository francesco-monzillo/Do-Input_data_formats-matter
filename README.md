# Do Input Data Formats Matter?

This repository contains supplementary material supporting the paper **“Do Input Data Formats Matter? Analyzing Their Effect on LLM-Driven Interpretation of Data Quality Assessment”**, which is currently under review.

---

## Repository Structure and Navigation

### Reconstructing CSV, Knowledge Graph, and Markdown Formats from Raw Results

The code and data used to generate the CSV, Knowledge Graph (KG), and Markdown representations are available in the following directory:

`./csv-kg-markdown_generation`

The Jupyter notebook `run_benchmark.ipynb` allows the regeneration of all output formats starting from the raw data quality results in CSV format produced by the *AnonymousTool*.

---

### Building Hypergraphs from our CSVs of interest

The code and data used to generate our 5 hypergraphs (4-uniform, Time-labeled, Measure-labeled, KG-labeled, Value-labeled) are available in the following directory:

`./hypergraphs_extraction`

Documentation and all the weekly quality assessment data can be found in `./hypergraphs_extraction/documentation` and `./hypergraphs_extraction/Weekly_Data`, respectively.

---

### Querying our selected Large Language Models (LLMs) using the standard OpenAI unified interface

The code used to create the prompts and store responses, for each representation, is available in the following directory:

`./querying_models`

Inside `./querying_models/baselines_relevant_chunks.py` and `./querying_models/hypergraphs_relevant_chunks.py` can be used for baselines (Free-text, CSV and KG) and Hypergraphs, respectively.

---

### Human Evaluation Results

The directory:

`/human_evaluated_output`

contains CSV files with annotations provided by five human annotators, organized by model and by the evaluated input formats.
Each subfolder also includes a `summary*.csv` file reporting the average accuracy, aggregated by question type and by input format.

---

### Annotator Agreement

The directory:

`/annotator_agreement`

includes the data obtained from the computation of inter-annotator agreement using **Krippendorff’s Alpha**.
It also contains the Python script used to derive the agreement scores starting from the human evaluation results.

---

### Statistical Analysis Results

This directory contains the datasets and results used for the statistical analyses reported in the paper, aimed at investigating whether input data formats have a statistically significant impact on LLM response accuracy.

Specifically, the folder includes a Python script that:

* Tests data normality using the **Shapiro–Wilk test**;
* Applies **ANOVA** or **Kruskal–Wallis** tests accordingly;
* Automatically performs post-hoc analyses when *p < 0.05* to identify statistically significant differences between formats;
* Generates boxplots (including mean values) for all evaluated formats;
* Outputs results both to the console and to CSV files.
