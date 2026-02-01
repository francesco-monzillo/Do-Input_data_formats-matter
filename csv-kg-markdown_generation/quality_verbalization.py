from langchain_core.prompts import PromptTemplate
from llms import LLMS
import yaml
import os
import pandas as pd
import json

HERE = os.path.dirname(os.path.abspath(__file__))
PROMPT_PATH = os.path.join(HERE, "prompts.yaml")

with open(PROMPT_PATH, "r", encoding="utf-8") as f:
    PROMPTS = yaml.safe_load(f)

def verbalize_quality_report(quality_report_path: str, quality_report_example, quality_documentation_path: str, llm_model: str, row_to_read = 0, output_path: str = None) -> str:
    """
    Verbalizes a quality report using a language model.

    Args:
        quality_report (str): Path to the quality report to be verbalized.
        quality_report_example (str): Example of a verbalized quality report.
        quality_documentation (str): Path to the documentation describing each metric.
        llm_model (str): The language model to use for verbalization.
        row_to_read (int): The row number in the quality report to read and verbalize
    Returns:
        str: The verbalized quality report.
    """
    # Read quality report
    quality_report_df = pd.read_csv(quality_report_path)
    row = quality_report_df.iloc[row_to_read]

    # Pair header and row values
    quality_report_text = ", ".join(
        f"{col}: {row[col]}" for col in quality_report_df.columns
    )
    quality_report_text_escaped = quality_report_text.replace("{", "{{").replace("}", "}}")

    # Read JSON documentation
    with open(quality_documentation_path, "r") as f:
        quality_doc = json.load(f)
    quality_doc_text = json.dumps(quality_doc, indent=2)
    quality_doc_text_escaped = quality_doc_text.replace("{", "{{").replace("}", "}}")

    # Read the .md file with the example of a verbalized quality report
    with open(quality_report_example, "r", encoding="utf-8") as f:
        quality_report_example_text = f.read()
    quality_report_example_text_escaped = quality_report_example_text.replace("{", "{{").replace("}", "}}")

    verbalization_prompt = PromptTemplate.from_template(PROMPTS["template_verbalization"])
    

    llms = LLMS(
        model=llm_model,
        prompt_template=verbalization_prompt,
        quality_report=quality_report_text_escaped,
        quality_documentation=quality_doc_text_escaped,
        question=quality_report_example_text_escaped
    )

    llm_response = llms.execute_verbalization()

    # Save to Markdown file
    if output_path is None:
        output_path = os.path.join(HERE, '..', "./data/verbalized_report.md")
    
    with open(output_path, "a", encoding="utf-8") as f:
        f.write("\n---\n")  # separator between entries
        f.write(f"## Verbalized Quality Report (Row {row_to_read})\n\n{llm_response}\n")

    return llm_response

if __name__ == "__main__":

    quality_report_path = "../data/quality_data_filtered/kg_quality_melted.csv"
    quality_documentation_path = "../data/metrics_doc.json"
    quality_report_example = "../data/verbalized_report_example.md"
    quality_report_df = pd.read_csv(quality_report_path)
    num_rows = quality_report_df.shape[0]
    llm_model = "gpt-5"

    for row in range(num_rows):
        
        print(f"Processing row {row}...")
        verbalized_report = verbalize_quality_report(quality_report_path, quality_report_example, quality_documentation_path, llm_model, row_to_read=num_rows)
        print("Verbalized Quality Report:\n", verbalized_report)