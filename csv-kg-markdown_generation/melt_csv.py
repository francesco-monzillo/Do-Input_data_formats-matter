import pandas as pd
import glob
import os
import json

def load_metric_map(json_file):
    """Load metric→dimension/category mapping from external JSON file."""
    with open(json_file, "r", encoding="utf-8") as f:
        return json.load(f)
    
def load_metric_doc_map(json_file):
    """Load the metrics documentation from external JSON file."""
    with open(json_file, "r", encoding="utf-8") as f:
        return json.load(f)

def melt_csv(folder, output_csv, json_file = '../data/dimension_category_mapping.json'):   
    """
    :param folder: path to folder containing CSV snapshots
    :param output_csv: path to output long-format CSV file
    """

    all_long_dfs = []
    # Load mapping 
    metric_map = load_metric_map(json_file)

    # Iterate over all CSV files in the folder
    for file_path in sorted(glob.glob(os.path.join(folder, "*.csv"))):

        if 'melted' in file_path:
            continue  # Skip already melted files

        # Extract filename without extension
        filename = os.path.basename(file_path)
        # Assume filename contains date like "2024-01-15.csv"
        snapshot_date = os.path.splitext(filename)[0].split(".")[0]  

        # Load CSV
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()

        # Rename columns except identifiers
        new_columns = []
        for col in df.columns:
            if col in ["KG id", "KG name", "Analysis date"]:
                new_columns.append(col)
            else:
                dimension = metric_map.get(col, "Unknown")
                category = metric_map.get(dimension, "Unknown")
                new_columns.append(f"{category}_{dimension}_{col}")
        df.columns = new_columns
        
        df['Analysis date'] = snapshot_date

        all_long_dfs.append(df)

    # Combine all snapshots
    df_all = pd.concat(all_long_dfs, ignore_index=True)

    # Save the long-format CSV
    df_all.to_csv(output_csv, index=False)
    print(f"Long-format CSV generated: {output_csv}")

def melt_csv_for_rml(folder, output_csv, json_file="../data/dimension_category_mapping.json", metric_doc_file="../data/metrics_doc.json"):   
    """
    :param folder: path to folder containing CSV snapshots
    :param output_csv: path to output long-format CSV file
    :param json_file: path to JSON file containing metric→dimension/category mapping
    """

    # Load metrics mapping 
    metric_map = load_metric_map(json_file)
    # Load metrics documentation
    metric_info = load_metric_doc_map(metric_doc_file)

    all_long_dfs = []

    # Iterate over all CSV files in the folder
    for file_path in sorted(glob.glob(os.path.join(folder, "*.csv"))):

        if 'melted' in file_path:
            continue  # Skip already melted files

        # Extract filename without extension
        filename = os.path.basename(file_path)
        # Assume filename contains date like "snapshot_2024-01-15.csv"
        snapshot_date = os.path.splitext(filename)[0] 

        # Load CSV
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()

        # Melt wide CSV to long format
        df_long = df.melt(
            id_vars=["KG id", "KG name", "Description", "SPARQL endpoint URL"],  # columns that identify the KG
            var_name="metric",
            value_name="metric_value_rml"
        )

        # Map metric -> Dimension
        df_long["Dimension"] = df_long["metric"].map(metric_map)

        # Map Dimension -> Category (second-level mapping)
        df_long["Category"] = df_long["Dimension"].map(metric_map)

        # Add snapshot column from filename
        df_long["Analysis date"] = snapshot_date

        # Enrich with metadata
        df_long["metric_description"] = df_long["metric"].map(lambda m: metric_info.get(m, {}).get("description"))
        df_long["metric_input"] = df_long["metric"].map(lambda m: metric_info.get(m, {}).get("input"))
        df_long["metric_output"] = df_long["metric"].map(lambda m: metric_info.get(m, {}).get("output"))

        all_long_dfs.append(df_long)

    # Combine all snapshots
    df_all = pd.concat(all_long_dfs, ignore_index=True)

    # Save the long-format CSV
    df_all.to_csv(output_csv, index=False)
    print(f"Long-format CSV generated: {output_csv}")



if __name__ == "__main__":

    quality_data_folder = "../data/quality_data_filtered/"
    output_csv = "../data/quality_data_filtered/kg_quality_melted.csv"

    melt_csv(folder=quality_data_folder, output_csv=output_csv)
    melt_csv_for_rml(folder=quality_data_folder, output_csv="../data/quality_data_filtered/kg_quality_melted_for_rml.csv")