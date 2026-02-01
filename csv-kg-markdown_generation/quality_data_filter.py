import pandas as pd
import os
import glob

class QualityDataFilter:
    def __init__(self, input_file, output_file, kg_ids=None, selected_columns=None, discard_columns=None):
        """
        :param input_file: path to input file (.csv or .ttl)
        :param output_file: path to output file (.csv or .ttl)
        :param kg_ids: list of KG ids to keep
        :param selected_columns: list of columns (CSV)
        :param discard_columns: list of columns to discard (CSV)
        """
        self.input_file = input_file
        self.output_file = output_file
        self.kg_ids = set(kg_ids) if kg_ids else None
        self.selected_columns = selected_columns
        self.discard_columns = discard_columns

    def filter_data(self):
        if self.input_file.endswith(".csv"):
            self._filter_csv()
        else:
            raise ValueError("Unsupported file format. Use .csv or .ttl")

    def _filter_csv(self):
        df = pd.read_csv(self.input_file)

        # filter rows by KG id
        if self.kg_ids:
            df = df[df["KG id"].astype(str).isin(self.kg_ids)]

        # keep only selected columns
        if self.selected_columns:
            df = df[self.selected_columns]

        df.rename(columns={"Volatility score": "Timeliness score", "Representational-Consistency score" : "Interoperability score"}, inplace=True)

        # drop unwanted columns
        if self.discard_columns:
            strip_map = {col.strip(): col for col in df.columns} # map stripped column names to original
            to_drop = [strip_map[col.strip()] for col in self.discard_columns if col.strip() in strip_map]
            df = df.drop(columns=to_drop)

        df.to_csv(self.output_file, index=False)

if __name__ == "__main__":
    quality_data_to_filter_folder = "../data/full_quality_data/"

    for file_path in sorted(glob.glob(os.path.join(quality_data_to_filter_folder, "*.csv"))):

        filter_obj = QualityDataFilter(
            input_file=file_path,
            output_file=f"../data/quality_data_filtered/{os.path.basename(file_path)}",
            kg_ids=["NoiPA", "dblp-kg", "bpr", "allie-abbreviation-and-long-form-database-in-life-science", "w3c-wordnet", "bbc-programmes", "LemmaBank", "micro-coronavirus", "CIDOC-CRM", "environment-agency-bathing-water-quality"],
            #selected_columns=["KG id", "KG name", "Score"]
            discard_columns=["MinTPNoOff","MaxTPNoOff","sdTPNoOff","Limited","MinTPNoOff","MeanTPNoOff","MaxTPNoOff","sdTPNoOff","CS2-value","IN3-value","RC1-value",
                             "RC2-value","IN4-value","Minimum throughput","25th percentile throughput","75th percentile throughput","Maximum throughput",
                             "Standard deviation of throughput","U5-value","PE2-value","PE3-value", " Standard deviation length URIs (subject)","Min length URIs (subject)",
                             "25th percentile length URIs (subject)", "75th percentile length URIs (subject)","Max length URIs (subject)"," Standard deviation length URIs (predicate)",
                             "Min length URIs (predicate)","25th percentile length URIs (predicate)","75th percentile length URIs (predicate)","Max length URIs (predicate)","Standard deviation length URIs (object)",
                             "Min length URIs (object)","25th percentile length URIs (object)","75th percentile length URIs (object)","Max length URIs (object)","Minimum latency","25th percentile latency","75th percentile latency",
                             "Maximum latency"," Standard deviation of throughput","Historical updates", "Offline dumps", "Number of triples linked", "Number of triples updated", 
                             "Availability VoID file", "U1-value", "Percentage of data updated", "Standard deviation lenght URIs (subject)","Median length URIs (subject)", "Standard deviation lenght URIs (predicate)", "Median length URIs (predicate)",
                             "Standard deviation lenght URIs (object)", "Median length URIs (object)", "Average latency", "Standard deviation of latency", "Average throughput", "External links", "Number of triples", "Percentage of triples with labels", 
                             "Uses RDF structures","Inactive links", "Url file VoID"]
        )
        filter_obj.filter_data()
        