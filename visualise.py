import pandas as pd
from pandas.core.api import DataFrame


def load_measures(file_path: str):
    return pd.read_csv(file_path)


def find_mean(measures: pd.DataFrame):
    summary = measures.drop("attempt", axis=1).groupby("store").aggregate("mean")
    print(summary)
    return summary


def get_comparison_counts(measures: DataFrame):
    print(f"Measures: {measures}")
    counts = measures.transpose()
    print(f"After transpose: {counts}")
    print(f"Columns in counts: {counts.columns}")
    counts["duckdb_better"] = 0 if counts["duckdb"] > counts["trino"] else 1
    counts["trino_better"] = 0 if counts["duckdb"] < counts["trino"] else 1
    print(f"Counts: {counts}")
    counts = counts.aggregate("sum")[["duckdb_better", "trino_better"]]
    print(f"Counts after agg: {counts}")
    return counts


if __name__ == "__main__":
    measures = load_measures("./debug/measures_2025-06-09_11-32.csv")
    averages = find_mean(measures)
    counts = get_comparison_counts(measures)
