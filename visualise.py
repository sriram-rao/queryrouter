from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes
from pandas.core.api import DataFrame

from experiment import get_time_string
from store import DUCKDB


def load_measures(file_path: str):
    return pd.read_csv(file_path)


def find_mean(measures: pd.DataFrame) -> DataFrame:
    summary = (
        measures.drop("attempt", axis=1)
        .groupby("store", as_index=False)
        .aggregate("mean")
    )
    summary = summary.melt(id_vars="store", var_name="query", value_name="time")
    print(f"Means:\n{summary}")
    return summary


def get_comparison_counts(measures: DataFrame) -> dict[str, Any]:
    print(f"Measures:\n{measures}")
    measures_pivot = measures.melt(
        id_vars=["attempt", "store"], var_name="query", value_name="time"
    )
    comparison = measures_pivot.pivot(
        index=["attempt", "query"], columns="store", values="time"
    )
    duckdb_better = comparison[comparison["duckdb"] < comparison["trino"]].count()[
        "duckdb"
    ]
    # print(f"Duck: {duckdb_better}")
    total = comparison.count()["duckdb"]
    # print(f"Counts after agg:\n{measures_pivot}")
    print(
        f"duckdb_better: {duckdb_better}\ntrino_better: {total - duckdb_better}\ntotal: {total}"
    )
    return {
        "pivot": measures_pivot,
        "duckdb": duckdb_better,
        "trino": total - duckdb_better,
        "total": total,
    }


def scatter(data: DataFrame, path: str = "debug/charts"):
    # print(data)
    grid = sns.relplot(data=data, x="query", y="time", hue="store")
    grid.set_axis_labels(x_var="Query", y_var="Time Taken")
    grid.savefig(f"./debug/charts/scatter_{get_time_string()}.pdf", dpi=300)
    plt.show()


def bar(data: DataFrame, path: str = "debug/charts"):
    format_plot(
        sns.barplot(data=data, x="query", y="time", hue="store"),
        x="Query",
        y="Avg. Time Taken",
        file="bar",
    )


def format_plot(ax: Axes, x: str, y: str, file: str, path: str = "debug/charts/"):
    ax.set(xlabel=x, ylabel=y)
    plt.savefig(f"{path}/{file}_{get_time_string()}.pdf", dpi=300)
    plt.show()


def f1_measure(prediction: DataFrame, truth: DataFrame, path: str = "debug/charts"):
    prediction_data = prediction[["attempt", "store", "query"]].pivot(
        index=["attempt", "query"], columns="store", values="time"
    )
    test_selection_data["selection"] = test_selection_data.apply(
        lambda row: 1 if row.store == DUCKDB else 0, axis=1
    )
    print(test_selection_data)
    test_selection_data["selection"] = test_selection_data.apply(
        lambda row: 1 if row["selection"] > 2 else 0, axis=1
    )
    test_data = test_selection_data[["selection"]].to_numpy()
    print(f"True:\n{true}")
    true_data = (
        true[["query", "store", "responsetime"]]
        .groupby(["query", "store"], as_index=False)
        .aggregate("mean")
        .pivot(index=["query"], columns="store", values="responsetime")
    )
    print(f"True data:\n{true_data}")
    true_data["selection"] = true_data.apply(
        lambda row: 1 if row.duckdb > row.trino else 0, axis=1
    )
    print(f"True data:\n{true_data}")
    true_data = true_data.sort_values(by="query")
    print(test_data)
    print(true_data)
    print("Done?")


if __name__ == "__main__":
    measures = load_measures("./debug/measures_2025-06-09_11-32.csv")
    # averages = find_mean(measures)
    counts = get_comparison_counts(measures)
    sns.set_theme()
    # scatter(counts["pivot"])
    # bar(averages)
    f1_measure(
        load_measures("./debug/selections_2025-06-09_22-25.csv"), counts["pivot"]
    )
