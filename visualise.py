from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes
from pandas.core.api import DataFrame

from experiment import get_time_string


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


if __name__ == "__main__":
    measures = load_measures("./debug/measures_2025-06-09_11-32.csv")
    averages = find_mean(measures)
    counts = get_comparison_counts(measures)
    sns.set_theme()
    scatter(counts["pivot"])
    bar(averages)
