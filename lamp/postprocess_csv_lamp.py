import dask.dataframe as dd
import pandas as pd
import zipfile
import sys
import os
from pathlib import Path


def create_combined_df(csv_dir: Path) -> dd.DataFrame:

    df = dd.read_csv(csv_dir, delimiter=",", dtype=str)

    return df


def filter_df(df: dd.DataFrame, template_path: Path):

    stns = pd.read_csv(template_path)
    stn_list = stns["STN"].tolist()
    df = df[df["Station"].isin(stn_list)]

    return df


def export_df(df: dd.DataFrame, output_path: Path) -> Path:

    df.to_csv(output_path, index=False, single_file=True)

    return output_path


def zip_csv(csv_path: Path) -> Path:

    with zipfile.ZipFile(
        csv_path.with_suffix(".zip"), "w", zipfile.ZIP_DEFLATED
    ) as zipf:
        zipf.write(csv_path)

    return csv_path.with_suffix(".zip")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <year> <base_dir>")
        sys.exit(1)

    year = sys.argv[1]
    base_dir = Path(sys.argv[2])

    csv_dir = base_dir / f"lmp_{year}"
    csv_pattern = csv_dir / "*.csv"
    combined_csv_path = base_dir / f"lmp_{year}_ak.csv"
    stations_path = base_dir / "lamp_stations_AK.csv"

    print(f"Creating dask dataframe from {csv_pattern}...")
    df = create_combined_df(csv_pattern)
    df = filter_df(df, stations_path)
    print(f"Exporting dataframe as {combined_csv_path}...")
    export_df(df, combined_csv_path)
    print(f"Zipping {combined_csv_path} into {combined_csv_path.with_suffix('.zip')}")
    zip_csv(combined_csv_path)
