import dask.dataframe as dd
import zipfile
import sys
import os
from pathlib import Path

def create_combined_df(csv_dir: Path) -> dd.DataFrame:
    
    df = dd.read_csv(csv_dir, delimiter=' ')

    return df

def export_df(df: dd.DataFrame, output_path: Path) -> Path:
    
    df.to_csv(output_path, index=False, single_file=True)
    
    return output_path

def zip_csv(csv_path: Path) -> Path:
    
    with zipfile.ZipFile(csv_path.with_suffix(".zip"), 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path)
    
    return csv_path.with_suffix(".zip")
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <year>")
        sys.exit(1)
        
    year = sys.argv[1]

    csv_dir = Path(f'./{year}/*.csv')
    combined_csv_path = Path(f'akurma_{year}.csv')
    
    print(f"Creating dask dataframe from {year}...")
    df = create_combined_df(csv_dir)
    print(f"Exporting dataframe as {combined_csv_path}...")
    export_df(df, combined_csv_path)
    print(f"Zipping {combined_csv_path} into {combined_csv_path.with_suffix(".zip")}")
    zip_csv(combined_csv_path)
    
    