import dask.dataframe as dd
import zipfile
import sys
import os
from pathlib import Path

def create_combined_df(csv_dir: Path):
    
    df = dd.read_csv(csv_dir, delimiter=' ')

    return df

def export_df(df, output_path):
    df.to_csv(f'akurma_{year}.csv', index=False, single_file=True)

def zip_csv(csv_path):
    with zipfile.ZipFile(csv_path + ".zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path)
    
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
    print(f"Zipping {combined_csv_path} into {combined_csv_path + ".zip"}")
    zip_csv(combined_csv_path)
    
    