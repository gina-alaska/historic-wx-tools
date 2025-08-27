import os
import tarfile
import gzip
import shutil
from scrape_lamp import parse_lamp_guidance_file

def process_lamp_tar_archive(tar_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    temp_extract_dir = os.path.join(output_dir, "temp")
    os.makedirs(temp_extract_dir, exist_ok=True)

    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=temp_extract_dir)

    for root, _, files in os.walk(temp_extract_dir):
        for file in files:
            if file.endswith("30z.gz"):
                print(f"Processing {file}")
                gz_path = os.path.join(root, file)
                txt_filename = file.replace(".gz", ".txt")
                txt_path = os.path.join(output_dir, txt_filename)

                with gzip.open(gz_path, 'rt') as gz_file, open(txt_path, 'w') as out_file:
                    shutil.copyfileobj(gz_file, out_file)

                csv_filename = txt_filename.replace(".txt", ".csv")
                csv_path = os.path.join(output_dir, csv_filename)

                parse_lamp_guidance_file(txt_path, csv_path)

    shutil.rmtree(temp_extract_dir)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process LAMP .tar archive and extract CSVs.")
    parser.add_argument("tar_file", help="Path to the .tar archive containing .gz files")
    parser.add_argument("output_dir", help="Directory to store extracted .txt and .csv files")
    args = parser.parse_args()

    process_lamp_tar_archive(args.tar_file, args.output_dir)
