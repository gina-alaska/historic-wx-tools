#!/bin/bash

mamba activate urma

for year in 2022 2023 2024; do
    echo "Processing year $year..."

    if python extract_urma_pts.py "$year"; then
        echo "Extraction succeeded for $year. Running postprocessing..."
        python postprocess_csv.py "$year"
    else
        echo "Extraction failed for $year. Skipping postprocessing."
    fi
done
