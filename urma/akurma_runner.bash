#!/bin/bash

set -e

conda activate urma

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for year in 2019 2020 2021 2022 2023 2024; do
    echo "Processing year $year..."

    if python "${SCRIPT_DIR}/extract_urma_pts.py" "$year"; then
        echo "Extraction succeeded for $year. Running postprocessing..."
        if python "${SCRIPT_DIR}/postprocess_csv.py" "$year" "${SCRIPT_DIR}"; then
            echo "Postprocessing succeeded for $year."
        else
            echo "Postprocessing failed for $year."
            exit 1
        fi
    else
        echo "Extraction failed for $year. Skipping postprocessing."
        exit 1
    fi
done