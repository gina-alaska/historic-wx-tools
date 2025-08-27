#!/bin/bash

set -e

conda activate urma

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for year in 2019 2020 2021 2022 2023 2024; do
    echo "Processing year $year..."

    tar_file="${SCRIPT_DIR}/lmp_lavtxt.${year}.tar"
    out_dir="${SCRIPT_DIR}/lmp_${year}"

    if curl --output "${tar_file}" "https://lamp.mdl.nws.noaa.gov/lamp/Data/archives/lmp_lavtxt.${year}.tar"; then
        echo "Download succeeded for $year. Running processing and postprocessing..."
        if python "${SCRIPT_DIR}/lamp_walk.py" "${tar_file}" "${out_dir}"; then
            if python "${SCRIPT_DIR}/postprocess_lamp.py" "${year}" "${SCRIPT_DIR}"; then
                echo "Postprocessing succeeded for $year."
            else
                echo "Postprocessing failed for $year."
                exit 1
            fi
        else
            echo "Processing failed for $year."
            exit 1
        fi
    else
        echo "Download failed for $year. Skipping processing and postprocessing."
        exit 1
    fi
done