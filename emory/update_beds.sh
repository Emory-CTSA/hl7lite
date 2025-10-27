#!/bin/bash

conda activate aecg
cd ~/src/waveform
python inspect_format_hl7aecg.py --rootdir /mnt/c/Users/tcp19/Downloads/HL7_aECG --folders 2025-10-22--12 --fields PV1.3 > raw_beds.txt

grep "PV1.3:" raw_beds.txt | cut -d ":" -f 5 | sort | uniq > new_bed_ids.txt

# merge with the existing bed list
cat tests/BEDS.tsv new_bed_ids.txt | sort | uniq > tests/NEW_BEDS.tsv

python tests/test_bed_ids.py tests/NEW_BEDS.tsv