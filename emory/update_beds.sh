#!/bin/bash

conda activate aecg
cd ~/src/waveform
python inspect_format_hl7aecg.py --rootdir /mnt/c/Users/tcp19/Downloads/HL7_aECG --folders 2025-10-22--12 --fields PV1.3 > raw_beds.txt

grep "PV1.3:" raw_beds.txt | cut -d ":" -f 5 | sort | uniq > new_bed_ids.txt

# merge with the existing bed list
cat tests/BEDS.tsv new_bed_ids.txt | sort | uniq > tests/NEW_BEDS.tsv

python tests/test_bed_ids.py tests/NEW_BEDS.tsv


# the 9 value PV1.3 variant has the device doe at PV1.3.9, which is potentially useful:
# Ventilators and respiratory
#   840: Puritan Bennett 840 ventilator
#   980: Puritan Bennett 980 ventilator
#   SERVO-U: Maquet/Getinge Servo‑U ventilator
#   VENT: Generic ventilator (bed flagged as ventilated)
#   TWIT: Likely “transport vent”/“T‑vent” (transport ventilator) or a local mnemonic for transport/withdrawal ventilation
#   OXI: Pulse oximetry/oxygen therapy associated with the bed

# Renal replacement / CRRT
#   Prismax: Baxter PrisMax CRRT machine
#   pflex: Baxter Prismaflex CRRT machine
#   CRRT: Continuous Renal Replacement Therapy active at this bed (generic flag)

# Hemodynamics / advanced monitoring
#   Hemosphere: Edwards HemoSphere hemodynamic monitoring platform
#   G5: Site‑specific, but commonly:
#     A gas module (e.g., “G5” gas analyzer) or
#     A particular monitor model/series (you’d confirm locally)

# Philips monitoring modules / patient monitoring
#   C3: Philips IntelliVue C3 measurement module (or similar C‑series module)
#   C6: Philips IntelliVue C6 measurement module (extended parameters)
#   MPS3: Philips Multi‑Parameter Server (MPS) revision/variant “3”
#   MON: Generic bedside monitor flag (bed has a monitor attached)

# Anesthesia / perfusion
#   ANES: Anesthesia machine / anesthesia care location
#   PERF: Perfusion device (e.g., perfusion pump, bypass/perfusion support)
#   ESNZ: Esophageal temperature probe (often associated with anesthesia care)