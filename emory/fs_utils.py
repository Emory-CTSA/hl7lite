import os

import logging
log = logging.getLogger(__name__)

def get_file_list(hl7_dir:str):
    log.info(f"scanning directory {hl7_dir}")
    hl7_files = {}
    # get the file list:  
    # could be YYYY-MM-DD--hh/part-xxxx.hl7
    # or  BED_ID/YYYY/MM/DD/hh/part-xxxx.hl7
    # or  HOSP/Unit/Bed/YYYY/MM/DD/hh/part-xxxx.hl7
    for root, _, files in os.walk(hl7_dir):
        if root == hl7_dir:
            # files are directly under hl7_dir - no bed separation
            bed_id = "all_beds"
        else:
            subdir = os.path.relpath(root, hl7_dir)
            tokens = subdir.split(os.sep)
            
            if len(tokens) == 5:
                # BED_ID/YYYY/MM/DD/hh
                bed_id = tokens[0]
            elif len(tokens) == 7:
                # HOSP/Unit/Bed/YYYY/MM/DD/hh
                bed_id = "_".join(tokens[0:3])
        
        for f in files:
            if f.endswith('.hl7'):
                # include this file
                if bed_id not in hl7_files:
                    hl7_files[bed_id] = []
                hl7_files[bed_id].append(os.path.join(root, f))

    for bed_id in hl7_files.keys():
        # sort the files by the number at the end of the filename - else the rows in dataframe would be out of order.
        hl7_files[bed_id].sort(key=lambda x: int(x.split('-')[-1].split('.')[0]))
                

    log.info(f"found {len(hl7_files)} beds and total of {sum(len(files) for files in hl7_files.values())} files in {hl7_dir}")
    return hl7_files

