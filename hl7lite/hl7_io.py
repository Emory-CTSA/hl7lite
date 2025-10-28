from hl7lite.hl7_tokenizer import tokenize_hl7_message, FIELD_SEPARATOR
import os
from hl7lite.hl7_ds import HierarchicalMessage, HL7ORUData, HL7ADTData, hl7_data_factory
from hl7lite.hl7_aecg_test import _verify_hl7_msg
from hl7lite.hl7_datatypes import missing_values
import pandas as pd
import json

import logging
log = logging.getLogger(__name__)

# %%
# process the HL7ECGData, and returns
# dict of bed, channel (OBX) to tuple of (obr start, obr end, OBR src, obx count, obx samples)
def extract_bed_channel_data(msg: HL7ORUData, dirname: str, filename: str, seg_id: int = -1):
    waveforms = []
    file_common = {
        'seg_id': int(seg_id),
        'dir': dirname,
        'file': filename
    }
    
    for d in msg.to_row_dicts():
        d.update(file_common)
        waveforms.append(d)
        
    return waveforms


 
#%%
def read_hl7_file(hl7_file: str, history_fn: str, current_fn: str, verify_message:bool = False):
    segment_id = int(hl7_file.split('-')[-1].split('.')[0])
    data = []
    pat_infos = []
    count = 0
    
    with open(hl7_file, 'r') as file:
                        
        # read the whole file
        file_content = file.read()
            
        # next split the file_content by double newlines
        # the double newlines are of the format \r\n\r\n or \n\n, but never \r\r
        # we do not see double newlines within a message between segments, only \r\n, \r, or \n
        # note that some tools only accept \r as line terminator.  ours does not have this limitation.
        # approach for split is to convert \r\n to \n, then target \n\n
        # should be faster than using regex.
        messages = file_content.replace('\r\n', '\n').replace('\r', '\n').split('\n\n')
        # messages = message_separator.split(file_content)
            
        converted = []
        for msg in messages:
            if len(msg) <= 0:
                continue
            
            # tokenize_hl7_message works has to split the segments anyways, so dont bother split then rejoin.
            # segs = segment_separator.split(msg)
            # hl7_msg = '\r'.join(segs)
            
            # parsed is the dict of segments.  segnames is name of hl7 segments.
            parsed, segnames = tokenize_hl7_message(msg)
            
            # first organize the parsed data
            omsg = HierarchicalMessage(parsed, segnames)
            
            # then extract the data and metadata.
            try:
                data_msg = hl7_data_factory(omsg)
            except ValueError as e:
                log.error(f"in file {hl7_file}, message {count}: {e}")
                continue
            
            if verify_message and (not isinstance(data_msg, HL7ADTData)):
                verify_result = _verify_hl7_msg(data_msg)
                if len(verify_result['errs']) > 0:
                    log.error(f"Types: {verify_result['wavetypes']}:  {verify_result['errs']}")
                if len(verify_result['warns']) > 0:
                    log.warning(f"Types: {verify_result['wavetypes']}:  {verify_result['warns']}")

                    
            converted.append(data_msg)
            
            # extract patient information
            info_dicts = data_msg.get_pid_loc_mapping()
            pat_infos.extend(info_dicts)
        
                
    # open history of past patient info
    if os.path.exists(history_fn):
        history_df = pd.read_parquet(history_fn, engine='fastparquet')
    else:
        history_df = None
    # this represents the starting point of the current period
    if os.path.exists(current_fn):
        current_df = pd.read_parquet(current_fn, engine='fastparquet')
    else:
        current_df = None

    # update the patient info
    history_df, bed_to_pat, next_df = update_patient_info(history_df, current_df, pat_infos)

    # write out
    history_df.to_parquet(history_fn, engine='fastparquet', compression='snappy', index=False)
    next_df.to_parquet(current_fn, engine='fastparquet', compression='snappy', index=False)

    # extract bed info, including using patient info.
    for data_msg in converted:    
                    
        # extract the channel info
        if not isinstance(data_msg, HL7ADTData):
            data_dict = extract_bed_channel_data(data_msg, dirname = os.path.basename(os.path.dirname(hl7_file)), filename = os.path.basename(hl7_file), seg_id = segment_id)
            
            # look up in current_df the patient id, name, visit id, etc.
            if (bed_to_pat is not None):
                for ddict in data_dict:
                    # get the patient info for this bed
                    bed_key = (ddict['hospital'], ddict['bed_unit'], ddict['bed_id'])
                    start_t = ddict['start_t']
                    if bed_key in bed_to_pat:
                        for pat_info in bed_to_pat[bed_key]:
                            pstart = pat_info['start_t']
                            pend = pat_info['end_t']
                            
                            if (pstart <= start_t) and ((pend is None) or pd.isna(pend) or (pend == '') or (start_t < pend)):
                                # this is the patient info for this bed
                                ddict['pid'] = pat_info['pid'] if ddict['pid'] == missing_values[str] else ddict['pid']
                                ddict['visit_id'] = pat_info['visit_id'] if ddict['visit_id'] == missing_values[str] else ddict['visit_id']
                                ddict['patient_first_name'] = pat_info['first_name'] if ddict['patient_first_name'] == missing_values[str] else ddict['patient_first_name']
                                ddict['patient_last_name'] = pat_info['last_name'] if ddict['patient_last_name'] == missing_values[str] else ddict['patient_last_name']
                    # else no patient info, no change.

            data.extend(data_dict)
                        
        count += 1
    log.info(f"Read {count} HL7 messages with total of {len(data)} waveforms from {hl7_file}")
    return data, pat_infos


#%%
def _extract_field(target_segment: list, target_field: int):
    # filter for the target fields
    if target_field is not None:
        # log.debug(f"extracting fields {fields} from segment {segment} in {hl7_file}")
        output = target_segment[target_field] if (len(target_segment) > target_field) else ''
        output = "^".join(output) if type(output) is list else output
    else:
        # log.debug(f"extracting all fields from segment {segment} in {hl7_file}")
        output = []
        for segf in target_segment:
            output.append("^".join(segf) if type(segf) is list else segf)
        output = '|'.join(output)
    return output

#%%
def read_hl7_file_for_segment(hl7_file: str, fields: list = None, verify_message:bool = False):

    log.info(f"reading {hl7_file} for fields '{fields}'.")

    # organize the target fields a bit.
    target_fields = {}
    for field in fields:
        tokens = field.split(".")
        segment = tokens[0].upper()
        if len(tokens) == 1:
            f = None
        else:
            f = int(tokens[1])

        if segment not in target_fields:
            target_fields[segment] = []

        target_fields[segment].append(f)
        

    data = []
    count = 0
    with open(hl7_file, 'r') as file:
            
        # read the whole file
        file_content = file.read()
            
        # next split the file_content by double newlines
        # the double newlines are of the format \r\n\r\n or \n\n, but never \r\r
        # we do not see double newlines within a message between segments, only \r\n, \r, or \n
        # note that some tools only accept \r as line terminator.  ours does not have this limitation.
        # approach for split is to convert \r\n to \n, then target \n\n
        # should be faster than using regex.
        messages = file_content.replace('\r\n', '\n').replace('\r', '\n').split('\n\n')
        # messages = message_separator.split(file_content)
            
        for msg in messages:
            if len(msg) <= 0:
                continue
            
            # tokenize_hl7_message works has to split the segments anyways, so dont bother split then rejoin.
            # segs = segment_separator.split(msg)
            # hl7_msg = '\r'.join(segs)
            
            # parsed is the dict of segments.  segnames is name of hl7 segments.
            parsed, segnames = tokenize_hl7_message(msg)
            
            # first organize the parsed data
            omsg = HierarchicalMessage(parsed, segnames)
            
            # then extract the data and metadata and check
            if verify_message:
                try:
                    data_msg = hl7_data_factory(omsg)
                except ValueError as e:
                    log.error(f"in file {hl7_file}, message {count}: {e}")
                    continue
                
                if not isinstance(data_msg, HL7ADTData):
                    verify_result = _verify_hl7_msg(data_msg)
                    if len(verify_result['errs']) > 0:
                        log.error(f"Types: {verify_result['wavetypes']}:  {verify_result['errs']}")
                    if len(verify_result['warns']) > 0:
                        log.warning(f"Types: {verify_result['wavetypes']}:  {verify_result['warns']}")



            output = []
            
            msg_out = {}
            # process the MSH/PID/PV1 parts first - there should be a single entry per message.
            if 'MSH' in target_fields:
                for target_field in target_fields['MSH']:
                    if target_field == 1:
                        field_str = FIELD_SEPARATOR
                    else:
                        field_str = _extract_field(omsg.msh, target_field - 1) # MSH_1 is the field separator so the actual list indices are shifted by -1 
                    msg_out[f'MSH.{target_field}'] = field_str
            if 'PID' in target_fields:
                for target_field in target_fields['PID']:
                    field_str = _extract_field(omsg.pid, target_field)
                    msg_out[f'PID.{target_field}'] = field_str
            if 'PV1' in target_fields:
                for target_field in target_fields['PV1']:
                    field_str = _extract_field(omsg.pv1, target_field)
                    msg_out[f'PV1.{target_field}'] = field_str

            if ('OBR' in target_fields) and ('OBX' not in target_fields):
                obrs = [obr['obr'] for obr in omsg.obrs]  # get just the obr text lines
                for obr in obrs:
                    obr_out = msg_out.copy()
                    for target_field in target_fields['OBR']:
                        field_str = _extract_field(obr, target_field)
                        obr_out[f'_OBR.{target_field}'] = field_str
                    output.append(obr_out)
            elif ('OBR' not in target_fields) and ('OBX' in target_fields):
                obxs = [obx for obr in omsg.obrs for obx in obr['obx']]  # get all the obx text lines
                for obx in obxs:
                    obx_out = msg_out.copy()
                    for target_field in target_fields['OBX']:
                        field_str = _extract_field(obx, target_field)
                        obx_out[f'_OBX.{target_field}'] = field_str
                    output.append(obx_out)
            elif ('OBR' in target_fields) and ('OBX' in target_fields):
                for obr in omsg.obrs:
                    obr_out = msg_out.copy()
                    # proccess OBR line
                    obr_str = obr['obr']
                    for target_field in target_fields['OBR']:
                        field_str = _extract_field(obr_str, target_field)
                        obr_out[f'_OBR.{target_field}'] = field_str
                    # process OBX lines
                    for obx in obr['obx']:
                        obx_out = obr_out.copy()
                        for target_field in target_fields['OBX']:
                            field_str = _extract_field(obx, target_field)
                            obx_out[f'_OBX.{target_field}'] = field_str
                        output.append(obx_out)
                        
            else:
                output.append(msg_out)
                
            for out in output:
                # make 1 string
                keys = sorted(out.keys())
                out_str = []
                for key in keys:
                    out_str.append(f"{key}:{out[key]}")
                data.append("\t".join(out_str))
                                            
                count += 1
    log.info(f"Read {count} HL7 messages with total of {len(data)} items from {hl7_file} (for MSH, PV1, PID, counts should be same)")

    return data


# update patient info in a database.
def update_patient_info(history: pd.DataFrame, current: pd.DataFrame, patient_ids: list):
    # database has columns: ['hospital', 'bed_unit', 'bed_id', 'pid', 'visit_id', 'first_name', 'last_name', 'middle_initial', 'start_t', 'end_t']

    # create the new patient list and merge with recent rows
    pid_df = pd.DataFrame(patient_ids)
    
    # from database, get the most recent rows for each bed.  note current may have more than 1 patient for a bed.  use just the latest.
    if (current is not None) and (not current.empty):
        merged = pd.concat([current, pid_df], ignore_index=True)
    else:
        merged = pid_df

    # group by hospital, bed_unit, bed_id, sort by time.
    grouped = merged.groupby(['hospital', 'bed_unit', 'bed_id'])

    filtered_rows = []
    for _, group in grouped:
        
        # sort by time
        group = group.sort_values('start_t')
        
        # check the number of unique patients.
        n_pids = group['pid'].nunique()
        # if there is only 1 patient, we are already keeping the oldest one, so are done.
        if n_pids == 1:
            # always keep the oldest one and the newest one.
            filtered_rows.append(group.iloc[[0]].copy())
            
        elif n_pids > 1:
            # now we need to scan and find the last entry for each patient, and first entry for each patient.
            pgroups = group.groupby('pid')
            first_entries = pgroups.nth(0).copy()
            # Shift start_t back by 1 row for each patient group
            
            first_entries['end_t'] = first_entries['start_t'].shift(-1, fill_value=missing_values[str]).values
            filtered_rows.append(first_entries)


    current = pd.concat(filtered_rows, ignore_index=True).sort_values('start_t')
    next = current.drop_duplicates(subset=['hospital', 'bed_unit', 'bed_id'], keep='last').reset_index(drop=True)
    history = pd.concat([history, current], ignore_index=True).sort_values('start_t')
    history = history.drop_duplicates(subset=['hospital', 'bed_unit', 'bed_id', 'pid', 'visit_id', 'start_t'], keep='last').reset_index(drop=True)

    current_dict = {}
    for row in current.itertuples():
        key = (row.hospital, row.bed_unit, row.bed_id)
        if key not in current_dict:
            current_dict[key] = []
            
        current_dict[key].append({'pid': row.pid, 
                                  'visit_id': row.visit_id, 
                                  'first_name': row.first_name, 
                                  'last_name': row.last_name, 
                                  'middle_initial': row.middle_initial,
                                  'start_t': row.start_t, 
                                  'end_t': row.end_t})
        
    return history, current_dict, next



#%%
def convert_msg_to_json(msg: str) -> str:
    # tokenize_hl7_message works has to split the segments anyways, so dont bother split then rejoin.
    # segs = segment_separator.split(msg)
    # hl7_msg = '\r'.join(segs)

    # parsed is the dict of segments.  segnames is name of hl7 segments.
    parsed, segnames = tokenize_hl7_message(msg)

    # first organize the parsed data
    omsg = HierarchicalMessage(parsed, segnames)

    try:
        data_msg = hl7_data_factory(omsg)
    except ValueError as e:
        raise ValueError(f"[ERROR] could not create data message from HL7 message type {omsg.msh[8]}") from e
    
    return data_msg.to_row_json()
        
