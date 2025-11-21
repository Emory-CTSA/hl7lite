from enum import Enum
from hl7lite.hl7_ds import HL7ECGData
from datetime import timedelta
import numpy as np
from hl7lite.hl7_datatypes import fix_time, parse_time

import logging
log = logging.getLogger(__name__)

#%%


def compute_duration(time1: str, time2: str):
    return parse_time(fix_time(time2)) - parse_time(fix_time(time1))


def compare_times(time1: str, time2: str):
    t1 = parse_time(fix_time(time1))
    t2 = parse_time(fix_time(time2))
    if t1 < t2:
        return 1
    elif t1 == t2:
        return 0
    else:
        return -1

#%%
class VerifyResultType(Enum):
    OK = "PASS"
    ERR_BAD_PARSE = "ERR: BAD_PARSE"
    ERR_OBR_OBX_ENV_MISMATCH = "ERR: OBR_OBX_ENV_MISMATCH"
    ERR_OBX_MISSING_TIME = "ERR: OBX_MISSING_TIME"
    ERR_UNKNOWN = "ERR: UNKNOWN"
    WARN_OBR_OBX_START_MISMATCH = "WARN: OBR_OBX_START_MISMATCH"
    WARN_OBR_OBX_END_MISMATCH = "WARN: OBR_OBX_END_MISMATCH"
    ERR_PV1_OBR_BED_MISMATCH = "ERR: PV1_OBR_BED_MISMATCH"
    ERR_OBR_MULTIPLE_ENVS = "ERR: OBR_MULTIPLE_ENVS"
    ERR_OBR_MISSING_START = "ERR: OBR_MISSING_START"
    ERR_OBR_MISSING_END = "ERR: OBR_MISSING_END"
    ERR_OBR_MISSING_EVENT_TIME = "ERR: OBR_MISSING_EVENT_TIME"
    WARN_OBR_MULTIPLE_START = "WARN: OBR_MULTIPLE_START"
    WARN_OBR_MULTIPLE_END = "WARN: OBR_MULTIPLE_END"
    WARN_OBR_MULTIPLE_EVENT_TIMES = "WARN: OBR_MULTIPLE_EVENT_TIMES"
    ERR_OBR_END_BEFORE_START = "ERR: OBR_END_BEFORE_START"
    WARN_MSH_BEFORE_OBR_START = "WARN: MSH_BEFORE_OBR_START"
    WARN_MSH_BEFORE_OBR_END = "WARN: MSH_BEFORE_OBR_END"
    ERR_DUR_MISMATCH_SAMPLE_TIMES = "ERR: DUR_MISMATCH_SAMPLE_TIMES"
    ERR_MSH_MISSING_TIME = "ERR: MSH_MISSING_TIME"

#%%
# verify the data.
def _verify_hl7_msg(msg: HL7ECGData, check_start_times: bool = False, verbose:bool = True):
    # check MSH time is later than all signal end time
    errs = set()
    warns = set()

    #private fields.
    all_beds = []
    bed_vs_pv1 = []
    envs = set()
    all_starts = []
    all_ends = []
    starts = set()
    ends = set()
    event_times = set()
    durations = []
    mshtime_gt_start = []
    mshtime_gt_end = []
    delays = []
    all_durs = []
    full_coverage = []
    millisec = timedelta(milliseconds=1)
    bad_parse = []
    wavetypes = set()
    
    if (msg.msh_time is None) or (len(msg.msh_time) == 0):
        if verbose:
            log.error(f"ERROR: missing MSH time")
        errs.add(VerifyResultType.ERR_MSH_MISSING_TIME)    
    
    for signal in msg.signals:
        all_beds.append(signal.bed)
        bed_vs_pv1.append(signal.bed == msg.pv1_bed)
        envs.add(signal.env)
        wavetypes.add(signal.type)
        
        if (signal.type in ['monitoring of patient', 'MDC_EVT_ALARM']) and ((signal.end_t is None) or (len(signal.end_t) == 0)):
            event_times.add(signal.start_t)
            delays.append(compute_duration(signal.start_t,msg.msh_time ))
        else:
            starts.add(signal.start_t)
            ends.add(signal.end_t)
            dur = compute_duration(signal.start_t, signal.end_t)
            durations.append(dur)
            all_ends.append(signal.end_t)
            mshtime_gt_end.append(compare_times(signal.end_t, msg.msh_time) == 1)
            delays.append(compute_duration(signal.end_t,msg.msh_time ))
            
            # compute samples * pd_samp_ms
            pd_samp_ms = None
            samp = None
            for key, obx in signal.attributes.items():
                if (obx['valtype'] == "NA"):
                    samp = len(obx['value'])
                elif "TIME_PD_SAMP" in key:
                    # type conversion handled by hl7_parser
                    pd_samp_ms = obx['value']
                    
            if (pd_samp_ms is None) or (samp is None):
                raise ValueError(f"ERROR: missing pd_samp_ms or samp. {msg}")
            comp_dur = pd_samp_ms * samp
            dur_milli = dur / millisec
            all_durs.append((dur_milli, comp_dur))
            full_coverage.append(np.abs(dur_milli - comp_dur) < 1.0)
            
            
        all_starts.append(signal.start_t)
        mshtime_gt_start.append(compare_times(signal.start_t, msg.msh_time) == 1)
        
        # all_envs = []
        # env_vs_obx = []
        obx_times = set()
        all_obx_times = []
        time_vs_obx = []
        bad_parse = []
        for key, obx in signal.attributes.items():
            # all_envs.append(obx['control_id'])
            # env_vs_obx.append(signal.env == obx['control_id'])
            obx_time = obx['obx_time']
            obx_times.add(obx_time)
            all_obx_times.append(obx_time)
            time_vs_obx.append(signal.start_t == obx_time)
            bad_parse.append(obx['value'] == ')')
            
        # if not all(env_vs_obx):
        #     selected = [v for k,v in zip(env_vs_obx, all_envs) if not k]
        #     if verbose:
        #         log.error(f"ERROR: not all OBX env have the same env as OBR. {signal.env} <-> {selected}")
        #     errs.add(VerifyResultType.ERR_OBR_OBX_ENV_MISMATCH)
        
        if (len(obx_times) == 0) or (None in obx_times) or ('' in obx_times):
            if verbose:
                log.error(f"ERROR: missing OBX time.  OBR time {signal.start_t}")
            errs.add(VerifyResultType.ERR_OBX_MISSING_TIME)
        
        if check_start_times and (not all(time_vs_obx)):
            selected = [ v for k, v in zip(time_vs_obx, all_obx_times) if not k]
            if verbose:
                log.warning(f"WARNING: OBX times do not match OBR start time {signal.start_t} <-> {selected}")
            warns.add(VerifyResultType.WARN_OBR_OBX_START_MISMATCH)
        
        if any(bad_parse):
            if verbose:
                log.error(f"ERROR: some values are not parsed correctly {bad_parse}")
            errs.add(VerifyResultType.ERR_BAD_PARSE)
        
    # check bed is consistent.
    if not all(bed_vs_pv1):
        selected = set([v for k,v in zip(bed_vs_pv1, all_beds) if not k])
        if verbose:
            log.error(f"ERROR: not all OBR have bed marked same as PV1 bed. PV1: {msg.pv1_bed}, OBR {selected}")
        errs.add(VerifyResultType.ERR_PV1_OBR_BED_MISMATCH)
        
    # check env is same for all signals
    if len(envs) > 1:
        if verbose:
            log.error(f"ERROR: not all OBR have the same env: {envs}")
        errs.add(VerifyResultType.ERR_OBR_MULTIPLE_ENVS)
            
    if (len(starts) == 0) or (None in starts) or ('' in starts):
        if verbose:
            log.error(f"ERROR: missing OBR start time")
        errs.add(VerifyResultType.ERR_OBR_MISSING_START)
    if (len(ends) == 0) or (None in ends) or ('' in ends):
        if verbose:
            log.error(f"ERROR: missing OBR end time")
        errs.add(VerifyResultType.ERR_OBR_MISSING_END)
    if (None in event_times) or ('' in event_times):
        if verbose:
            log.error(f"ERROR: missing OBR event time.  OBR time {signal.start_t}")
        errs.add(VerifyResultType.ERR_OBR_MISSING_EVENT_TIME)
    
    # check if all signals in a message have the same starting and ending time
    if check_start_times and (len(starts) > 1):
        if verbose:
            log.warning(f"WARNING: not all OBR wave have the same start_time {starts}")
        warns.add(VerifyResultType.WARN_OBR_MULTIPLE_START)
        
    if check_start_times and (len(ends) > 1):
        if verbose:
            log.warning(f"WARNING: not all OBR wave have the same end_time {ends}")
        warns.add(VerifyResultType.WARN_OBR_MULTIPLE_END)

    if len(event_times) > 1:
        if verbose:
            log.error(f"ERROR: not all OBR event times are the same {event_times}")
        warns.add(VerifyResultType.WARN_OBR_MULTIPLE_EVENT_TIMES)
    
    zerodur = timedelta(seconds= 0)
    if any([dur <= zerodur for dur in durations]):
        if verbose:
            log.error(f"ERROR: not all OBR have the end time greater than start time for {signal.type}")
        errs.add(VerifyResultType.ERR_OBR_END_BEFORE_START)
    
    if check_start_times and (not all(mshtime_gt_start)):
        selected = [ v for k, v in zip(mshtime_gt_start, all_starts) if k != 1]
        if verbose:
            log.warning(f"WARNING: MSH time is not later than all OBR start time. {msg.msh_time} <-> {selected}")
        warns.add(VerifyResultType.WARN_MSH_BEFORE_OBR_START)
    
    if check_start_times and (not all(mshtime_gt_end)):
        selected = [ v for k, v in zip(mshtime_gt_end, all_ends) if k != 1]
        if verbose:
            log.warning(f"WARNING: MSH time is not later than all OBR end time. {msg.msh_time} <-> {selected}")
        warns.add(VerifyResultType.WARN_MSH_BEFORE_OBR_END)
        
    if not all(full_coverage):
        selected = [v  for k, v in zip(full_coverage, all_durs) if not k]
        if verbose:
            log.error(f"ERROR: number of samples * intersample spacing does not match total duration. {selected}")
        errs.add(VerifyResultType.ERR_DUR_MISMATCH_SAMPLE_TIMES)
    
    # calculate the delays between msh_time and signal end time, after parsing time string
    # log.info(f"Average Delays for msg {msg.pv1_bed}: {sum(delays, timedelta(0))/len(delays)}")    
    
    return {"errs": errs, "warns": warns, "wavetypes": wavetypes}


            