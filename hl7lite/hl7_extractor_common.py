#%%
from hl7lite.hl7_tokenizer import get_with_default
from hl7lite.hl7_datatypes import missing_values
import re
import json

import logging
log = logging.getLogger(__name__)

#%%

with open("emory/bed_location_mappings.json", "r") as f:
    all_bed_location_mappings = json.load(f)
    _bed_wildcard_to_unit = all_bed_location_mappings["bed_wildcard_to_unit"]
    _unit_to_canonical = all_bed_location_mappings["unit_to_canonical"]
    _hospital_to_canonical = all_bed_location_mappings["hospital_to_canonical"]

    _canonical_units = set([unit for _, unit in _unit_to_canonical.values()])

    _canonical_hospitals = set([hosp for hosp, _ in _unit_to_canonical.values()])

    _hospital_to_canonical.update({hosp : hosp for hosp in _canonical_hospitals})


#%%

# multiple formats:
# PID|||10636478||XXXX^YYYY^^^^^L||YYYYMMDD|M|||||||ENG|||204022211|
# PID|||1208966602^^^A^MR||XXXX^YYYYone|||||||||||||100361906^^^A^MR|
# PID|||1208966603^^^A^MR||XXXX^YYYYtwo|||||||||||||100361906^^^A^MR|
# PID|||1208966604^^^A^MR||XXXX^YYYYfour|||||||||||||100361906^^^A^MR|
# PID|||18002928^^^A^MR||HARDADVISOR^NINE|||||||||||||100364371^^^A^MR|
# PID|||860679383^^^BERNOULLI MRN^MR||TEST81|||||||||||||ACC860679383
# PID|||I||ANDERSON^DWAYNE|
# PID|||||Not Admitted|
# PID|||||^^^^^^U|
# PID|||12121962||^^^^^^U|
def extract_pid(pid: list, pv1: list) -> tuple:
    
    # https://rhapsody.health/resources/hl7-pid-segment/
    # keeping pid 3 internal id, 18 account number (treat as visit id?), 5 patient name
    _mrn = get_with_default(pid, 'pid', 3)
    if isinstance(_mrn, list) and ((len(_mrn) == 5) or (len(_mrn) == 1)):
        _mrn = _mrn[0]
    elif (isinstance(_mrn, str)):
        _mrn = _mrn
        if len(_mrn) == 0 or _mrn == "I":
            _mrn = missing_values[str] 
    else:
        raise ValueError(f"Unexpected PID.3 format {pid[3]}. Expected list with 5 or 1 elements, or a non-empty string.")
    
    name = get_with_default(pid, 'pid', 5)
    if isinstance(name, str):
        _last_name = name
        _first_name = missing_values[str]
    elif isinstance(name, list):
        if len(name) == 1:
            _last_name = name[0]
            _first_name = missing_values[str]
        elif (len(name) == 2) or (len(name) == 7):
            # log.debug(f"Found patient name: {name}")
            _last_name = name[0]
            _first_name = name[1]
        else:
            raise ValueError(f"Unexpected PID.5 format {pid[5]}. Expected list with 1, 2, or 7 elements.")
    else:
        raise ValueError(f"Unexpected PID.5 format {pid[5]}. Expected str or list, got {type(pid[5])}")
    
    # while PV1.19 is supposed to be the visit id, it is a common practice to use pid.18 for visit/csn,
    _visit = get_with_default(pid, 'pid', 18)  # pid.18 visit number
    if isinstance(_visit, str):
        _visit = _visit
    elif isinstance(_visit, list):
        if (len(_visit) == 1) or (len(_visit) == 5):
            _visit = _visit[0]
            _visit = missing_values[str] if (_visit == '') else _visit
        else:
            log.warning(f"Unexpected PID.18 format {_visit}. Expected str or list with 1 or 5 elements.")
            _visit = missing_values[str]
    else:
        raise ValueError(f"Unexpected PID.18 format {_visit}. Expected str or list, got {type(_visit)}")

    if (_visit != missing_values[str]):
        return (_mrn, _visit, _first_name, _last_name)
    
    
    # fallback to pv1_19.   Don't have examples of this.
    _visit = get_with_default(pv1, 'pv1', 19)  # pv1.19 visit number
    if isinstance(_visit, str):
        _visit = _visit
    else:
        raise ValueError(f"Unexpected PV1.19 format {_visit}. Expected str or list, got {type(_visit)}")
        
    return (_mrn, _visit, _first_name, _last_name)


# ------------ Bed extraction ------------


#%%

# match pattern
euh_bed_re = re.compile(r'^[A-Z]\d{3}$')
def _canonicalize_bed_id_euh(bed_unit: str, bed_id: str) -> str:
    return (bed_id + '-01') if euh_bed_re.match(bed_id) else bed_id

# cases:
#   xxxx??, xxxx-??
#   T4??, t4??-01
#   ER??01, PA??01
#   NICU21??, NICU41??

# assume bed room id varies by 2 digits, even if only 1 is presented.
sub_re = re.compile(r'\d{1,2}$')  # trailing 1 or 2 digits
# replace 
EJCHNICUsub_re = re.compile(r'\d[A-Za-z]$') # trailing digit and letter
EUHmatch_re = re.compile(r'^([A-Za-z]\d)\d{2}(-01)?$')  # T4??-01 or T4??
EDHmatch_re = re.compile(r'^([A-Za-z]+)\d{2}(-?)01$')  # PA??01 or ER??01
def _bed_id_to_wildcard(bed_id: str) -> str:
    first2 = bed_id[:2]
    
    if first2 in ['ER', 'PA']:
        match = EDHmatch_re.match(bed_id)
        if not match:
            raise ValueError(f"Expected match for special bed prefix {bed_id} in unit {unit_name}")
        output = match.group(1) + '??' + (match.group(2) if match.group(2) else '') + '01'
    elif (len(bed_id) >= 4) and bed_id[0].isalpha() and bed_id[1].isdigit():
        # include "T434" but exclude "N28"
        match = EUHmatch_re.match(bed_id)
        if not match:
            raise ValueError(f"Expected match for special bed prefix {bed_id} in unit {unit_name}")
        output = match.group(1) + '??' + (match.group(2) if match.group(2) else '')
    elif bed_id.startswith('EMS'):
        output = 'EMS?'
    elif bed_id.startswith('NICU') and (bed_id[-1] in ['A', 'B']):
        # replace the last digit and letter.
        output = EJCHNICUsub_re.sub('??', bed_id)
    else:
        # replace the final 2 char.
        output = sub_re.sub('??', bed_id)

    return output


# for bed id extraction using EPIC format - 11 components.  should have 'DEPID' at the end.
def _extract_bed_id_epic(pv1_bed) -> tuple:
    if (pv1_bed[10] != 'DEPID'):
        raise ValueError(f"Unexpected PV1 bed identifier format {pv1_bed}. Expected EPIC format with 'DEPID' at the end.")
    
    # EPIC format, 11 components.
    # PV1.3.1	Epic Department ID
    # PV1.3.2	Room
    # PV1.3.3	Bed
    # PV1.3.4	Facility
    # PV1.3.5	Location Status
    # PV1.3.6	Person Location Type
    # PV1.3.7	Building
    # PV1.3.8	Floor
    # PV1.3.9	Epic Department Name
    # PV1.3.10	Comprehensive Location Identifier
    # PV1.3.11	Assigning Authority for Location
    
    # PV1-3-1: 10001056	
    # PV1-3-2: EUH G505
    # PV1-3-3: G505-01
    # PV1-3-4: 10001
    # PV1-3-5: R
    # PV1-3-9: EUH 5G ICU
    # PV1-3-11: DEPID
    
    # full example with pv1.2 == IP
    # PV1.2:IP        PV1.3:10001021^EUH B471^B471-01^10001^R^^^^EUH 4B NORTH^^DEPID
    # PV1.2:IP        PV1.3:10001029^EUH ED CDU 13^CDU 13^10001^R^^^^EUH EMERGENCY^^DEPID
    # PV1.2:IP        PV1.3:10001041^EUH E512^E512-01^10001^R^^^^EUH 5E ICU^^DEPID
    
    # get the bed string - 
    hospital = pv1_bed[3]  # use PV1.3.4 facility code or name
    bed_unit = pv1_bed[8]  # use PV1.3.9 Epic Department Name, replace spaces with underscores
    # bed_room = pv1_bed[1]  # use PV1.3.2 Room, last word
    bed_id = pv1_bed[2]  # use PV1.3.3 Bed, first part before '-'

    return (hospital, bed_unit, bed_id)
    

# list length of 9
def _extract_bed_id_neuron(pv1_bed) -> tuple:
    if pv1_bed[0].startswith('100'):
        # assume:
        # PV1.3.1	Epic Department ID
        # PV1.3.2	Room
        # PV1.3.3	Bed
        # PV1.3.4	Facility
        # PV1.3.9   monitor type?
            
        # 10001217^EUH T444^T444-01^EUH^^^^^MON
        hospital, bed_unit, bed_id = _extract_bed_id_sjjc(pv1_bed)
        # hospital = pv1_bed[3]  # pv1.3.4 = facility
        # bed_unit = missing_values[str]  # missing department name - to look up.
        # # bed_room = pv1_bed[1] # pv1.3.2
        # bed_id = pv1_bed[2] # pv1.3.3
        
    elif ('CART' in pv1_bed[0]) or ('4TN' in pv1_bed[0]):            
        # PV1.3.1	Epic Department ID
        # PV1.3.2	Room
        # PV1.3.3	Bed
        # PV1.3.4	Facility
        # PV1.3.9   monitor type?
        
        # ESJH-ANES-CART-AD^^^^^^^^MON
        # ESJH-ANES-CART-AE^^^^^^^^ANES
        # EUH-4TN-T434^^^^^^^^MON  # 9 components total
        (hospital, bed_unit, bed_id) = _extract_bed_id_str(pv1_bed[0])
        # leave the MON/PERF/ANES/OXI off for now.
        # bed_auth = pv1_bed[8] # pv1.3.9 = monitor type?
        # bed_id = bed_id + '-' + bed_auth  # append monitor type to bed id to make unique.

    else:
        raise ValueError(f"Unexpected PV1 bed identifier format {pv1_bed}.")

    return (hospital, bed_unit, bed_id)


# should be 4 entries.
def _extract_bed_id_sjjc(pv1_bed) -> tuple:
    # two forms.  one with Hospital in the 4th position, one without
    if pv1_bed[3].endswith('Hospital'):
        #ICU^^ICU 7^Emory Johns Creek Hospital
        #PV1||I|ARU^^ARU5^Emory St Joseph's Hospital|
        hospital = pv1_bed[3]        
        bed_unit = pv1_bed[0]
        bed_id = pv1_bed[2]
        if (bed_unit == 'PACU'):
            if hospital == 'Emory Johns Creek Hospital':
                bed_id = 'JC' + bed_id
            elif hospital == 'Emory St Joseph\'s Hospital':
                bed_id = 'SJ' + bed_id
    elif pv1_bed[0].startswith('100'):
        # ['10001021', 'EUH B462', 'B462-01', '10001']
        # ['10001029', 'EUH ED 21', '21', '10001']
        hospital = pv1_bed[3]  # use PV1.3.4 facility code or name
        bed_unit = pv1_bed[0]  # lookup by department number. 
        bed_id = pv1_bed[2]        
    else:
        #ICU^ICU 17^17^1
        hospital = missing_values[str]
        bed_unit = pv1_bed[0]
        bed_id = pv1_bed[1]

    return (hospital, bed_unit, bed_id)

# 2 entries
icu41_re = re.compile(r'^(410\d)-(\d\d)$')
def _extract_bed_id_euh(pv1_bed) -> tuple:
    # two forms.  EUHM or EUH

    if pv1_bed[0] in _canonical_hospitals:
        # EUHM^4107-06  (alarm)
        # EUHM^PICU L110
        hospital = pv1_bed[0]
        bed_id = pv1_bed[1]
        
        if (bed_id.startswith('PICU L')):
            bed_unit = 'PICU'
            if len(bed_id) - len("PICU L") == 2:
                # PICU L10 -> MHPICUL110
                bed_id = 'MHPICUL1' + bed_id[len("PICU L"):]
            else:
                bed_id = 'MHPICUL' + bed_id[len("PICU L"):]
            
        elif (bed_id.startswith("4107") or bed_id.startswith("4108")):
            #4107-06 -> 4107-600
            #4108-01 -> 4108-100
            bed_unit = "41ICU"
            match = icu41_re.match(bed_id)
            if match:
                bed_id = match.group(1) + '-' + str(int(match.group(2)) * 100)
        else:
            bed_unit = _lookup_unit_from_bed(bed_id, None)
            if bed_unit is None:
                if '-' in bed_id:
                    #4107-06
                    bed_unit = bed_id.split('-')[0]
                else:
                    bed_unit = bed_id.split(' ')[0]

    else:
        #G5ICU^G505
        hospital = missing_values[str]
        bed_unit = pv1_bed[0]
        bed_id = pv1_bed[1]
        if (bed_unit == 'NLIC'):
            # NLIC^2035 -> NLICU^NL2035
            bed_unit = 'NLICU'
            bed_id = 'NL' + bed_id
        elif (bed_unit == 'POHA'):
            # POHA^11 -> EHPOHA^EHPOHA11
            bed_unit = 'EHPOHA'
            bed_id = bed_unit + bed_id
        elif (bed_unit == 'NLPACU'):
            # NLPACU^21 -> NLPACU^NLPACU21
            bed_id = bed_unit + bed_id
        # elif (bed_unit == 'EHIR') or (bed_unit == 'EUHIR'):
        #     # EHIR^14 -> EUH Interventional Radiology^14
        #     bed_unit = 'EUH INTERVENTIONAL RADIOLOGY'
        # elif (bed_unit == 'EHECHO'):
        #     # EHECHO^MAIN1 -> EUH ECHO^MAIN1
        #     bed_unit = 'EUH ECHO'
       
            
        bed_id = _canonicalize_bed_id_euh(bed_unit, bed_id)

    return (hospital, bed_unit, bed_id)

# there should be 1 string.
def _extract_bed_id_str(pv1_bed) -> tuple:
    # and others.
    
    if pv1_bed == 'WH-TEST':
        #WH-TEST
        hospital = missing_values[str]
        bed_unit = pv1_bed
        bed_id = pv1_bed
        
    else:
        #EJCH-3S-IC01
        #EUH-4TN-T434
        #ESJH-ANES-CART-AD
        tokens = pv1_bed.split('-')
        if len(tokens) == 3:
            hospital = tokens[0]
            bed_unit = tokens[1]
            bed_id = _canonicalize_bed_id_euh(bed_unit, tokens[2])
        elif (tokens[2] == 'CART'):
            hospital = tokens[0]
            bed_unit = '-'.join(tokens[0:3])
            bed_id = pv1_bed
        else:
            raise ValueError(f"Unexpected PV1 bed identifier format {pv1_bed}.")


    return (hospital, bed_unit, bed_id)

# prefix_re = re.compile(r'^([A-Za-z]+[ -]*)[0-9]$')
def _lookup_unit_from_bed(bed_id, default : str = missing_values[str]) -> str:

    bed_wildcard = _bed_id_to_wildcard(bed_id)
    return _bed_wildcard_to_unit.get(bed_wildcard, default)


def _canonicalize_location_id(orig, hospital, bed_unit, bed_id) -> tuple:
    out_hospital = hospital
    out_bed_unit = bed_unit
    out_bed_id = bed_id
    
    # if unit starts with hospital name or V, likely it's a ADT entry
    likely_adt = (bed_unit.split(' ')[0] in _canonical_hospitals) or (bed_unit.startswith('V '))

    
    # does not matter if hospital is missing or not.
    if (bed_unit == missing_values[str]):
        # missing bed unit, get from bed_id
        log.info("Missing bed unit,  looking up by bed_id {bed_id}")
        ca_unit = _lookup_unit_from_bed(bed_id, missing_values[str])
        
        # if still not found, error
        if ca_unit == missing_values[str]:
            log.error(f"hospital and bed unit in '{orig}' missing. bed id '{bed_id}' not found in bed_to_unit mapping.")
    else:
        ca_unit = bed_unit
    
    # now check if ca_unit is likely from epic
    if likely_adt:
        # unit name begins with V_<hospital>, likely from ADT and is canonical.
        out_hospital = _hospital_to_canonical.get(hospital, missing_values[str])
        out_bed_unit = ca_unit
    else:
        # need to look up unit name.
        out_hospital, out_bed_unit = _unit_to_canonical.get(ca_unit, (missing_values[str], missing_values[str]))
        if out_bed_unit == missing_values[str]:
            # unit is not enough. look up by bed id.
            log.debug(f"bed unit '{bed_unit}' not found in _unit_to_canonical mapping.  looking up by bed_id {bed_id}")
            ca_unit = _lookup_unit_from_bed(bed_id, None)
            out_hospital, out_bed_unit = _unit_to_canonical.get(ca_unit, (missing_values[str], missing_values[str]))

        if out_bed_unit == missing_values[str]:
            log.error(f"unit in '{orig}' missing. bed unit '{bed_unit}' not found in _unit_to_canonical mapping.")

    if hospital != missing_values[str]:
        canon_hosp = _hospital_to_canonical.get(hospital, missing_values[str])
        if out_hospital != canon_hosp:
            log.debug(f"{orig} hospital name from unit {out_hospital} does not match hospital lookup {canon_hosp}")

    if (out_hospital not in _canonical_hospitals) or (not likely_adt and (out_bed_unit not in _canonical_units)):
        log.debug(f"hosp or unit '{hospital}' '{bed_unit}' '{bed_id}' -> '{out_hospital}' '{out_bed_unit}' '{out_bed_id}'")
    elif ((bed_id != missing_values[str]) and (out_bed_id == missing_values[str])):
        log.error(f"mapping bed '{hospital}' '{bed_unit}' '{bed_id}' -> '{out_hospital}' '{out_bed_unit}' '{out_bed_id}'")
    
    
    # return (out_hospital, out_bed_unit.replace(' ', '_'), out_bed_id.replace(' ', '_'))  
    return(out_hospital, out_bed_unit, out_bed_id)

# Compile a regular expression to parse a string of the form EUH-4TN-T434 into 3 parts separated by '-'
def extract_bed_id(pv1_bed) -> tuple:
    
    if pv1_bed is None or len(pv1_bed) == 0:
        raise ValueError(f"PV1 bed identifier is missing or empty: {pv1_bed}")

    # get the bed string - multiple formats to parse
    if isinstance(pv1_bed, list):
        # log.debug(f"pv1_bed is a list: {pv1_bed}")
        if len(pv1_bed) == 11:
            (hospital, bed_unit, bed_id) = _extract_bed_id_epic(pv1_bed)
        elif len(pv1_bed) == 9:
            (hospital, bed_unit, bed_id) = _extract_bed_id_neuron(pv1_bed)
        elif (len(pv1_bed) == 4) or (len(pv1_bed) == 5):
            (hospital, bed_unit, bed_id) = _extract_bed_id_sjjc(pv1_bed)
        elif len(pv1_bed) == 2:
            (hospital, bed_unit, bed_id) = _extract_bed_id_euh(pv1_bed)
        elif len(pv1_bed) == 1:
            # single string in a list  should not happen.
            if pv1_bed[0] == '':
                raise ValueError(f"WARNING:  pv1_bed not populated. {pv1_bed}")
            else:
                (hospital, bed_unit, bed_id) = _extract_bed_id_str(pv1_bed[0])
        else:
            raise ValueError(f"Unexpected PV1 bed list form  {pv1_bed}. Expected list with 1, 2, 4, 5 or 9 elements, got {len(pv1_bed)} elements.")
    elif isinstance(pv1_bed, str):
        if pv1_bed == '':
            raise ValueError(f"WARNING:  pv1_bed not populated. {pv1_bed}")
        else:
            (hospital, bed_unit, bed_id) = _extract_bed_id_str(pv1_bed)

    else:
        raise ValueError(f"Unexpected PV1 bed format {pv1_bed}. Expected str or list, got {type(pv1_bed)}")

    # bed_str = "|".join([hospital, bed_unit, bed_id])
    return _canonicalize_location_id(pv1_bed, hospital, bed_unit, bed_id)


