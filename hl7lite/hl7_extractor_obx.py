

#%%
from hl7lite.hl7_tokenizer import get_with_default
from hl7lite.hl7_datatypes import missing_values

# extract and transform the specified obx fields.

# %%
# example OBR
# [['OBR'],
#   ['1'],
#   [''],
#   [[['AWS_Data_00011dd26e7f0018bba4'],
#     ['CAPSULE'],
#     ['6226F836FEE5445B'],
#     ['EUI-64']]],
#   [[['69121'], ['MDC_OBS_WAVE_CTS'], ['MDC']]],
#   [''],
#   [''],
#   ['20250228155958.138-0500'],
#   ['20250228155959.137-0500'],
#   [''],
#   ['EUH-4TN-T435'],
#   [''],
#   [''],
#   ['EUH-4TN-T435'],
#   [''],
#   [''],
#   [''],
#   [''],
#   [''],
#   [''],
#   [''],
#   ['DatexA_5.3.20.34_CARESCAPE B850_Datex_{6152B923-11BA-4536-A7A0-D04F3749C910}'],
#   ['']],
        
# handle OBX.3
def extract_signal_name(obx_fields: list) -> tuple:
    sig_name = get_with_default(obx_fields, 'obx', 3)
    # get date type and value
    if isinstance(sig_name, list):
        # if not 3 parts, then assume no scheme.
        if (len(sig_name) == 1):
            code = f"NO_SCHEME:{sig_name[0]}"
            obx_name = sig_name[0]
        elif (len(sig_name) == 2):
            code = f"NO_SCHEME:{sig_name[0]}"
            obx_name = sig_name[1]
        elif (len(sig_name) == 3):
            # assume code^codename^scheme
            code = f"{sig_name[2]}:{sig_name[0]}"
            
            # if signame[2] is CAPSULE, then obx_name is UNKNOWN.  We then check for channel id in obx_container.
            obx_name = sig_name[1]
            if obx_name == '':
                # format is y^^CAPSULE
                obx_name = code
            # general format is y^MDCName^MDC
        else: 
            raise ValueError(f"Strange sig_name {sig_name}, obx list: {obx_fields}")
            
    elif isinstance(sig_name, str):
        # code = f"NO_SCHEME:NO_CODE"
        # obx_name = sig_name
        raise ValueError(f"Strange sig_name {sig_name}.  Should be a list.  obx list: {obx_fields}")
        
    return (obx_name, code)

# handle OBX.6                    
def extract_signal_uom(obx_fields: list) -> str:
    unit_of_meas = get_with_default(obx_fields, 'obx', 6)
    if isinstance(unit_of_meas, list):
        if (len(unit_of_meas) == 1):
            unit_of_meas = unit_of_meas[0]
        elif (len(unit_of_meas) == 3):
            unit_of_meas = unit_of_meas[1]
        elif (len(unit_of_meas) == 0):
            unit_of_meas = missing_values[str]
        else:
            raise ValueError(f"Strange unit_of_meas {unit_of_meas}")        
    else:
        raise ValueError(f"Strange unit_of_meas {unit_of_meas}.  should be a list.")
    return unit_of_meas

# get OBX.4 and OBX.21 to form a channel id.
def extract_signal_id(obx_fields: list) -> str:
    obx_container = get_with_default(obx_fields, 'obx', 4)     # OBX.4 observation sub id
    # get channel id.  this is in case there are channels with the same name.
    channel_id = get_with_default(obx_fields, 'obx', 21)  # OBX.21 - some crazy number, may be useful.
    # # if channel_id is None, and  obx_container is a.b.x.y.  extract x. and overwrite the channel_id
    # if (channel_id == None) and (obx_container.startswith("1.99.") or obx_container.startswith("1.20.")):
    #     # get the x value
    #     channel_id = int(obx_container.split(".")[2])
    return ":".join([obx_container, channel_id])


#%%
# for extracting additional patient info from vitals OBX
# ====== from OBX - may not be proper.
# combined name: 1930^^CAPSULE
# first name: 3185^^CAPSULE, 8338^^CAPSULE, 67933^MDC_ATTR_PT_NAME_GIVEN^MDC
# last name: 2901^^CAPSULE, 8340^^CAPSULE, 67932^MDC_ATTR_PT_NAME_FAMILY^MDC
# middle initial: 67935^MDC_ATTR_PT_NAME_MIDDLE^MDC
# patient id: CAPSULE_3426, MDC_ATTR_PT_ID (not a good one)
# visit id: CAPSULE_9569?
# location id: 50121^CAPSULE_ATTR_ID_LOCATION^CAPSULE  - should be same as PV1?
#    else hospital = 50125^CAPSULE_ATTR_ID_FACILTY^CAPSULE
#         unit = 50124^CAPSULE_ATTR_ID_POC^CAPSULE
#         bed id = 68037^MDC_ATTR_ID_BED^MDC
# ======= from Guy


# _OBR.21:BlinkA_5.2.0.3_TwitchView_Blink Device Company
# _OBR.21:DatexA_5.2.17.1_CARESCAPE B850_Datex
# _OBR.21:DatexA_5.2.18.4_CARESCAPE B650_Datex
# _OBR.21:DatexA_5.2.18.4_CARESCAPE B850_Datex
# _OBR.21:DatexA_5.3.20.34_CARESCAPE B850_Datex
# _OBR.21:DatexOhmedaCom_5.1.11.1_Aisys CS2_Datex
# _OBR.21:DatexOhmedaCom_5.3.14.11_Aisys CS2_Datex
# _OBR.21:DatexOhmedaCom_5.3.14.11_Aisys_Datex
# _OBR.21:DatexOhmedaCom_5.3.14.11_Avance CS2_Datex
# _OBR.21:DatexOhmedaCom_5.3.14.11_Avance_Datex
# _OBR.21:DragerMedibus_5.3.22.12_Apollo_Dr�ger
# _OBR.21:GECarescapeC_5.4.0.33_Carescape Connect_GE Healthcare
# _OBR.21:InvivoB_5.1.6.5_Expression IP5_Invivo
# _OBR.21:MasimoD_5.3.4.6_Root_Masimo
# _OBR.21:PhilipsDataExport_5.3.19.21_IntelliVue MX750_Philips Medical Systems
# _OBR.21:PhilipsDataExport_5.3.19.21_IntelliVue MX800_Philips Medical Systems
# _OBR.21:PhilipsL_5.4.1.16_IntelliVue Information Center (IIC) iX (HSOI)_Philips Medical Systems
# _OBR.21:PhilipsL_5.4.3.30_IntelliVue Information Center (IIC) iX (HSOI)_Philips Medical Systems
# _OBR.21:SomaneticsA_5.3.5.9_InVos Cerebral/Somatic Oximeter 5100C_Somanetics
# _OBR.21:SorinA_5.2.2.12_S5_Sorin
_pid_codes = {
    "GECarescap": {  #GECarescapeC
        'mrn': 'CAPSULE:50101',
        'account': 'CAPSULE:50112',
        'visit': 'CAPSULE:50113',
        'first_name': '',
        'last_name': '',
        'middle_initial': '',
        'name': 'CAPSULE:50102'  # last^first^suffix
    },
    'GEUnityIS': {
        'mrn': 'CAPSULE:1929',
        'account': '',
        'visit': '',
        'first_name': 'CAPSULE:2901',
        'last_name': 'CAPSULE:3185',
        'middle_initial': '',
        'name': ''
    },
    'PhillipsI': {   #PhillipsIIC
        'mrn': 'CAPSULE:1929',
        'account': '',
        'visit': 'CAPSULE:6544',
        'first_name': 'CAPSULE:8338',
        'last_name': 'CAPSULE:8340',
        'middle_initial': '',
        'name': 'CAPSULE:1930'  # last^first^middle
    },
    'defaultcodes': {
        'mrn': 'CAPSULE:3426',
        'account': '',
        'visit': 'CAPSULE:9569',
        'first_name': 'MDC:67933',
        'last_name': 'MDC:67932',
        'middle_initial': 'MDC:67935',
        'name': ''
    }
}


#%%
# extract patient names from HL7 monitoring of patient OBX segment
def extract_pid_from_obx(obr_fields: list, obxes: list) -> list:
    # patient names can be present in PID, or in one of the OBX fields from "monitoring of patient"
    # read from patient name to bed mapping file, with timestamps

    source = get_with_default(obr_fields, 'obr', 21)
    start_t = get_with_default(obr_fields, 'obr', 7)
    first_name = missing_values[str]
    last_name = missing_values[str]
    pid = missing_values[str]
    visit = missing_values[str]
    
    codes = _pid_codes.get(source[:9], _pid_codes['defaultcodes'])
    mrn_code = codes.get('mrn', '')
    visit_code = codes.get('visit', '')
    first_name_code = codes.get('first_name', '')
    last_name_code = codes.get('last_name', '')
    name_code = codes.get('name', '')

    for obx_fields in obxes:  # obx field list
        (_, code) = extract_signal_name(obx_fields)  # OBX.3 observation identifier
        try:
            data = get_with_default(obx_fields, 'obx', 5)  # OBX.5 observation value    
        except TypeError as e:
            raise ValueError(f"Strange OBX.5 data for code {code}, obx fields: {obx_fields}") from e
        if code == first_name_code:
            first_name = data if first_name == '' else first_name
        elif code == last_name_code:
            last_name = data if last_name == '' else last_name
        elif code == name_code:
            tokens = data.split('^')
            last_name = tokens[0] if last_name == '' else last_name
            first_name = tokens[1] if first_name == '' else first_name
        elif code == mrn_code:
            pid = data
        elif code == visit_code:
            visit = data if visit == '' else visit

        # fix if lastname or first name is '""'
        if first_name in ['""', 'U']:
            first_name = missing_values[str]
        if last_name in ['""', 'U']:
            last_name = missing_values[str]

    return pid, visit, first_name, last_name, missing_values[str], start_t

