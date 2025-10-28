from hl7lite.hl7_datatypes import missing_values
from hl7lite.hl7_tokenizer import get_with_default
from hl7lite.hl7_extractor_common import extract_bed_id, extract_pid
from hl7lite.hl7_extractor_obx import extract_signal_name, extract_signal_id, extract_signal_uom, extract_pid_from_obx
import numpy as np
import pandas as pd
from hl7lite.hl7_waveform import channel_to_type
import json

import logging
log = logging.getLogger(__name__)

# organization:
# MSH - 1 message
# PID - patient info - PID_3 is empi_nbr, PID_18 is encounter_id
# PV1 - visit info
# OBR - 1 or more
# OBX - 1 or more, associated with prev OBR

# parsing:  the data type in OBX.2 applies to value in OBX.5
#    all other fields are treated as strings, except for explicitly defined ones
#       that are known to be of particular type (e.g. date time in OBR.7, OBR.8, OBX.14)


# MAY WANT TO LOAD and use GATEWAY CAPSULE BEDS excel file - there are canonical bed ids in there.

class HierarchicalMessage:
    def __init__(self, hl7_parsed:list, seg_names: set):
        self.hl7_parsed = hl7_parsed
        # check presence of the fields.
        keywords = set(['MSH', 'PID', 'PV1'])
        if len(keywords - seg_names) > 0:
            raise ValueError(f"ERROR: missing some keywords {keywords - seg_names}")
        # elif len(seg_names - keywords) > 0:
        #     log.warn(f"WARNING: unexpected keywords {seg_names - keywords}")
        
        # waveform, alarm, patient monitoring have OBR/OBX segments.
        # ADT segments:
        # HL7 ADT segments
        #* MSH,
        #* PID (potentially more complex)
        #* EVN - triggering event
        # ROL xn - role (of a person/provider)
        # NK1 xn - related parties, next of kin - 3 different formats.
        #* PV1 - location, patient info
        #* PV2 - patient info continued - admit, discharge reasons, etc.
        # AL1 xn - allergy
        #* DG1 xn - diagnosis
        # GT1 - guarantor
        # IN1 xn - insurance
        # ZPD - locally defined.
        # ZPV - locally defined.
        # ZIF - locally defined.
        self.obrs = []
        self.pv2 = None
        self.evn = None
        self.dg1 = []
        for fields in hl7_parsed:
            label = fields[0]
            if label == "MSH":
                self.msh = fields
            elif label == "PID":
                self.pid = fields
            elif label == "PV1":
                self.pv1 = fields
                
            elif label == "OBR":
                self.obrs.append({'obr': fields, 'obx': []})
            elif label == "OBX":
                # add to the last one present.
                self.obrs[-1]['obx'].append(fields)
                
            elif label == "PV2":
                self.pv2 = fields
            elif label == "EVN":
                self.evn = fields
            elif label == "DG1":
                self.dg1.append(fields)
            # the others are ignored.
            
    def __repr__(self):
        obrstr = ''
        for obr in self.obrs:
            obrstr += f"\tOBR {obr['obr'][1]}\n"
            for obx in obr['obx']:
                obrstr += f"\t\tOBX {obx[1]}\n"
        
        return f"MSH\n\tPID\n\tPV1\n{obrstr}"




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

# conditions for parsing.
# a waveform signal occurs if OBR.4 is "MDC_OBS_WAVE_CTS". This leads to OBX.1 of NA for an OBX.
#   for some OBX line where OBX.1 == NA, we also observe OBX.4 is 1.0.0.1, or 1.99.x.27, or 1.20.x.y.z
#   for some OBX line where OBX.3 has the wave name, say in the MDC coding form.
#   OBX.3 as "27^^capsule", which will have OBX.4 as 1.99.x.27, or it could be changed to 1.20.x.51920.1
#   signal name may not be specified or may be the same for multiple channels.
#       OBX.21 may be have a channel id for some devices
#       or the third entry in OBX.4 may indicate the channel id
#       or it may need to be assigned 0.
class Signal:
    # organization:  metadata include start, end, type, target, source, and each OBX is a separate attribute.
    def __init__(self, obr: dict):
        # https://hl7.docs.careevolution.com/segments/obr.html
        # https://hl7.docs.careevolution.com/segments/obx.html
        obr_field_list = obr['obr']
        
        # OBR.4 universal service identifier
        # get type - rom obr.4 universal service identifier
        self.type = get_with_default(obr_field_list, 'obr', 4)[1]
        
        # proceed if type is "MDC_OBS_WAVE_CTS" at OBR level.  this would mean there is an OBX with type NA
        if self.type == 'MDC_OBS_WAVE_CTS':  #"69121^MDC_OBS_WAVE_CTS^MDC":
            # log.info(f"Waveform signal {self.type}")
            ...
        # proceed if type is "monitoring of patient" at OBR level.
        elif self.type == 'monitoring of patient': # "182777000^monitoring of patient^SCT":
            # log.info(f"monitoring of patient {self.type}")
            ...
        # proceed if type is "MDC_EVT_ALARM" at OBR level. 
        elif self.type == 'MDC_EVT_ALARM': # "196616^MDC_EVT_ALARM^MDC":
            # log.info(f"Alarm signal {self.type}")
            ...
        else:
            log.error(f"Unknown signal {self.type}, skipping")
            return

        # some tests:
        # self.id = get_with_default(obr_field_list, 1, 'int')
        
        # get time - OBR.7 and OBR.8 - Observation Date/Time and Observation End Date/Time
        self.start_t = get_with_default(obr_field_list, 'obr', 7)
        self.end_t = get_with_default(obr_field_list, 'obr', 8)
        
        # get location:  OBR.10 collector identifier
        self.bed = get_with_default(obr_field_list, 'obr', 10)
        self.env = get_with_default(obr_field_list, 'obr', 13)

        # src target
        obr3 = get_with_default(obr_field_list, 'obr', 3)
        # self.source1 = f"{obr3[1]}:{obr3[2]}"
        self.source2 = get_with_default(obr_field_list, 'obr', 21)
        
        self.attributes = {}
        for obx_fields in obr['obx']:
            #https://hl7.docs.careevolution.com/segments/obx.html
            valtype = get_with_default(obx_fields, 'obx', 2)  #  OBX.2
            obx_time = get_with_default(obx_fields, 'obx', 14)  # OBX.14 Date/Time of the Observation

            (obx_name, code) = extract_signal_name(obx_fields)  # OBX.3 observation identifier
            channel_id = extract_signal_id(obx_fields)

            # if obx_time != self.start_t:
            #     raise ValueError(f"OBX time {obx_time} does not match OBR start time {self.start_t} for signal {obx_name}")

            unit_of_meas = extract_signal_uom(obx_fields)  # OBX.6 unit of measure
            
            # if numeric array, get the channel name and id, as well as data.
            data = obx_fields[5]  # obx.5 : parser handled the list construction and type conversion
                
            # # drop if sig_name is "Patient Monitor, Physiologic Multi-Parameter"
            # if (sig_name == "Patient Monitor, Physiologic Multi-Parameter"):
            #     continue
            ref_range = get_with_default(obx_fields, 'obx', 7)
        
            self.attributes[obx_name] = {'valtype': valtype, 
                                         'type': obx_name, 
                                         'code': code,
                                         'channel_id': channel_id, 
                                         'obs_time': obx_time, 
                                         'UoM' : unit_of_meas, 
                                         'value': data,
                                         'ref_range': ref_range,
                                         }

    def __repr__(self):
        attrs = []
        for key, value in self.attributes.items():
            obxstr = f"{key}:: valtype: {value['valtype']} type: {value['type']}, code: {value['code']}, chan_id: {value['channel_id']} unit:{value['UoM']} time:{value['obs_time']}"
            
            if isinstance(value['value'], list) and (len(value['value']) > 5):
                obxstr += f"{len(value['value'])} value {value['value'][:5]}..."
            else:
                obxstr += f"{value['value']}"
                
            attrs.append(f"\t\t\t{obxstr}")
        attributes_str = "\n".join(attrs)
        return (f"\tSignal bed={self.bed}, env={self.env}\n\t\tstart_t={self.start_t}, end_t={self.end_t}\n\t\ttype={self.type}, "
            f"\t\ttarget={self.target}, "
            f"source2={self.source2}\n{attributes_str})")


# %%
# signal corresponds to an OBR and its OBX children.
# example
# [[['MSH'],
#   ['|'],
#   ['^~\\&'],
#   ['DATACAPTOR'],
#   [''],
#   [''],
#   [''],
#   ['20250228155959.885-0500'],
#   [''],
#   [[['ORU'], ['R01'], ['ORU_R01']]],
#   ['AWS_Data_00011dd26e7f0015d1e1'],
#   ['P'],
#   ['2.6'],
#   [''],
#   [''],
#   ['NE'],
#   ['NE'],
#   [''],
#   ['UNICODE UTF-8'],
#   [''],
#   [''],
#   [[['IHE_PCD_001'], ['IHE PCD'], ['1.3.6.1.4.1.19376.1.6.1.1.1'], ['ISO']],
#    [['IHEPCD0104_Capsule V1_11.6.1.260'], ['CAPSULE']]],
#   ['']],
#  [['PID'],
#   [''],
#   [''],
#   [''],
#   [''],
#   [[[''], [''], [''], [''], [''], [''], ['U']]],
#   ['']],
#  [['PV1'], [''], ['I'], ['EUH-4TN-T435'], ['']],

class HL7Data:
    def __init__(self, message: HierarchicalMessage):
        self.orig_message = message
        self.msh_time = get_with_default(message.msh, 'msh', 6)
        self.msh_send_app = get_with_default(message.msh, 'msh', 2)
        self.control_id = get_with_default(message.msh, 'msh', 10)
        self.msh_type = get_with_default(message.msh, 'msh', 9)
        self.pid, self.pid_visit, self.pid_first_name, self.pid_last_name = extract_pid(message.pid, message.pv1)
        self.pid_middle_initial = missing_values[str]
        self.message_type = "Other"
        
        # https://hl7.docs.careevolution.com/segments/pv1.html
        self.pv1_bed_type = get_with_default(message.pv1, 'pv1', 2)
        self.pv1_bed = get_with_default(message.pv1, 'pv1', 3)
        self.hospital, self.bed_unit, self.bed_id = extract_bed_id(self.pv1_bed)

    def get_pid_loc_mapping(self):
        return [{
            'hospital': self.hospital,
            'bed_unit': self.bed_unit,
            'bed_id': self.bed_id,
            'pid': self.pid,
            'visit_id': self.pid_visit,
            'first_name': self.pid_first_name,
            'last_name': self.pid_last_name,
            'middle_initial': self.pid_middle_initial,
            'start_t': self.msh_time,
            'end_t': missing_values[np.datetime64]
        }]
        
    def _to_row_dicts(self, for_serialization=False):
        return []
        
    def to_row_dicts(self):
        return self._to_row_dicts(for_serialization=False)
    
    def to_row_json(self):
        res = self._to_row_dicts(for_serialization=True)
        return json.dumps(res) if len(res) > 0 else ''


class HL7ORUData(HL7Data):
    def __init__(self, message: HierarchicalMessage):
        super().__init__(message)
        self.msh_profile = message.msh[20][0]
        self.signals = []
        for obr in message.obrs:
            # https://hl7.docs.careevolution.com/segments/obr.html
            self.signals.append(Signal(obr))

    def __repr__(self):
        signalstr = []
        for signal in self.signals:
            signalstr.append(signal.__repr__())
        return f"ORU MSH time={self.msh_time}, source={self.msh_profile}, PID={self.pid}, VISIT={self.pid_visit}, PV1 bed={self.bed_id}\n" + "\n".join(signalstr)

    def _extract_from_signal(self, signal: Signal) -> dict:
        channel = missing_values[str]
        channel_id = missing_values[str]
        values = missing_values[list]
        valtype = missing_values[str]
        UoM = missing_values[str]
        ref_range = missing_values[str]
        obx_start = missing_values[np.datetime64]   # default to NaT instead of ''
        sample_interval_ms = missing_values[float]
        
        results = []
        # only process waveform types.  assume these are the ones with end_t
        # get the other value
        for name, obx in signal.attributes.items():
            channel = name
            if (channel == missing_values[str]):
                continue
            
            channel_id = obx['channel_id']
            
            values = obx['value']  # type conversion in hl7_parser
            valtype = obx['valtype']
            UoM = obx['UoM']
            ref_range = obx['ref_range']
            obx_start = obx['obs_time']
            sample_interval_ms = missing_values[float]

            nsamples = 1
            
            results.append((channel, channel_id, obx_start, values, valtype, UoM, ref_range, sample_interval_ms, nsamples))

        return results

    # missing values set to '' for json serialization
    def _to_row_dicts(self, for_serialization=False):
        outs = []
        common = {
            'msh_time': self.msh_time if pd.notna(self.msh_time) else missing_values[str if for_serialization else np.datetime64],
            'msh_send_app': self.msh_send_app,
            'profile': self.msh_profile,
            'control_id': self.control_id,
            'hospital': self.hospital,
            'bed_unit': self.bed_unit,
            'bed_id': self.bed_id,
            'pid': self.pid,
            'visit_id': self.pid_visit,
            'patient_last_name': self.pid_last_name,
            'patient_first_name': self.pid_first_name,
        }

        for signal in self.signals:  # a signal is an OBR
            signal_common = common.copy()
            signal_common.update({
                'src': signal.source2,
                'msg_type': signal.type,
                'start_t': signal.start_t if pd.notna(signal.start_t) else missing_values[str if for_serialization else np.datetime64],
                'end_t': missing_values[str if for_serialization else np.datetime64],
            })
            data = self._extract_from_signal(signal)
            
            for (channel, channel_id, obx_start, values, valtype, UoM, ref_range, _, nsamples) in data:
                if nsamples == 0:
                    log.error(f"{self.bed_id} empty list {channel}, {signal.start_t}")
                out = signal_common.copy()
                out.update({
                    'channel': channel,
                    'id': channel_id,
                    'channel_type': channel_to_type.get(channel, 'other'),
                    'obx_start_t': obx_start if pd.notna(obx_start) else missing_values[str if for_serialization else np.datetime64],
                    'values': [values,] if type(values) is not list else values,  # parquet does not allow mixed types.
                    'value_type': valtype,  # not used.
                    'UoM': UoM,
                    'ref_range': ref_range,
                    
                    'pd_samp_ms': missing_values[str if for_serialization else float],
                    'nsamp': 1,
                })
                outs.append(out)
        return outs


class HL7AlarmData(HL7ORUData):
    def __init__(self, message: HierarchicalMessage):
        super().__init__(message)
        self.msh_profile = '^'.join(message.msh[8])
        self.message_type = 'Alarm'

    def __repr__(self):
        signalstr = []
        for signal in self.signals:
            signalstr.append(signal.__repr__())
        return f"Alarm MSH time={self.msh_time}, source={self.msh_profile}, PID={self.pid}, VISIT={self.pid_visit}, PV1 bed={self.bed_id}\n" + "\n".join(signalstr)


class HL7WaveformData(HL7ORUData):
    def __init__(self, message: HierarchicalMessage):
        super().__init__(message)
        self.message_type = 'Waveform'

    def __repr__(self):
        signalstr = []
        for signal in self.signals:
            signalstr.append(signal.__repr__())
        return f"Waveform MSH time={self.msh_time}, source={self.msh_profile}, PID={self.pid}, VISIT={self.pid_visit}, PV1 bed={self.bed_id}\n" + "\n".join(signalstr)

    def _extract_from_signal(self, signal: Signal) -> dict:
        channel = missing_values[str]
        channel_id = missing_values[str]
        values = missing_values[list]
        valtype = missing_values[str]
        UoM = missing_values[str]
        ref_range = missing_values[str]
        obx_start = missing_values[np.datetime64]   # default to NaT instead of ''
        samp_interval_ms = missing_values[float]
        
        # only process waveform types.  assume these are the ones with end_t
        # get the other value
        for name, obx in signal.attributes.items():
            if (obx['valtype'] == "NA"):
                channel = name
                channel_id = obx['channel_id']
                
                values = obx['value']  # type conversion in hl7_parser
                valtype = obx['valtype']
                UoM = obx['UoM']
                ref_range = obx['ref_range']

                obx_start = obx['obs_time']
            elif "TIME_PD_SAMP" in name:
                # type convert handled by hl7 parser
                samp_interval_ms = obx['value'] if obx['value'] != missing_values[str] else missing_values[float]

        if isinstance(values, (list, np.ndarray)):
            nsamples = len(values)
        else:
            nsamples = 1
            
        return (channel, channel_id, obx_start, values, valtype, UoM, ref_range, samp_interval_ms, nsamples)

    def _to_row_dicts(self, for_serialization=False):
        outs = []
        common = {
            'msh_time': self.msh_time if pd.notna(self.msh_time) else missing_values[str if for_serialization else np.datetime64],
            'msh_send_app': self.msh_send_app,
            'profile': self.msh_profile,
            'control_id': self.control_id,
            'hospital': self.hospital,
            'bed_unit': self.bed_unit,
            'bed_id': self.bed_id,
            'pid': self.pid,
            'visit_id': self.pid_visit,
            'patient_last_name': self.pid_last_name,
            'patient_first_name': self.pid_first_name,
        }
        for signal in self.signals:
            # extract the waveform data from signal.
            (channel, channel_id, obx_start,
                values, valtype, UoM, ref_range, samp_interval_ms, nsamples) = self._extract_from_signal(signal)
            if nsamples == 1:
                log.warning(f"{self.bed_id} single sample list {channel}, {signal.start_t}")
            elif nsamples == 0:
                log.error(f"{self.bed_id} empty list {channel}, {signal.start_t}")
            
            out = common.copy()
            out.update({
                'src': signal.source2,
                'msg_type': signal.type,
                'start_t': signal.start_t if pd.notna(signal.start_t) else missing_values[str if for_serialization else np.datetime64],
                'end_t': signal.end_t if pd.notna(signal.end_t) else missing_values[str if for_serialization else np.datetime64],
                
                'channel': channel,
                'id': channel_id,
                'channel_type': channel_to_type.get(channel, 'other_waveform'),
                'obx_start_t': obx_start if pd.notna(obx_start) else missing_values[str if for_serialization else np.datetime64],                

                'values': values,
                'value_type': valtype,
                'UoM': UoM,
                'ref_range': ref_range,
                
                'pd_samp_ms': samp_interval_ms if pd.notna(samp_interval_ms) else missing_values[str if for_serialization else float],
                'nsamp': nsamples if pd.notna(nsamples) else missing_values[str if for_serialization else int],
            })
            outs.append(out)
        return outs


HL7ECGData = HL7WaveformData

class HL7VitalsData(HL7ORUData):
    def __init__(self, message: HierarchicalMessage):
        super().__init__(message)

        self.pid2, self.pid_visit2, self.pid_first_name2, \
            self.pid_last_name2, self.pid_middle_initial2, self.pid_time2 = \
                extract_pid_from_obx(message.obrs[0]['obr'], message.obrs[0]['obx'])

        self.message_type = "Vitals"

        self.signals = []
        for obr in message.obrs:
            # https://hl7.docs.careevolution.com/segments/obr.html
            self.signals.append(Signal(obr))
            
    def get_pid_loc_mapping(self):
        out = super().get_pid_loc_mapping()
        out.append({
            'hospital': self.hospital,
            'bed_unit': self.bed_unit,
            'bed_id': self.bed_id,
            'pid': self.pid2,
            'visit_id': self.pid_visit2,
            'first_name': self.pid_first_name2,
            'last_name': self.pid_last_name2,
            'middle_initial': self.pid_middle_initial2,
            'start_t': self.pid_time2,
            'end_t': missing_values[np.datetime64]
        })
        return out
            
    def __repr__(self):
        signalstr = []
        for signal in self.signals:
            signalstr.append(signal.__repr__())
        return f"Vitals MSH time={self.msh_time}, source={self.msh_profile}, PID={self.pid}, VISIT={self.pid_visit}, PV1 bed={self.bed_id}\n" + "\n".join(signalstr)

class HL7ADTData(HL7Data):
    def __init__(self, message: HierarchicalMessage):
        super().__init__(message)
        self.msh_profile = '^'.join(message.msh[8])
        self.message_type = 'ADT'
        
    def __repr__(self):
        return f"ADT MSH time={self.msh_time}, source={self.msh_profile}, PID={self.pid}, VISIT={self.pid_visit}, PV1 bed={self.bed_id}"


# dispatcher.
def hl7_data_factory(omsg: HierarchicalMessage):
    msh_profile = omsg.msh[8][0]   # MSH_9
    if msh_profile == "ORU":
        # get the first OBR to determine type
        obr = omsg.obrs[0]['obr']
        obr_type = get_with_default(obr, 'obr', 4)[0]
        if obr_type == '182777000':  # monitoring of patient
            return HL7VitalsData(omsg)
        elif obr_type == '196616':  # alarm event
            return HL7AlarmData(omsg)
        elif obr_type == '69121':  # waveform
            return HL7WaveformData(omsg)
        else:
            raise ValueError(f"[ERROR] skipping unknown OBR type {obr_type}.")
        
    elif msh_profile == "ADT":
        return HL7ADTData(omsg)
    else:
        raise ValueError(f"[ERROR] skipping unknown message type {msh_profile}.")
        