import re
from hl7lite.hl7_datatypes import convert_field, hl7_type_to_pandas_type, hl7_field_to_pandas_type, missing_values, DataType

import logging
log = logging.getLogger(__name__)
# this file contains functions that are used for tokenizing HL7 messages AND convert OBX data types, and only OBX data types.


# default separators.
# global FIELD_SEPARATOR, COMPONENT_SEPARATOR, REPETITION_SEPARATOR, ESCAPE_SEPARATOR, SUBCOMPONENT_SEPARATOR, MSH_2
FIELD_SEPARATOR: str = '|'
COMPONENT_SEPARATOR: str = '^'
REPETITION_SEPARATOR: str = '~'
ESCAPE_SEPARATOR: str = '\\'
SUBCOMPONENT_SEPARATOR: str = '&'
MSH_2: str = '^~\\&'

#%%

# check to see if we have a decimal point and at least 1 digit after it is not 0.
# int(str) cannot handle *.0 integers.

def _convert_obx_value_type(data, datatype:str):
    if datatype is None or len(datatype) == 0:  # empty or missing datatype string, return as string.
        datatype = 'ST' # string type
        
    output_type = hl7_type_to_pandas_type.get(datatype, DataType.STR)
    
    try:
        return convert_field(data, output_type, immediate = True)
    except Exception as e:
        raise ValueError(f"[ERROR] converting {data} to datatype {datatype} ({output_type}): {e}")


# this is a simple parser for HL7 messages.
# previous tests show that 
# python hl7 package (generating similar parsed structure, except with more layers of lists)
#   can be 12x slower than this simple parser, and also may not handle large amount of message data well
#   some of the issues experienced may be memory corruption related - hl7 segments get incorectly concatenated or missing MSH.
#   also possibly hangs
# python hl7apy package may be even slower than hl7.


# HL7 MSH defines the component, repetition, escape, and subcomponent separators.
message_separator = re.compile(r'\r\n\r\n|\r\n\n|\n\n')
segment_separator = re.compile(r'\r\n|\n|\r')


# doing the "in" check avoids calling split if the delimiter is not present.  this speeds things up significantly.
def _component_to_subcomponents(component: str):
    if SUBCOMPONENT_SEPARATOR not in component:
        return component
    else:
        return component.split(SUBCOMPONENT_SEPARATOR)

def _repetition_to_components(field: str):
    
    if COMPONENT_SEPARATOR not in field:
        return _component_to_subcomponents(field)
    else:
        cs = field.split(COMPONENT_SEPARATOR)
        return [_component_to_subcomponents(c) for c in cs]
    
def _field_to_repetitions(field: str):
    if REPETITION_SEPARATOR not in field:
        return _repetition_to_components(field)
    else:
        fs = field.split(REPETITION_SEPARATOR)
        return [_repetition_to_components(f) for f in fs]


# parse 1 segment into fields
# this function skips repetition and subcomponent tokenization if they are absent, else invoke the full tokenization chain.
def _segment_to_fields(segment: str):
    fields = segment.split(FIELD_SEPARATOR)
    
    parsed_fields = [ field if field == MSH_2 else \
            _field_to_repetitions(field) if ((REPETITION_SEPARATOR in field) or (SUBCOMPONENT_SEPARATOR in field)) else \
            field.split(COMPONENT_SEPARATOR) if (COMPONENT_SEPARATOR in field) else field \
            for field in fields ]
    
    if parsed_fields[0].upper() == 'OBX':
        parsed_fields[5] = _convert_obx_value_type(data = parsed_fields[5], datatype = parsed_fields[2])
    return parsed_fields

# # UNUSED
# def _convert_value(parsed: list):
#     for seg in parsed:
#         if seg[0].upper() == 'OBX':
#
#             res = _convert_obx_value_type(data=seg[5], datatype=seg[2])
#             # log.debug(f"immediate mode:  Converting {type(seg[5])} {seg[5]} to type {seg[2]}, result {type(res)} {res}")
#             seg[5] = res
#     return parsed

# else, get the separators from global variables
def tokenize_hl7_message(hl7_str: str):
    # perhaps a streaming parser would be best.
    # first split the message into segments
    parsed = []  # dict of segments, repeats are organized as a list.
    seg_names = set()
    
    # first segment should be MSH
    if not hl7_str.startswith("MSH"):
        raise ValueError("ERROR: first segment is not MSH")
    
    FIELD_SEPARATOR = hl7_str[3]     # |
    COMPONENT_SEPARATOR = hl7_str[4]  # ^
    REPETITION_SEPARATOR = hl7_str[5]  # ~   no known usage
    ESCAPE_SEPARATOR = hl7_str[6]   # \   no known usage.
    SUBCOMPONENT_SEPARATOR = hl7_str[7]  # &  no known usage
    MSH_2 = hl7_str[4:8]  # MSH_2 are the component separators, which is the first 4 characters after MSH|.

    # is it faster to use regex to split (2-3s per 5 files), or to replace \n with \r then split by \r?
    segments = hl7_str.replace('\n', '\r').split('\r')
    
    # batch parse segments version
    parsed = [_segment_to_fields(segment) for segment in segments if segment.strip() != '']
    # parse a list of segment strings, return a list of lists, each list is 1 segment's fields
    seg_names = set([seg_fields[0].upper() for seg_fields in parsed])

    # parsed = _convert_value(parsed)     # convert parsed OBX5 based on OBX2 datatype.
    
    return parsed, seg_names

def get_with_default(elements: list, seg_name: str, index: int):
    """
    Get the value from the segment at the given index.
    If the index is out of range, return None.
    """
    out_type = hl7_field_to_pandas_type.get((seg_name, index), None)
    if out_type is None:
        raise ValueError(f"ERROR: no type mapping for segment {seg_name} at index {index}.")
    
    if (elements is None) or (len(elements) <= index):
        return missing_values[out_type[0][1]]
    
    out_data = elements[index]
    if out_data is None:
        raise ValueError(f"ERROR: out_data is None for segment {elements}, index {index}. should be at least an empty string or [].")

    return convert_field(out_data, out_type)



#%%
#-- NOTE: UNUSED - regex is slower.

# # NOTE regex version.  not used - more complex, may be slower.
# def _field_to_components_re(component: str):
#     if not SUBCOMPONENT_SEPARATOR.search(component):
#         return component
#     return SUBCOMPONENT_SEPARATOR.split(component)

# def _repetition_to_components_re(field: str):
#     if not COMPONENT_SEPARATOR.search(field):
#         return _field_to_components_re(field)
#     cs = COMPONENT_SEPARATOR.split(field)
#     return [_field_to_components_re(c) for c in cs]

# def _field_to_repetitions_re(field: str):
#     if REPETITION_SEPARATOR.search(field):
#         fs = REPETITION_SEPARATOR.split(field)
#         return [_repetition_to_components_re(f) for f in fs]
#     return _repetition_to_components_re(field)
        
# # parse 1 segment into fields
# def _segment_to_fields_re(segment: str):
#     fields = FIELD_SEPARATOR.split(segment)
#     parsed_fields = [ _field_to_repetitions_re(f) for f in fields ]
#
#     if parsed_fields[0].upper() == 'OBX':
#         parsed_fields[5] = _convert_obx_value_type(data = parsed_fields[5], datatype = parsed_fields[2])
#     return parsed_fields
    
# def tokenize_hl7_message_re(hl7_str: str):
#     regex_escape = {
#         '|': '\\|',
#         '^': '\\^',
#         '.': '\\.',
#         '*': '\\*',
#         '?': '\\?',
#         '+': '\\+',
#         '$': '\\$',
#         '{': '\\{',
#         '[': '\\[',
#         '(': '\\(',
#         ')': '\\)',
#         ']': '\\]',
#         '}': '\\}',
#     }
#     # perhaps a streaming parser would be best.
#     # first split the message into segments
#     parsed = []  # dict of segments, repeats are organized as a list.
#     seg_names = set()
#     segments = segment_separator.split(hl7_str)

#     # first segment should be MSH
#     if not hl7_str.startswith("MSH"):
#         log.error(f"first segment is not MSH")
#         return None
    
#     # else, get the separators
#     separators = {}
#     FIELD_SEPARATOR = re.compile(regex_escape[hl7_str[3]] if hl7_str[3] in regex_escape else hl7_str[3])
#     COMPONENT_SEPARATOR = re.compile(regex_escape[hl7_str[4]] if hl7_str[4] in regex_escape else hl7_str[4])
#     REPETITION_SEPARATOR = re.compile(regex_escape[hl7_str[5]] if hl7_str[5] in regex_escape else hl7_str[5])
#     ESCAPE_SEPARATOR = hl7_str[6]
#     SUBCOMPONENT_SEPARATOR = re.compile(regex_escape[hl7_str[7]] if hl7_str[7] in regex_escape else hl7_str[7])
    
#     for segment in segments:
        
#         # gets segment
#         seg_fields = _segment_to_fields_re(segment)
#         label = seg_fields[0].upper()
#         seg_names.add(label)
#         parsed.append(seg_fields)

#         # log.debug(f"Segment {label}: {seg_fields}")

#     return parsed, seg_names