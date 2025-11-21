import pandas as pd
import numpy as np
# from datetime import datetime, timedelta
import re

from zoneinfo import ZoneInfo

from hl7lite.parse_time_tz import c_parse_time, c_parse_time_batch

# individually convert is slower than vectorized convert.
# python's float() function wraps a C double and follows IEEE 754
# so it can precisely represent integers up to +/- 2^53
# use float for everything.
# Enum is slow compared to class attributes.

class DataType:
    # (name, container_type, element_type)
    INT = ('int', int, int),
    FLOAT = ('float', float, float),
    # NUMERIC = ('numeric', (int, float), (int, float)),
    NUMERIC = ('numeric', float, float),
    DATETIME = ('datetime', np.datetime64, np.datetime64),
    # DATETIME_STR = ('datetime_str', str, str),
    # TIMEDELTA = ('timedelta', timedelta, timedelta),
    STR = ('str', str, str),
    
    STR_OR_LIST = ('str_or_list', (str, list), str),
    LIST_OF_STR = ('list', list, str),
    LIST_OF_INT = ('list_of_int', list, int),
    LIST_OF_FLOAT = ('list_of_float', list, float),
    # LIST_OF_NUMERIC = ('list_of_numeric', list, (int, float)),
    LIST_OF_NUMERIC = ('list_of_numeric', list, float),
    # LIST_OF_DATETIME = ('list_of_datetime', list, np.datetime64),
    # LIST_OF_DATETIME_STR = ('list_of_datetime_str', list, str),
    
    ANY = ('any', object, object),
    # BOOL = ('bool', bool, bool),

missing_values = {
    int : np.iinfo(np.int64).min, # json cannot serialize pd.NA, pd.to_numeric can parse '' as np.float64(nan), float('') fails
    float : np.nan,
    (int, float) : np.nan, # json cannot serialize pd.NA, pd.to_numeric can parse '' as np.float64(nan), float('') fails
    np.datetime64 : pd.NaT,   # json cannot serialize NaT.  pd.to_datetime can parse '' as NaT.
    str : '',
    list : [],
    (str, list): '',
}

hl7_field_to_pandas_type = {  # MSH.2 is the separators, so labeling is 1 more than actual list index.
    ('msh', 2): DataType.STR,  # MSH.3 sending application
    ('msh', 6): DataType.DATETIME,  # MSH.7 message date/time
    ('msh', 8): DataType.STR_OR_LIST,  # MSH.9 message type
    ('msh', 9): DataType.STR,  # MSH.10 control id
    ('msh', 10): DataType.STR,  # MSH.11 production vs test
    ('msh', 20): DataType.STR_OR_LIST,  # MSH.21 message profile
    ('pid', 3): DataType.STR_OR_LIST, # PID.3 patient identifier list
    ('pid', 5): DataType.STR_OR_LIST,  # PID.5 patient name
    ('pid', 18): DataType.STR_OR_LIST,  # PID.18 visit number /patient account.
    ('pv1', 2): DataType.STR,  # PV1.2 patient class
    ('pv1', 3): DataType.STR_OR_LIST,  # PV1.3 assigned patient location
    ('pv1', 19): DataType.STR,  # PV1.19 visit number
    ('obr', 3): DataType.LIST_OF_STR,  # OBR.3 src target?
    ('obr', 4): DataType.STR_OR_LIST,  # OBR.4 universal service identifier (wave, alarm, vital)
    ('obr', 7): DataType.DATETIME, # OBR.7 Observation Date/Time
    ('obr', 8): DataType.DATETIME, # OBR.8 Observation End Date/Time
    ('obr', 10): DataType.STR_OR_LIST,  # OBR.10 collector identifier (bed)
    ('obr', 13): DataType.STR,  # OBR.13 environment
    ('obr', 21): DataType.STR,  # OBR.21 source2
    ('obx', 2): DataType.STR,  # OBX.2 value type
    ('obx', 3): DataType.STR_OR_LIST,  # OBX.3 observation identifier
    ('obx', 4): DataType.STR,  # OBX.4 observation sub-id
    ('obx', 5): DataType.ANY,  # OBX.5 observation value (could be str, numeric, datetime, list of str, list of numeric)
    ('obx', 6): DataType.LIST_OF_STR,  # OBX.6 units of measurement
    ('obx', 7): DataType.STR,  # OBX.7 reference range
    ('obx', 14): DataType.DATETIME, # OBX.14 Date/Time of the Observation
    ('obx', 21): DataType.STR,  # OBX.21 observation source
    # all others are either str or list.
}

hl7_type_to_pandas_type = {
    'ST': DataType.STR,
    'NM': DataType.NUMERIC,
    'NA': DataType.LIST_OF_NUMERIC,
    'NR': DataType.LIST_OF_NUMERIC,
    'CWE': DataType.STR_OR_LIST,
    # 'CD' : DataType.STR_OR_LIST,
    # 'CSU' : DataType.STR_OR_LIST,
    # 'MA' : DataType.LIST_OF_NUMERIC,
    # 'WVI': DataType.STR_OR_LIST,
    # 'WVS': DataType.STR_OR_LIST,    
}


# parse and return datetime64 objects.  parquet uses int64 to represent date time.
# numpy's datetime64 should be pretty compatible. but natively datetime64 constructor is not a good parser.
# python datetime does not respect timezones well.
# pandas's timestamp class may be good but it similarly does not have a good parser on its own.
# pandas's to_datetime is good, except that depending on whether input is a scalar, array, or series/dataframe, different return types are used.
# fastest approach is PROBABLY to keep time as string until the dataframe is populated, then convert to datetime64 in batch. - profile
# fallback is to directly convert to datetime64 from timestamp.
def fix_time(time_str: str):
    if time_str.endswith('00') or (time_str.endswith('30')):
        # either proper timezone, or no timezone info.
        ...
    elif time_str.endswith('-0359'):
        time_str = time_str.replace('-0359', '-0400')  # convert -0359 to -0400 for EST
    elif time_str.endswith('-0401'):
        time_str = time_str.replace('-0401', '-0400')  # convert -0401 to -0400 for EST
    elif time_str.endswith('-0459'):
        time_str = time_str.replace('-0459', '-0500')  # convert -0459 to -0500 for EST
    elif time_str.endswith('-0501'):
        time_str = time_str.replace('-0501', '-0500')  # convert -0501 to -0500 for EST
        
    return time_str

# return int64, down to nanosecond precision, representing epoch seconds or datetime64
def parse_time_python(time_strs, as_epoch_ns: bool = False):
    # if time_strs is a single string, need to convert from pd.Timestamp to datetime64
    if isinstance(time_strs, str):
        try:
            out = None
            if time_strs[-5] in ['-', '+'] and ((len(time_strs) > 14) and (time_strs[14] == '.')):
                out = pd.to_datetime(time_strs, format="%Y%m%d%H%M%S.%f%z", utc=True, errors='raise', exact=True).to_datetime64()
            elif time_strs[-5] in ['-', '+']:
                out = pd.to_datetime(time_strs, format="%Y%m%d%H%M%S%z", utc=True, errors='raise', exact=True).to_datetime64()
            elif ((len(time_strs) > 14) and (time_strs[14] == '.')):
                out = pd.to_datetime(time_strs, format="%Y%m%d%H%M%S.%f", utc=False, errors='raise', exact=True).tz_localize(ZoneInfo("America/New_York")).tz_convert('UTC').to_datetime64()
            else:
                out = pd.to_datetime(time_strs, format="%Y%m%d%H%M%S", utc=False, errors='raise', exact=True).tz_localize(ZoneInfo("America/New_York")).tz_convert('UTC').to_datetime64()
            return out.astype('int64') if as_epoch_ns else out
        except Exception as e:
            raise ValueError(f"Error parsing time string '{time_strs}': {e}") from e
    else:
        # if time_strs is from a series or list, datetime64 is returned already.
        out = pd.to_datetime(time_strs, format="%Y%m%d%H%M%S.%f%z", utc=True, errors='raise', exact=True)
        return out.astype('int64') if as_epoch_ns else out
        
def parse_time(time_strs, as_epoch_ns: bool = False):
    if isinstance(time_strs, str):
        return c_parse_time(time_strs, as_epoch_ns=as_epoch_ns)
    else:
        return c_parse_time_batch(time_strs, as_epoch_ns=as_epoch_ns)


float_re = re.compile(r"^[-+]?((\d+\.*\d*)|(\.\d+))$") 
int_re = re.compile(r"^[-+]?\d+$")

    
def convert_field(data, datatype: DataType, as_string : bool = True):
    """
    Convert data to the specified DataType.
    data could be string, list of strings, float, int, list of float, or list of ints 
    datatype could be any of the DataType enum values.
    """
    if data is None:
        raise ValueError(f"Cannot convert None to {datatype}")
    
    ## not checked.  datatype has to be one of the ones defined in DataType
    # if any(datatype == dt for dt in DataType.__dict__.values()):
    #     pass
    # else:
    #     raise ValueError(f"Unsupported DataType {datatype} for data: {data}")
    
    is_str = (type(data) is str)
    # can assume a list is a list of strings
    is_list_of_str = (type(data) is list) and (type(data[0]) is str)
    # is_list_of_str = isinstance(data, list) and ((datalen == 0) or all(isinstance(datum, str) for datum in data))

    container_type = datatype[0][1]
    # element_type = datatype[0][2]
    if isinstance(data, str) and len(data) == 0:
        return missing_values[container_type]

    # ANY type mean no change.  e.g. OBX_5 which is obx value, and is converted previously.

    if as_string:  # not converting.
        if is_str:
            match datatype:
                case DataType.STR | DataType.STR_OR_LIST | DataType.NUMERIC | DataType.INT | DataType.FLOAT | DataType.ANY:
                    return data
                case DataType.LIST_OF_STR | DataType.LIST_OF_FLOAT | DataType.LIST_OF_INT | DataType.LIST_OF_NUMERIC:
                    return [data, ]
                case DataType.DATETIME:
                    return fix_time(data)
                case _:
                    raise ValueError(f"Unsupported datatype {datatype} for string data: {data}")
        elif is_list_of_str:
            match datatype:
                case DataType.STR_OR_LIST | DataType.LIST_OF_STR | DataType.LIST_OF_FLOAT | DataType.LIST_OF_INT | DataType.LIST_OF_NUMERIC | DataType.ANY:
                    return data
                case DataType.STR:
                    return '^'.join(s for s in data)
                case DataType.NUMERIC | DataType.INT | DataType.FLOAT | DataType.DATETIME:
                    raise ValueError(f"Cannot convert list {data} to {datatype} as target type is not a list of elemental types")
                case _:
                    raise ValueError(f"Unsupported datatype {datatype} for list data: {data}")
    else:  # converting.
        if is_str:
            match datatype:
                case DataType.STR | DataType.STR_OR_LIST | DataType.ANY:
                    return data
                case DataType.LIST_OF_STR:
                    return [data, ]
                case DataType.DATETIME:
                    return parse_time(fix_time(data))
                case DataType.NUMERIC:
                    return float(data) if '.' in data else int(data)
                case DataType.INT:
                    return int(data)
                case DataType.FLOAT:
                    return float(data)
                case DataType.LIST_OF_NUMERIC:
                    return [float(data), ] if '.' in data else [int(data), ]
                case DataType.LIST_OF_INT:
                    return [int(data), ]
                case DataType.LIST_OF_FLOAT:
                    return [float(data), ]
                case _:
                    raise ValueError(f"Unsupported datatype {datatype} for list data: {data}")
        elif is_list_of_str:
            match datatype:
                case DataType.LIST_OF_STR | DataType.STR_OR_LIST | DataType.ANY:
                    return data
                case DataType.STR:
                    return '^'.join(s for s in data)
                case DataType.LIST_OF_INT:
                    return [int(datum) for datum in data]
                case DataType.LIST_OF_FLOAT:
                    return [float(datum) for datum in data]
                case DataType.LIST_OF_NUMERIC:
                    any_floats = any(['.' in datum for datum in data])
                    if any_floats:
                        return list(map(float, data))
                    else:
                        return list(map(int, data))
                case DataType.INT | DataType.FLOAT | DataType.NUMERIC | DataType.DATETIME:
                    raise ValueError(f"Cannot convert list {data} to {datatype} as target type is not a list of elemental types")
                case _:
                    raise ValueError(f"Unsupported datatype {datatype} for list data: {data}")
        else:
            raise ValueError(f"Unsupported input type {type(data)} for data (should be only str or list of strs): {data} to {datatype}")


    # if as_string:  # not converting.
    #     if is_str:
    #         if datatype == DataType.DATETIME:
    #             return fix_time(data)
    #         elif datatype in [DataType.LIST_OF_STR, DataType.LIST_OF_FLOAT, DataType.LIST_OF_INT, DataType.LIST_OF_NUMERIC]:
    #             return [data, ]
    #         else: # datatype = DataType.STR, STR_OR_LIST, NUMERIC, INT, FLOAT
    #             return data
    #     elif is_list_of_str:
    #         if datatype == DataType.STR:
    #             return '^'.join(s for s in data)
    #         elif datatype in [DataType.NUMERIC, DataType.INT, DataType.FLOAT, DataType.DATETIME]:
    #             raise ValueError(f"Cannot convert list {data} to {datatype} as target type is not a list of elemental types")
    #         else: # datatype = DataType.STR_OR_LIST, LIST_OF_STR, LIST_OF_FLOAT, LIST_OF_INT, LIST_OF_NUMERIC, ANY
    #             return data
    # else:  # converting.
    #     if is_str:
    #         if datatype == DataType.DATETIME:
    #             return parse_time(fix_time(data)) 
    #         elif datatype == DataType.NUMERIC:
    #             if '.' in data:
    #                 return float(data)
    #             else:
    #                 return int(data)   
    #         elif datatype == DataType.INT:
    #             return int(data)
    #         elif datatype == DataType.FLOAT:
    #             return float(data)
    #         elif datatype == DataType.LIST_OF_STR:
    #             return [data, ]
    #         elif datatype == DataType.LIST_OF_NUMERIC:
    #             if '.' in data:
    #                 return [float(data),]
    #             else:
    #                 return [int(data),]
    #         elif datatype == DataType.LIST_OF_INT:
    #             return [int(data),]
    #         elif datatype == DataType.LIST_OF_FLOAT:
    #             return [float(data),]
    #         else:  # datatype = DataType.STR or DataType.STR_OR_LIST, ANY
    #             return data       
    #     elif is_list_of_str:
    #         if datatype in [DataType.INT, DataType.FLOAT, DataType.NUMERIC, DataType.DATETIME]:
    #             raise ValueError(f"Cannot convert list {data} to {datatype} as target type is not a list of elemental types")
    #         elif datatype == DataType.STR:
    #             return '^'.join(s for s in data)
    #         elif datatype == DataType.LIST_OF_INT:
    #             return [int(datum) for datum in data]
    #         elif datatype == DataType.LIST_OF_FLOAT:
    #             return [float(datum) for datum in data]
    #         elif datatype == DataType.LIST_OF_NUMERIC:
    #             # if we have a list of numerics as a value, let's convert now, as we don't convert column of lists.

    #             # TWO ASSUMPTIONS:
    #             # 1. there are no missing values.
    #             # 2. all values are integer or floats with the same format.  this is NOT TRUE. there may be mixtures of float and int reps.
    #             # all_ints = (datalen == 0) or int_re.match(data[0])
    #             # reg ex matching is slow.  check for decimal point.
    #             any_floats = any(['.' in datum for datum in data])
    #             # element_type = int if all_ints else float
    #             # replace_missing = [missing_values[element_type] if (datum == '') else datum for datum in data]
                
                
    #             # # Try converting all to int first
    #             # # use of pd.to_numeric produces a ndarray, which is not directly serializable as df cell content.
    #             # # tolist() helps. but adds on average 2 sec per write and 2.4 sec per file for waveform conversion, much slower than map. 
    #             # map is faster than list comprehension when all values are populated and proper
    #             # inline simple functions to reduce overhead.
    #             if any_floats:
    #                 return list(map(float, data))
    #             else:
    #                 return list(map(int, data))
    #         else: # datatype = DataType.LIST_OF_STR, STR_OR_LIST, ANY
    #             return data
    #     else:
    #         raise ValueError(f"Unsupported input type {type(data)} for data (should be only str or list of strs): {data} to {datatype}")





def convert_column(data: pd.Series, datatype: DataType):
    """
    Convert data to the specified DataType.
    data could be string, list of strings, float, int, list of float, or list of ints 
    datatype could be any of the DataType enum values.
    """
    if data is None:
        raise ValueError(f"Cannot convert None to {datatype}")
    
    if datatype is None:
        raise ValueError("datatype cannot be None")
    
    if data.empty:
        return data
    
    if datatype == DataType.DATETIME:
        return parse_time(data)
    elif datatype == DataType.NUMERIC:
        try:
            out = data.astype(int, errors='raise')
        except ValueError:
            out = data.astype(float, errors='raise')
        return out
    elif datatype == DataType.INT:
        return data.astype(int, errors='raise')
    elif datatype == DataType.FLOAT:
        return data.astype(float, errors='raise')
    elif datatype in [DataType.STR, DataType.STR_OR_LIST, DataType.LIST_OF_STR, DataType.LIST_OF_NUMERIC]:
        # list are either subcomponent lists, or list of numerics from waveform obx values.
        # no further conversions needed.
        return data
    else:
        raise ValueError(f"Unsupported datatype {datatype} for data: {data}")   
        

# check '', 'unknown', 'missing', 'null', 'nan' as missing values 