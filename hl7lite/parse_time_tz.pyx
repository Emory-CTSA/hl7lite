# cython: language_level=3
# from libc.time cimport tm, strptime, mktime, time_t
from libc.time cimport tm, strptime, tzset, mktime, time_t
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.bytes cimport PyBytes_AsString
from cpython cimport PyObject
import numpy as np
cimport numpy as np

cdef extern from "time.h":
    time_t timegm(tm* timeptr)
cdef extern from "stdlib.h":
    int setenv(const char* name, const char* value, int overwrite)


default_tz = "America/New_York"

def set_timezone(str timezone):
    setenv(b"TZ", timezone.encode('utf-8'), 1)
    tzset()

set_timezone(default_tz)

cdef char* fmt_basic = b"%Y%m%d%H%M%S"
# cdef char* fmt_frac = b"%Y%m%d%H%M%S.%f"
# cdef char* fmt_basic_tz = b"%Y%m%d%H%M%S%z"
# cdef char* fmt_frac_tz = b"%Y%m%d%H%M%S.%f%z"

# ✅ Single timestamp parser
def _c_parse_time_as_epoch_ns(str ts, str timezone = default_tz):
    cdef tm tt
    cdef time_t epoch
    cdef char* fmt

    if len(ts) < 14:
        raise ValueError(f"Invalid timestamp format: {ts}")

    # parse the actual timestamp part.
    cdef bytes dt_bytes = PyUnicode_AsUTF8String(ts[0:14])
    cdef char* c_dt = PyBytes_AsString(dt_bytes)

    if strptime(c_dt, fmt_basic, &tt) == NULL:
        raise ValueError(f"Invalid timestamp format: {ts}")


    # check to see if we have a fraction and/or timezone.
    cdef int has_frac = len(ts) >= 15 and ts[14] == '.'
    cdef int has_tz = len(ts) >= 19 and ts[-5] in ('+', '-')


    cdef time_t frac = 0
    if has_frac:
        frac_str = "000000000"
        src_frac_str = ts[15:-5] if has_tz else ts[15:]
        src_frac_str2 = src_frac_str[:min(9, len(src_frac_str))]  # take up to 9 digits
        frac_str = src_frac_str + frac_str[0:(9-len(src_frac_str))] # pad right with zeros to 9 digits
        frac = int(frac_str)
    
    # if we have timezone, parse it.

    cdef int sign, tz_hour, tz_min, tz_offset
    if has_tz:
        # parse timezone info if present
        # HL7 timezone format is ±HHMM at the end
        sign = 1 if ts[-5] == '+' else -1
        tz_hour = int(ts[-4:-2])
        tz_min = int(ts[-2:])
        tz_offset = sign * (tz_hour * 3600 + tz_min * 60)

        epoch = (timegm(&tt) - tz_offset) * 1000000000 + frac

    else:


        tt.tm_isdst = -1  # Let mktime determine if DST is in effect

        # default timezone is local system timezone.
        # set before running.
        epoch = mktime(&tt) * 1000000000 + frac

    return epoch


def c_parse_time(str time_str, bint as_epoch_ns=False):
    cdef time_t epoch = _c_parse_time_as_epoch_ns(time_str)
    return epoch if as_epoch_ns else np.datetime64(epoch, 'ns' )

# ✅ Batch parser
def c_parse_time_batch(list time_strs, bint as_epoch_ns=False):
    cdef time_t epoch
    cdef int i
    cdef int n = len(time_strs)

    cdef np.ndarray[np.int64_t, ndim=1] result = np.empty(n, dtype=np.int64)

    for i in range(n):
        ts = time_strs[i]

        epoch = _c_parse_time_as_epoch_ns(ts)

        result[i] = epoch

    return result if as_epoch_ns else result.astype("datetime64[ns]")
