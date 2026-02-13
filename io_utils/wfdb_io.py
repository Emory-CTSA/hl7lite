# import wfdb
import os
import pickle
import pandas as pd
import wfdb
import numpy as np
import datetime

import logging
log = logging.getLogger(__name__)

#%%

# from https://github.com/MIT-LCP/wfdb-python/blob/66853f1529ac26de105d28d2f8652fc39af5f107/wfdb/io/_signal.py#L2110
# Minimum and maximum digital sample values for each of the WFDB dat
# formats.
SAMPLE_VALUE_RANGE = {
    "80": (-(2**7), 2**7 - 1),
    "508": (-(2**7), 2**7 - 1),
    "310": (-(2**9), 2**9 - 1),
    "311": (-(2**9), 2**9 - 1),
    "212": (-(2**11), 2**11 - 1),
    "16": (-(2**15), 2**15 - 1),
    "61": (-(2**15), 2**15 - 1),
    "160": (-(2**15), 2**15 - 1),
    "516": (-(2**15), 2**15 - 1),
    "24": (-(2**23), 2**23 - 1),
    "524": (-(2**23), 2**23 - 1),
    "32": (-(2**31), 2**31 - 1),
    "8": (-(2**31), 2**31 - 1),
}
INVALID_SAMPLE_VALUE = {
    "80": -(2**7),
    "508": -(2**7),
    "310": -(2**9),
    "311": -(2**9),
    "212": -(2**11),
    "16": -(2**15),
    "61": -(2**15),
    "160": -(2**15),
    "516": -(2**15),
    "24": -(2**23),
    "524": -(2**23),
    "32": -(2**31),
    "8": None,
}
MAX_I32 = 2147483647
MIN_I32 = -2147483648
DEFAULT_FMT = '16'  # this allows about 64K possible values. most of the values are have 3 to 4 significant digits (anectodal), so 16bit should work.


def _digi_bounds(fmt):
    """
    Return min and max digital values for each format type.

    Parmeters
    ---------
    fmt : str, list
        The WFDB dat format, or a list of them.

    Returns
    -------
    tuple (int, int)
        The min and max WFDB digital value per format type.

    """
    if isinstance(fmt, list):
        return [_digi_bounds(f) for f in fmt]
    return SAMPLE_VALUE_RANGE[fmt]


# NOTE:  ADC conversion code in wfdb-python is not correct for the MAXI32 case.
# calculate the adc_gain and baseline
# coped from _signal.py https://github.com/MIT-LCP/wfdb-python/blob/66853f1529ac26de105d28d2f8652fc39af5f107/wfdb/io/_signal.py#L704
def calc_adc_gain_baseline(fmt, minval, maxval):
        """
        Compute adc_gain and baseline parameters for a given channel.

        Parameters
        ----------
        fmt: int
            The channel that the adc_gain and baseline are being computed for.
        minvals: list
            The minimum values for each channel.
        maxvals: list
            The maximum values for each channel.

        Returns
        -------
        adc_gain : float
            Calculated `adc_gain` value for a given channel.
        baseline : int
            Calculated `baseline` value for a given channel.

        Notes
        -----
        This is the mapping equation:
            `digital - baseline / adc_gain = physical`
            `physical * adc_gain + baseline = digital`

        The original WFDB library stores `baseline` as int32.
        Constrain abs(adc_gain) <= 2**31 == 2147483648.

        This function does carefully deal with overflow for calculated
        int32 `baseline` values, but does not consider over/underflow
        for calculated float `adc_gain` values.

        """
        # Get the minimum and maximum (valid) storage values
        dmin, dmax = _digi_bounds(fmt)
        # add 1 because the lowest value is used to store nans
        dmin = dmin + 1
        # log.debug(f"dmin: {dmin}, dmax: {dmax}")

        pmin = minval
        pmax = maxval
        # log.debug(f"pmin: {pmin}, pmax: {pmax}")
        
        # Figure out digital samples used to store physical samples

        # If the entire signal is NAN, gain/baseline won't be used
        if pmin == np.nan:
            adc_gain = 1
            baseline = 1
        # If the signal is just one value, store one digital value.
        elif pmin == pmax:
            if pmin == 0:
                adc_gain = 1
                baseline = 1
            else:
                # All digital values are +1 or -1. Keep adc_gain > 0
                adc_gain = abs(1 / pmin)
                baseline = 0
        # Regular varied signal case.
        else:
            # The equation is: p = (d - b) / g

            # Approximately, pmax maps to dmax, and pmin maps to
            # dmin. Gradient will be equal to, or close to
            # delta(d) / delta(p), since intercept baseline has
            # to be an integer.

            # Constraint: baseline must be between +/- 2**31
            adc_gain = (dmax - dmin) / (pmax - pmin)
            baseline = dmin - adc_gain * pmin
            # log.debug(f"adc_gain 1: {adc_gain}, baseline: {baseline}")

            # Make adjustments for baseline to be an integer
            # This up/down round logic of baseline is to ensure
            # there is no overshoot of dmax. Now pmax will map
            # to dmax or dmax-1 which is also fine.
            if pmin > 0:
                baseline = int(np.ceil(baseline))
            else:
                baseline = int(np.floor(baseline))
                
            # log.debug(f"adc_gain 2: {adc_gain}, baseline: {baseline}")

            # After baseline is set, adjust gain correspondingly.Set
            # the gain to map pmin to dmin, and p==0 to baseline.
            # In the case where pmin == 0 and dmin == baseline,
            # adc_gain is already correct. Avoid dividing by 0.
            if dmin != baseline:
                adc_gain = (dmin - baseline) / pmin
                # log.debug(f"adc_gain 3: {adc_gain}, baseline: {baseline}")

        # Remap signal if baseline exceeds boundaries.
        # This may happen if pmax < 0.  essentially set dmax to MAX_I32, and pmax to 0
        if baseline > MAX_I32:
            # pmin maps to dmin, baseline maps to 2**31 - 1
            # pmax will map to a lower value than before
            log.warning(f"baseline > MAX_I32: {baseline} > {MAX_I32}.")
            # TCP: this is the wrong equation.  should numerator needs to be in params
            adc_gain = (MAX_I32 - dmin) / abs(pmin)
            # adc_gain = (MAX_I32) - dmin / abs(pmin)
            baseline = MAX_I32
            log.debug(f" adc_gain 4: {adc_gain}, baseline: {baseline}")
        # This may happen if pmin > 0.  essentially set dmin to MIN_I32, and pmin to 0
        elif baseline < MIN_I32:
            log.warning(f"baseline < MIN_I32: {baseline} < {MIN_I32}.")

            # pmax maps to dmax, baseline maps to -2**31 + 1
            adc_gain = (dmax - MIN_I32) / pmax
            baseline = MIN_I32
            log.debug(f" adc_gain 5: {adc_gain}, baseline: {baseline}")

        return adc_gain, baseline
#%%

# determine best format for the signal.  note that this only works if we have the same digital and physical types for all value arrays if we use extended digital or physical formats.
# do not change for now.
def get_format_for_array(values):
    mn = np.min(values)
    mx = np.max(values)
    if (isinstance(values, list) and isinstance(values[0], int)) or \
        (isinstance(values, np.ndarray) and np.issubdtype(values.dtype, np.integer)):
        for i in ['80', '311', '212', '16', '24', '32']:
            if mn > SAMPLE_VALUE_RANGE[i][0] and mx <= SAMPLE_VALUE_RANGE[i][1]:
                return (i, 'digital')
    elif (isinstance(values, list) and isinstance(values[0], float)) or \
        (isinstance(values, np.ndarray) and np.issubdtype(values.dtype, np.floating)):
        return (DEFAULT_FMT, 'physical')
        
    raise ValueError(f"Invalid values format type {type(values[0])}. should be numeric lists or ndarrays")


def write_wfdb_segment(output_dir, dir_name, file_prefix, 
                       start_ts,
                       values, sig_names, fs, 
                       UoMs=['mV'], samples_per_frame = None):
    # save the values to a file
    file_name = f"{file_prefix}_{int(start_ts.timestamp())}"
    # Convert to 2D ndarray, nxm,  n is signal length, m is number of signals
    
    row_min = [] 
    row_max = [] 
    if (samples_per_frame is None):
        # all have same length, only allows ndarray or 1D list
        if isinstance(values, np.ndarray):
            values_array = values
            row_min = np.nanmin(values_array, axis=0)
            row_max = np.nanmax(values_array, axis=0)
        elif isinstance(values, list) and all(isinstance(v, (int, float)) for v in values):
            values_array = np.array(values).reshape(-1, 1)
            row_min = np.nanmin(values_array)
            row_max = np.nanmax(values_array)
        else: 
            raise ValueError(f"Invalid values format type {type(values)}. samples per frame not specified so type should be 1D list or 2d ndarray")

    else:
        # samples_per_frame specified, allow list of 1D arrays or a 2D array
        if isinstance(values, np.ndarray):
            values_array = values
            row_min = list(np.nanmin(values_array, axis=0))
            row_max = list(np.nanmax(values_array, axis=0))
        elif isinstance(values, list):
            if all(isinstance(v, (int, float)) for v in values):
                values_array = [np.array(values).reshape(-1, 1)]
                row_min = [np.nanmin(values_array)]
                row_max = [np.nanmax(values_array)]
            elif all(isinstance(v, (list, np.ndarray)) for v in values):
                values_array = [
                    vals if isinstance(vals, np.ndarray) \
                        else np.array(vals).reshape(-1, 1) \
                            for vals in values
                    ]
                row_min = [np.nanmin(vals) for vals in values_array]
                row_max = [np.nanmax(vals) for vals in values_array]
            else:
                valtypes = set(type(v) for v in values)
                raise ValueError(f"Invalid values format type {valtypes}. should be numeric lists or ndarrays")
        else: 
            raise ValueError(f"Invalid values format type {type(values)}. should be numeric lists or ndarrays")
        
    sig_name_list = sig_names if isinstance(sig_names, list) else [sig_names]
    unit_list = UoMs if isinstance(UoMs, list) else [UoMs]
    
    # convert start date time to UTC
    # start_dt_str = start_ts.strftime('%Y-%m-%d %H:%M:%S.%f %z')
    # start_dt = datetime.datetime.strptime(start_dt_str, '%Y-%m-%d %H:%M:%S.%f %z')
    # start_dt = start_dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    start_dt = start_ts.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    
    # log.debug(f"datetime: {start_dt}, {start_ts}, {type(start_dt)}, {type(start_ts)} ")
    # Specify the directory where the WFDB file will be written
    wfdb_dir = os.path.join(output_dir, dir_name)
    os.makedirs(wfdb_dir, exist_ok=True)  # Ensure the directory exists
    # os.chdir(wfdb_dir)  # Change the working directory to the target directory

    # if any of the min and max are the same, then we need to set adc_gain and baseline
    fmt_list = [DEFAULT_FMT] * len(sig_name_list)
    adc_gain_list = []
    baseline_list = []
    for fmt, mn, mx in zip(fmt_list, row_min, row_max):
        adc_gain, baseline = calc_adc_gain_baseline(fmt, mn, mx)
        adc_gain_list.append(adc_gain)
        baseline_list.append(baseline)
        
    try:
        if (samples_per_frame is not None):
            rec = wfdb.Record(
                        record_name=file_name, 
                        n_sig = len(sig_name_list),
                        fs=fs, 
                        samps_per_frame=samples_per_frame,
                        units=unit_list, 
                        sig_name=sig_name_list,
                        e_p_signal=values_array,
                        base_datetime=start_dt,
                        fmt=fmt_list,
                        adc_gain=adc_gain_list,
                        baseline=baseline_list)
            rec.e_d_signal = rec.adc(expanded = True)
            rec.set_d_features(expanded = True)
            rec.set_defaults()
            rec.wrsamp(expanded=True, write_dir = wfdb_dir)
        else:
            wfdb.wrsamp(record_name=file_name, 
                        fs=fs, 
                        units=unit_list, 
                        sig_name=sig_name_list,
                        p_signal=values_array,
                        base_datetime=start_dt,
                        fmt=fmt_list,
                        adc_gain=adc_gain_list,
                        baseline=baseline_list,
                        write_dir = wfdb_dir)
    except Exception as e:
        log.debug(f"unit_list length: {len(unit_list)}")
        log.debug(f"sig_name_list length: {len(sig_name_list)}")
        log.debug(f"values_array shape: {values_array.shape if hasattr(values_array, 'shape') else [len(v) for v in values_array]}")
        log.debug(f"Minimum value in values_array: {row_min}")
        log.debug(f"Maximum value in values_array: {row_max}")
        log.debug(f"adc_gain_list length: {len(adc_gain_list)}")
        log.debug(f"baseline_list length: {len(baseline_list)}")
        log.debug(f"fmt_list length: {len(fmt_list)}")
        log.debug(f"samples per frame: {samples_per_frame}")
        log.error(f"1 writing {file_name}: {e}")
        log.debug(f"channel lengths = {[len(v) for v in values_array] if isinstance(values_array, list) else values_array.shape}, {samples_per_frame}")       
        
    # log.info(f"wrote {file_name} to {wfdb_dir}")
    return file_name
        
# example code:
