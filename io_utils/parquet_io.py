import pandas as pd
import os
from fastparquet import ParquetFile, write
import shutil
from datetime import datetime
from hl7lite.hl7_datatypes import missing_values
import numpy as np
from emory.fs_utils import get_file_list

import logging
log = logging.getLogger(__name__)

# Convert column types based on object_encoding mapping
object_encoding = {
    'msh_time': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
    'msh_send_app': (str, 'string'),
    'src': (str, 'string'),
    'profile': (str, 'string'),
    'control_id': (str, 'string'),
    'hospital': (str, 'string'),
    'bed_unit': (str, 'string'),
    'bed_id': (str, 'string'),
    'pid': (str, 'string'),
    'visit_id': (str, 'string'),
    'pat_fn': (str, 'string'),
    'pat_ln': (str, 'string'),
    'msg_type': (str, 'string'),
    'channel': (str, 'string'),
    'channel_id': (str, 'string'),
    'channel_type': (str, 'string'),
    'start_t': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
    'end_t': (np.datetime64, 'datetime64[ns, UTC]'),    # timezone-aware UTC
    'obx_start_t': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
    'obx_end_t': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
    'values': (list, 'object'),  # keep as lists - parquet cannot handle mixture of object and int/floats
    # 'values_num': (float, 'float'),
    # 'values_str': (str, 'string'),
    'value_type': (str, 'string'),
    'UoM': (str, 'string'),
    'ref_range': (str, 'string'),
    'pd_samp_ms': (float, 'float'),
    'nsamp': (int, 'Int64'),
    'seg_id': (int, 'Int64'),
    'dir': (str, 'string'),
    'file': (str, 'string'),
}

# no significant space savings vs string (possibly because of fastparquet engine).  use string for simplicity.
# # Convert column types based on object_encoding mapping
# object_encoding = {
#    'msh_time': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
#     'msh_send_app': (str, 'category'),
#     'src': (str, 'category'),
#     'profile': (str, 'category'),
#     'control_id': (str, 'category'),
#     'hospital': (str, 'category'),
#     'bed_unit': (str, 'category'),
#     'bed_id': (str, 'category'),
#     'pid': (str, 'string'),
#     'visit_id': (str, 'string'),
#     'pat_fn': (str, 'string'),
#     'pat_ln': (str, 'string'),
#     'msg_type': (str, 'category'),
#     'channel': (str, 'category'),
#     'channel_id': (str, 'category'),
#     'channel_type': (str, 'category'),
#     'start_t': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
#     'end_t': (np.datetime64, 'datetime64[ns, UTC]'),    # timezone-aware UTC
#     'obx_start_t': (np.datetime64, 'datetime64[ns, UTC]'),  # timezone-aware UTC
#     'values': (list, 'object'),  # keep as object for lists
#     # 'values_num': (float, 'float'),
#     # 'values_str': (str, 'string'),
#     'value_type': (str, 'category'),
#     'UoM': (str, 'category'),
#     'ref_range': (str, 'category'),
#     'pd_samp_ms': (float, 'float'),
#     'nsamp': (int, 'Int64'),
#     'seg_id': (int, 'Int64'),
#     'dir': (str, 'string'),
#     'file': (str, 'string'),
# }

# perform actual write.
def write_hl7data_parquet(output_dir, file_name, df: pd.DataFrame):
    # log.info(f"Writing {len(df)} rows to {file_name} in {output_dir}")
    
    if df is None or df.empty:
        return
    
    outfile = os.path.join(output_dir, file_name)
    df = df.copy().reset_index(drop=True)
    
    # safer, but likely slower and uses more memory.    
    # if os.path.exists(outfile):
    #     df1 = pd.read_parquet(outfile, engine='fastparquet', memory_map=True)
    #     df = pd.concat([df1, df], ignore_index=True)
    # df.to_parquet(outfile, engine='fastparquet', compression='snappy', index=False)
    
    # since datetime64 is timezone aware, we need to standardize the timezone.  to avoid daylight savings vs not, use UTC.
    # log.debug(df.dtypes)
    
    # start_t, end_t and obx_start_t are all datetime, not np.datetime64 types, so a conversion is needed.
    # the datetime should have timezone info, though.
    dtypes = {col: dtype[1] for col, dtype in object_encoding.items() if col in df.columns}
    df = df.astype(dtypes, copy=True, errors='raise')
    # df['start_t'] = df['start_t'].dt.tz_convert('UTC')
    # df['end_t'] = df['end_t'].dt.tz_convert('UTC')
    # try:
    #     df['obx_start_t'] = df['obx_start_t'].dt.tz_convert('UTC')
    # except Exception as e:
    #     log.debug(df['obx_start_t'].dtype)
    #     for x in df['obx_start_t']:
    #         if not (isinstance(x, np.datetime64) or isinstance(x, type(missing_values[np.datetime64]))):
    #             log.error(f"Found non-datetime value in 'obx_start_t' so can't use .dt: {x}")
    #     raise(e)


    # for col, dtype in object_encoding.items():
    #     if col in df.columns:
    #         try:
    #             if dtype == 'category':
    #                 df[col] = df[col].astype('category')
    #             elif dtype == 'string':
    #                 df[col] = df[col].astype('string')
    #             elif dtype == 'datetime64[ns]':
    #                 df[col] = pd.to_datetime(df[col], errors='coerce')
    #             elif dtype == 'float64':
    #                 df[col] = pd.to_numeric(df[col], errors='coerce')
    #             elif dtype == 'Int64':
    #                 df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    #             # 'object' for lists, leave as is
    #         except Exception as e:
    #             log.warning(f"Could not convert column '{col}' to {dtype}: {e}")
    
    # PERFORMANCE, on lenovo P1G4 waveform, vitals, alarms for 1 hr, EJCH, EUH, EUHM - about 20min.
    # df.to_parquet - about 20 min  (writing may be slightly faster)
    # fastparquet.write - about 10 min.
    try:
        # write the parquet file, append if it exists.
        if os.path.exists(outfile):
            # write(outfile, df, compression='snappy', append=True)
            df.to_parquet(outfile, engine='fastparquet', compression='snappy', append=True)
        else:
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            # write(outfile, df, compression='snappy', file_scheme='simple')
            df.to_parquet(outfile, engine='fastparquet', compression='snappy', file_scheme='simple')
    except Exception as e:
        first_bed_id = df['bed_id'].iloc[0] if 'bed_id' in df.columns and not df.empty else missing_values[str]
        # first_pid = df['pid'].iloc[0] if 'pid' in df.columns and not df.empty else missing_values[str]
        # log.error(f"Failed to write parquet file '{outfile}' for bedid '{first_bed_id}' and patient '{first_pid}', number of record {len(df)}: {e}")
        log.error(f"Failed to write parquet file '{outfile}' for bedid '{first_bed_id}'', number of record {len(df)}: {e}")
        # DEBUG
        df2 = df.copy().drop(columns=['values'])
        # log.debug(f"df2 columns: {df2.to_markdown()}")
        raise(e)


# Note : VERY SLOW, 20X slower - have to open multiple files.  if i is left as 0, accumulates into _0.parquet
def hl7_to_parquet_bed(hl7_dir: str,  df: pd.DataFrame, i: int = 0):
    if df is None or df.empty:
        return

    # log.info(f"Writing {len(df)} rows to patient-bed parquet in {hl7_dir}/stitched")
    # log.debug(df[['pid', 'bed_id', 'hospital', 'bed_unit']].head(100))
    groups = df.groupby(['hospital', 'bed_unit', 'bed_id'], dropna=False)
    # testfn = os.path.join(hl7_dir, 'stitched', 'test.parquet')
    # testfn2 = os.path.join(hl7_dir, 'stitched', 'test2.parquet')
    for (hosp, unit, bed), group_df in groups:
        unit2 = unit.replace(" ", "_").replace("-", "_")
        bed2 = bed.replace(" ", "_").replace("-", "_")
        fname = f"BED_{hosp}-{unit2}-{bed2}.parquet"
        # log.info(f"Writing group {len(group_df)} rows for [{bed}] and [{pid}] to {fname} in {hl7_dir}/stitched")
        fn = os.path.join(hl7_dir, 'stitched', fname)
        # DEBUGGING ONLY
        # if os.path.exists(fn):
        #     shutil.copyfile(os.path.join(hl7_dir, 'stitched', fname), testfn)

        try:
            write_hl7data_parquet(os.path.join(hl7_dir, 'stitched'), 
                                fname,
                                group_df)
        except Exception as e:            
            # #### debugging now,
            # log.debug(f"the groups data types are {group_df.dtypes.to_dict()}")
            # test_df = group_df.copy()
            # # if os.path.exists(testfn):
            # #     shutil.copyfile(testfn, testfn2)

            raise(e)

        
# write out directly (mixed beds/patients) if i is set.  else accumualted into _0.parquet.
def hl7_to_parquet_direct(hl7_dir: str, df: pd.DataFrame, i: int = 0):
    if df is None or df.empty:
        return
    
    # if DEBUG or not os.path.exists(datafile):
    write_hl7data_parquet(os.path.join(hl7_dir, 'stitched'), 
                          os.path.basename(hl7_dir) + "-" + str(i) + ".parquet",
                          df)
    # else:
    #     log.info(f"loading {datafile}")
    #     try:
    #         df = pd.read_parquet(datafile, engine='fastparquet')
    #         df['values'] = df['values'].apply(lambda x: x if isinstance(x, list) else x.tolist() if isinstance(x, np.ndarray) else [x])
    #     except Exception as e:
    #         log.info(f"load parquet failed.  reading {hl7_file}")
    #         data = read_hl7_file(hl7_file)
    #         df = pd.DataFrame(data)
    #         write_hl7data_parquet(os.path.join(hl7_dir, 'stitched'), 
    #                      os.path.splitext(os.path.basename(hl7_file))[0] + ".parquet",
    #                      df)

def finalize_parquet(hl7_dir: str):
    # parquet_files = get_file_list(hl7_dir, extension='.parquet')
    # if not parquet_files:
    #     return None
    # for f in parquet_files:
    #     pf = ParquetFile(f)
    #     if pf.count == 0:
    #         log.warning(f"Parquet file {f} is empty, removing it.")
    #         os.remove(f)
    #     pf.merge(f)  # can't find merge function.
    ...


def load_direct_parquets(hl7_dir: str, file_start:int, num_files: int):
    parquet_files = get_file_list(hl7_dir, extension='.parquet')
    if not parquet_files:
        return None
    
    if isinstance(parquet_files, dict):
        # flatten dict to list
        parquet_files = [f for files in parquet_files.values() for f in files]
    
    parquet_files.sort(key=lambda x: int(x.split('-')[-1].split('.')[0]))
    
    file_start = max(0, file_start) # ensure file_start is non-negative
    file_start = min(file_start, len(parquet_files) - 1)  # ensure file_start does not exceed the number of files
    nfiles = len(parquet_files) if (num_files is None) or (num_files < 0) else min(num_files, len(parquet_files))
    file_end = min(file_start + nfiles, len(parquet_files))  # ensure we do not exceed the number of files
    
    parquet_files = parquet_files[file_start:file_end]
    log.info(f"Loading {len(parquet_files)} parquet files from {hl7_dir}/stitched, starting at index {file_start}")
    df = pd.concat([pd.read_parquet(f, engine='fastparquet') for f in parquet_files], ignore_index=True)
    value_cols = ['values', 'index']  #['values', 'values_num', 'values_str', 'index']  # adjust as needed
    cols_to_check = [col for col in df.columns if col not in value_cols]
    return df.drop_duplicates(subset=cols_to_check)

def load_bed_parquets(hl7_dir: str, file_start:int = 0, num_files: int = -1, batch_size: int = 0):
    parquet_files = get_file_list(hl7_dir, extension='.parquet')
    if not parquet_files:
        return None
    
    file_start = max(0, file_start) # ensure file_start is non-negative
    file_start = min(file_start, len(parquet_files) - 1)  # ensure file_start does not exceed the number of files
    nfiles = len(parquet_files) if (num_files is None) or (num_files < 0) else min(num_files, len(parquet_files))
    file_end = min(file_start + nfiles, len(parquet_files))  # ensure we do not exceed the number of files

    keys = list(parquet_files.keys())
    keys.sort()
    keys = keys[file_start:file_end]

    parquet_files_list = []
    for key in keys:
        parquet_files_list.extend(parquet_files[key])
    
    if batch_size <= 0:
        log.info(f"Loading {len(parquet_files)} bed parquets from {hl7_dir}")
        return load_bed_parquets2(parquet_files_list)
    else:    
        for batch in range(0, len(parquet_files_list), batch_size):
            log.info(f"Loading bed parquets batch {batch} to {min(batch+batch_size, len(parquet_files_list))}")
            file_batch = parquet_files_list[batch:batch+batch_size]
            
            yield load_bed_parquets2(file_batch)
        

# loads 1 bed's parquet.  assumes each parquet file has only a single bed.
def load_bed_parquet(filename: str):

    df = pd.read_parquet(filename, engine='fastparquet')

    if df is None:
        return (None, None, None), None
    
    # Ensure the required columns exist
    required_cols = ['hospital', 'bed_unit', 'bed_id']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"File {filename} is missing one of the required columns: {required_cols}")
    
    # Get unique combinations of hospital, unit, bed
    unique_combinations = df[required_cols].drop_duplicates()
    if len(unique_combinations) != 1:
        raise ValueError(f"File {filename} contains multiple hospital/unit/bed combinations.")
    
    hospital, unit, bed = unique_combinations.iloc[0]

    return (hospital, unit, bed), df


def load_bed_parquets2(filenames: list[str]):
    dfs = {}
    # index is actually not there.
    value_cols = ['values', 'index']  #['values', 'values_num', 'values_str', 'index']  # adjust as needed
    # load all the files.
    for f in filenames:
        key, df = load_bed_parquet(f)
        if df is None:
            continue

        if key not in dfs:
            dfs[key] = []

        dfs[key].append(df)

    # concatenate and drop duplicates for each bed.
    for key in dfs.keys():
        dfs[key] = pd.concat(dfs[key], ignore_index=True)
        # drop duplicates based on all columns except value_cols
        cols_to_check = [col for col in dfs[key].columns if col not in value_cols]
        dfs[key] = dfs[key].drop_duplicates(subset=cols_to_check)

    return dfs