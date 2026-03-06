"""Integration tests: Parquet write / read round-trips via io_utils.parquet_io."""
import os
import pytest
import pandas as pd
import numpy as np
from io_utils.parquet_io import (
    write_hl7data_parquet,
    load_bed_parquet,
    load_bed_parquets2,
)


# ---------------------------------------------------------------------------
# Minimal waveform DataFrame matching the hl7lite parquet schema
# ---------------------------------------------------------------------------

def _make_df(n: int = 2) -> pd.DataFrame:
    ts = pd.Timestamp("2023-06-15T12:00:00", tz="UTC")
    te = pd.Timestamp("2023-06-15T12:01:00", tz="UTC")
    return pd.DataFrame({
        "hospital":    ["EUHM"] * n,
        "bed_unit":    ["MICU"] * n,
        "bed_id":      ["BED01"] * n,
        "channel":     [f"MDC_ECG_LEAD_{i}" for i in range(n)],
        "channel_id":  [""] * n,
        "channel_type":["WV_ECG"] * n,
        "msg_type":    ["MDC_OBS_WAVE_CTS"] * n,
        "start_t":     pd.array([ts] * n, dtype="datetime64[ns, UTC]"),
        "end_t":       pd.array([te] * n, dtype="datetime64[ns, UTC]"),
        "values":      [[100.0, 101.0, 102.0]] * n,
        "value_type":  ["NM"] * n,
        "UoM":         ["mV"] * n,
        "ref_range":   [""] * n,
        "pd_samp_ms":  [2.0] * n,
        "nsamp":       pd.array([3] * n, dtype="Int64"),
    })


# ---------------------------------------------------------------------------
# write_hl7data_parquet
# ---------------------------------------------------------------------------

class TestWriteParquet:
    def test_creates_file(self, tmp_path):
        df = _make_df()
        write_hl7data_parquet(str(tmp_path), "test.parquet", df)
        assert os.path.exists(tmp_path / "test.parquet")

    def test_creates_output_dir_if_missing(self, tmp_path):
        subdir = tmp_path / "newdir"
        df = _make_df()
        write_hl7data_parquet(str(subdir), "test.parquet", df)
        assert (subdir / "test.parquet").exists()

    def test_append_produces_readable_file(self, tmp_path):
        df = _make_df(1)
        write_hl7data_parquet(str(tmp_path), "test.parquet", df)
        write_hl7data_parquet(str(tmp_path), "test.parquet", df)
        assert (tmp_path / "test.parquet").exists()

    def test_multiple_writes_accumulate_rows(self, tmp_path):
        df = _make_df(1)
        write_hl7data_parquet(str(tmp_path), "test.parquet", df)
        write_hl7data_parquet(str(tmp_path), "test.parquet", df)
        pf_path = str(tmp_path / "test.parquet")
        loaded = pd.read_parquet(pf_path, engine="fastparquet")
        assert len(loaded) >= 2


# ---------------------------------------------------------------------------
# load_bed_parquet
# ---------------------------------------------------------------------------

class TestLoadBedParquet:
    def _write_bed_file(self, tmp_path):
        df = _make_df(2)
        fname = "BED_EUHM-MICU-BED01.parquet"
        write_hl7data_parquet(str(tmp_path), fname, df)
        return str(tmp_path / fname)

    def test_returns_tuple(self, tmp_path):
        path = self._write_bed_file(tmp_path)
        result = load_bed_parquet(path)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_bed_key_correct(self, tmp_path):
        path = self._write_bed_file(tmp_path)
        bed_key, _ = load_bed_parquet(path)
        assert bed_key == ("EUHM", "MICU", "BED01")

    def test_dataframe_returned(self, tmp_path):
        path = self._write_bed_file(tmp_path)
        _, df = load_bed_parquet(path)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_expected_columns_present(self, tmp_path):
        path = self._write_bed_file(tmp_path)
        _, df = load_bed_parquet(path)
        for col in ("hospital", "bed_unit", "bed_id", "channel"):
            assert col in df.columns

    def test_hospital_value_preserved(self, tmp_path):
        path = self._write_bed_file(tmp_path)
        _, df = load_bed_parquet(path)
        assert (df["hospital"] == "EUHM").all()

    def test_channel_values_preserved(self, tmp_path):
        path = self._write_bed_file(tmp_path)
        _, df = load_bed_parquet(path)
        assert set(df["channel"]) == {"MDC_ECG_LEAD_0", "MDC_ECG_LEAD_1"}


# ---------------------------------------------------------------------------
# load_bed_parquets2
# ---------------------------------------------------------------------------

class TestLoadBedParquets2:
    def test_loads_multiple_files_into_dict(self, tmp_path):
        df = _make_df(1)
        paths = []
        for i, bed in enumerate(["BED01", "BED02"]):
            fname = f"BED_EUHM-MICU-{bed}.parquet"
            df_bed = df.copy()
            df_bed["bed_id"] = bed
            df_bed["channel"] = f"MDC_ECG_{i}"
            write_hl7data_parquet(str(tmp_path), fname, df_bed)
            paths.append(str(tmp_path / fname))

        result = load_bed_parquets2(paths)
        assert isinstance(result, dict)
        assert len(result) == 2

    def test_dict_keyed_by_bed_tuple(self, tmp_path):
        df = _make_df(1)
        fname = "BED_EUHM-MICU-BED01.parquet"
        write_hl7data_parquet(str(tmp_path), fname, df)

        result = load_bed_parquets2([str(tmp_path / fname)])
        assert ("EUHM", "MICU", "BED01") in result
