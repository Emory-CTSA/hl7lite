"""Unit tests for hl7_datatypes: fix_time, parse_time_python, convert_field."""
import pytest
import numpy as np
import pandas as pd
from hl7lite.hl7_datatypes import (
    DataType,
    convert_field,
    parse_time_python,
    fix_time,
    missing_values,
)


# ---------------------------------------------------------------------------
# fix_time
# ---------------------------------------------------------------------------

class TestFixTime:
    def test_valid_hour_timezone_unchanged(self):
        assert fix_time("20230615120000-0400") == "20230615120000-0400"

    def test_utc_unchanged(self):
        assert fix_time("20230615120000+0000") == "20230615120000+0000"

    def test_half_hour_tz_unchanged(self):
        assert fix_time("20230615120000+0530") == "20230615120000+0530"

    def test_minus0359_corrected_to_minus0400(self):
        assert fix_time("20230615120000-0359") == "20230615120000-0400"

    def test_minus0401_corrected_to_minus0400(self):
        assert fix_time("20230615120000-0401") == "20230615120000-0400"

    def test_minus0459_corrected_to_minus0500(self):
        assert fix_time("20230615120000-0459") == "20230615120000-0500"

    def test_minus0501_corrected_to_minus0500(self):
        assert fix_time("20230615120000-0501") == "20230615120000-0500"

    def test_no_timezone_unchanged(self):
        # No timezone suffix — ends with seconds digits, not special suffix
        ts = "20230615120000"
        assert fix_time(ts) == ts


# ---------------------------------------------------------------------------
# parse_time_python
# ---------------------------------------------------------------------------

class TestParseTimePython:
    def test_datetime_with_negative_tz(self):
        result = parse_time_python("20230615120000-0400")
        assert isinstance(result, np.datetime64)

    def test_datetime_with_positive_tz(self):
        result = parse_time_python("20230615120000+0000")
        assert isinstance(result, np.datetime64)

    def test_datetime_without_tz(self):
        result = parse_time_python("20230615120000")
        assert isinstance(result, np.datetime64)

    def test_datetime_with_fractional_seconds_and_tz(self):
        result = parse_time_python("20230615120000.123-0400")
        assert isinstance(result, np.datetime64)

    def test_epoch_ns_returns_integer(self):
        result = parse_time_python("20230615120000-0400", as_epoch_ns=True)
        assert isinstance(result, (int, np.int64))

    def test_epoch_ns_is_positive(self):
        # 2023-06-15 is well past the epoch
        result = parse_time_python("20230615120000-0400", as_epoch_ns=True)
        assert result > 0

    def test_timezone_negative_offset_shifts_correctly(self):
        # -0400 means UTC+4h, so 12:00-0400 == 16:00 UTC
        utc = parse_time_python("20230615120000-0400", as_epoch_ns=True)
        naive = parse_time_python("20230615160000+0000", as_epoch_ns=True)
        assert utc == naive

    def test_empty_string_raises(self):
        with pytest.raises((ValueError, IndexError)):
            parse_time_python("")


# ---------------------------------------------------------------------------
# convert_field — scalar string inputs
# ---------------------------------------------------------------------------

class TestConvertFieldScalar:
    def test_int_conversion(self):
        assert convert_field("42", DataType.INT, as_string=False) == 42

    def test_float_conversion(self):
        result = convert_field("3.14", DataType.FLOAT, as_string=False)
        assert abs(result - 3.14) < 1e-10

    def test_numeric_int_string(self):
        result = convert_field("7", DataType.NUMERIC, as_string=False)
        assert result == 7

    def test_numeric_float_string(self):
        result = convert_field("7.5", DataType.NUMERIC, as_string=False)
        assert result == 7.5

    def test_str_passthrough(self):
        assert convert_field("hello", DataType.STR, as_string=False) == "hello"

    def test_str_or_list_passthrough(self):
        assert convert_field("hello", DataType.STR_OR_LIST, as_string=False) == "hello"

    def test_list_of_str_wraps_single(self):
        assert convert_field("hello", DataType.LIST_OF_STR, as_string=False) == ["hello"]

    def test_list_of_int_wraps_single(self):
        assert convert_field("5", DataType.LIST_OF_INT, as_string=False) == [5]

    def test_list_of_float_wraps_single(self):
        assert convert_field("2.5", DataType.LIST_OF_FLOAT, as_string=False) == [2.5]


# ---------------------------------------------------------------------------
# convert_field — missing values for empty string
# ---------------------------------------------------------------------------

class TestConvertFieldMissing:
    def test_empty_str_with_int_type(self):
        result = convert_field("", DataType.INT, as_string=False)
        assert result == np.iinfo(np.int64).min

    def test_empty_str_with_float_type(self):
        result = convert_field("", DataType.FLOAT, as_string=False)
        assert np.isnan(result)

    def test_empty_str_with_str_type(self):
        assert convert_field("", DataType.STR, as_string=False) == ""

    def test_empty_str_with_numeric_type(self):
        result = convert_field("", DataType.NUMERIC, as_string=False)
        assert np.isnan(result)


# ---------------------------------------------------------------------------
# convert_field — list inputs
# ---------------------------------------------------------------------------

class TestConvertFieldList:
    def test_list_of_str_passthrough(self):
        data = ["a", "b", "c"]
        assert convert_field(data, DataType.LIST_OF_STR, as_string=False) == data

    def test_list_of_int(self):
        assert convert_field(["1", "2", "3"], DataType.LIST_OF_INT, as_string=False) == [1, 2, 3]

    def test_list_of_float(self):
        result = convert_field(["1.1", "2.2"], DataType.LIST_OF_FLOAT, as_string=False)
        assert result == pytest.approx([1.1, 2.2])

    def test_list_of_numeric_all_ints(self):
        result = convert_field(["1", "2", "3"], DataType.LIST_OF_NUMERIC, as_string=False)
        assert result == [1, 2, 3]

    def test_list_of_numeric_all_floats(self):
        result = convert_field(["1.0", "2.5", "3.0"], DataType.LIST_OF_NUMERIC, as_string=False)
        assert result == pytest.approx([1.0, 2.5, 3.0])

    def test_list_to_str_joins_with_caret(self):
        result = convert_field(["A", "B", "C"], DataType.STR, as_string=False)
        assert result == "A^B^C"

    def test_list_to_invalid_scalar_type_raises(self):
        with pytest.raises(ValueError):
            convert_field(["1", "2"], DataType.INT, as_string=False)


# ---------------------------------------------------------------------------
# convert_field — as_string=True (passthrough / fix_time mode)
# ---------------------------------------------------------------------------

class TestConvertFieldAsString:
    def test_str_passthrough(self):
        assert convert_field("hello", DataType.STR, as_string=True) == "hello"

    def test_datetime_applies_fix_time(self):
        # fix_time should be applied when as_string=True, DataType.DATETIME
        result = convert_field("20230615120000-0359", DataType.DATETIME, as_string=True)
        assert result == "20230615120000-0400"

    def test_list_passthrough_for_list_type(self):
        data = ["x", "y"]
        assert convert_field(data, DataType.LIST_OF_STR, as_string=True) == data

    def test_none_raises(self):
        with pytest.raises(ValueError):
            convert_field(None, DataType.STR, as_string=False)


# ---------------------------------------------------------------------------
# missing_values table sanity
# ---------------------------------------------------------------------------

class TestMissingValues:
    def test_int_missing_is_int64_min(self):
        assert missing_values[int] == np.iinfo(np.int64).min

    def test_float_missing_is_nan(self):
        assert np.isnan(missing_values[float])

    def test_datetime_missing_is_nat(self):
        assert missing_values[np.datetime64] is pd.NaT

    def test_str_missing_is_empty(self):
        assert missing_values[str] == ""

    def test_list_missing_is_empty_list(self):
        assert missing_values[list] == []
