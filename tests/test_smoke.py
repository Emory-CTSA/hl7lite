"""Smoke tests: verify every module and both Cython extensions import cleanly."""


def test_import_hl7lite():
    import hl7lite
    assert hl7lite.__version__ == "0.1.0"


def test_version_is_semver():
    import hl7lite
    parts = hl7lite.__version__.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_import_tokenizer():
    from hl7lite import hl7_tokenizer
    assert callable(hl7_tokenizer.tokenize_hl7_message)


def test_import_datatypes():
    from hl7lite import hl7_datatypes
    assert hasattr(hl7_datatypes, "DataType")
    assert hasattr(hl7_datatypes, "convert_field")
    assert hasattr(hl7_datatypes, "parse_time")


def test_import_ds():
    from hl7lite import hl7_ds
    assert callable(hl7_ds.hl7_data_factory)
    assert hasattr(hl7_ds, "HierarchicalMessage")


def test_import_io():
    from hl7lite import hl7_io
    assert callable(hl7_io.read_hl7_file)
    assert callable(hl7_io.convert_msg_to_json)


def test_import_string_utils():
    from hl7lite import string_utils
    assert callable(string_utils.compress_string)
    assert callable(string_utils.decompress_string)
    assert callable(string_utils.sanitize_nonascii_python)


def test_cython_parse_time():
    """Cython parse_time_tz extension must be present and callable."""
    from hl7lite.parse_time_tz import c_parse_time, c_parse_time_batch
    assert callable(c_parse_time)
    assert callable(c_parse_time_batch)


def test_cython_sanitize_unicode():
    """Cython sanitize_unicode extension must be present and callable."""
    from hl7lite.sanitize_unicode import c_sanitize_unicode
    assert callable(c_sanitize_unicode)


def test_import_io_utils():
    import io_utils
    from io_utils import parquet_io
    assert callable(parquet_io.write_hl7data_parquet)
    assert callable(parquet_io.load_bed_parquet)


def test_import_emory():
    import emory
    from emory import fs_utils
    assert callable(fs_utils.get_file_list)
