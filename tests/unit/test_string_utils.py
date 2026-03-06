"""Unit tests for string_utils: sanitize_nonascii_python and compress/decompress."""
import pytest
from hl7lite.string_utils import (
    sanitize_nonascii_python,
    compress_string,
    decompress_string,
)


# ---------------------------------------------------------------------------
# sanitize_nonascii_python
# ---------------------------------------------------------------------------

class TestSanitizeNonasciiPython:
    def test_pure_ascii_unchanged(self):
        text = "Hello, World! 123"
        result, found = sanitize_nonascii_python(text)
        assert result == text
        assert found is None

    def test_empty_string(self):
        result, found = sanitize_nonascii_python("")
        assert result == ""
        assert found is None

    def test_left_double_quote_replaced(self):
        text = "\u201cHello\u201d"  # "Hello"
        result, found = sanitize_nonascii_python(text)
        assert "\u201c" not in result
        assert "\u201d" not in result

    def test_smart_single_quotes_replaced(self):
        text = "\u2018it\u2019s"  # 'it's
        result, found = sanitize_nonascii_python(text)
        assert "\u2018" not in result
        assert "\u2019" not in result

    def test_em_dash_replaced(self):
        text = "one\u2014two"  # em dash
        result, found = sanitize_nonascii_python(text)
        assert "\u2014" not in result
        # replacement should contain something (hyphen or similar)
        assert "one" in result
        assert "two" in result

    def test_ellipsis_replaced(self):
        text = "wait\u2026"  # horizontal ellipsis …
        result, found = sanitize_nonascii_python(text)
        assert "\u2026" not in result

    def test_found_set_contains_replaced_chars(self):
        text = "\u201ctest\u201d"
        _, found = sanitize_nonascii_python(text)
        assert isinstance(found, set)
        assert len(found) > 0

    def test_mixed_ascii_and_unicode(self):
        text = "Hello \u201cworld\u201d!"
        result, found = sanitize_nonascii_python(text)
        assert "Hello" in result
        assert "world" in result
        assert "!" in result
        assert "\u201c" not in result

    def test_result_is_all_ascii(self):
        text = "\u201cCaf\u00e9 \u2014 Emory\u2019s \u2018best\u2019\u201d"
        result, _ = sanitize_nonascii_python(text)
        assert all(ord(c) < 128 for c in result), f"Non-ASCII chars remain: {result}"


# ---------------------------------------------------------------------------
# compress_string / decompress_string round-trips
# ---------------------------------------------------------------------------

class TestCompressRoundtrip:
    @pytest.mark.parametrize("method", ["zlib", "lz4", "none"])
    def test_roundtrip(self, method):
        text = "Hello, World! " * 20
        compressed = compress_string(text, compression=method)
        assert decompress_string(compressed, compression=method) == text

    @pytest.mark.parametrize("method", ["zlib", "lz4", "none"])
    def test_roundtrip_empty_string(self, method):
        compressed = compress_string("", compression=method)
        assert decompress_string(compressed, compression=method) == ""

    @pytest.mark.parametrize("method", ["zlib", "lz4", "none"])
    def test_roundtrip_single_char(self, method):
        compressed = compress_string("x", compression=method)
        assert decompress_string(compressed, compression=method) == "x"


class TestCompressionSize:
    def test_zlib_compresses_repetitive_data(self):
        text = "A" * 1000
        assert len(compress_string(text, "zlib")) < len(text)

    def test_lz4_compresses_repetitive_data(self):
        text = "A" * 1000
        assert len(compress_string(text, "lz4")) < len(text)

    def test_none_returns_original(self):
        text = "short"
        assert compress_string(text, "none") == text


class TestCompressionContent:
    def test_zlib_output_is_base64(self):
        import base64
        compressed = compress_string("test data", "zlib")
        # should not raise
        base64.b64decode(compressed)

    def test_lz4_output_is_base64(self):
        import base64
        compressed = compress_string("test data", "lz4")
        base64.b64decode(compressed)

    def test_unicode_content_survives_roundtrip(self):
        # After sanitization, content is ASCII; verify basic unicode that fits in str
        text = "temperature: 98.6\u00b0F"  # degree symbol
        for method in ["zlib", "lz4", "none"]:
            assert decompress_string(compress_string(text, method), method) == text
