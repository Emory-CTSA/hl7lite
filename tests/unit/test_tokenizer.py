"""Unit tests for hl7_tokenizer."""
import pytest
from hl7lite.hl7_tokenizer import (
    tokenize_hl7_message,
    get_with_default,
    _segment_to_fields,
    _field_to_repetitions,
    _repetition_to_components,
    _component_to_subcomponents,
)

# ---------------------------------------------------------------------------
# Minimal test messages (no fixtures needed – these are tokenizer-only tests)
# ---------------------------------------------------------------------------

_MINIMAL_MSH = (
    "MSH|^~\\&|CAPSULE|EUHM|RECEIVER|HOSPITAL|20230615120000-0400||ORU^R01|CTRL001|P|2.3\n"
    "PID|1|PAT001\n"
    "PV1|1|I|EUH-4TN-T434\n"
)

_WITH_OBR_OBX = (
    "MSH|^~\\&|CAPSULE|EUHM|RECEIVER|HOSPITAL|20230615120000-0400||ORU^R01|CTRL001|P|2.3\n"
    "PID|1|PAT001\n"
    "PV1|1|I|EUH-4TN-T434\n"
    "OBR|1||OBS001|69121^MDC_OBS_WAVE_CTS^MDC|||20230615120000-0400|20230615120100-0400\n"
    "OBX|1|NM|MDC_ECG_LEAD_I^MDC_ECG_LEAD_I^Local||100|mV\n"
)


# ---------------------------------------------------------------------------
# tokenize_hl7_message — structure
# ---------------------------------------------------------------------------

class TestTokenizeReturnType:
    def test_returns_tuple_of_list_and_set(self):
        segments, seg_names = tokenize_hl7_message(_MINIMAL_MSH)
        assert isinstance(segments, list)
        assert isinstance(seg_names, set)

    def test_segment_count(self):
        segments, _ = tokenize_hl7_message(_MINIMAL_MSH)
        assert len(segments) == 3  # MSH + PID + PV1

    def test_seg_names_msh_pid_pv1(self):
        _, seg_names = tokenize_hl7_message(_MINIMAL_MSH)
        assert {"MSH", "PID", "PV1"}.issubset(seg_names)

    def test_seg_names_with_obr_obx(self):
        _, seg_names = tokenize_hl7_message(_WITH_OBR_OBX)
        assert {"MSH", "PID", "PV1", "OBR", "OBX"}.issubset(seg_names)

    def test_raises_if_not_starting_with_msh(self):
        with pytest.raises(ValueError, match="MSH"):
            tokenize_hl7_message("PID|1|PAT001\nMSH|^~\\&|...\n")


# ---------------------------------------------------------------------------
# tokenize_hl7_message — field values at level=2 (no component parsing)
# ---------------------------------------------------------------------------

class TestMSHFieldsLevel2:
    def setup_method(self):
        self.segments, _ = tokenize_hl7_message(_MINIMAL_MSH, level=2)
        self.msh = self.segments[0]

    def test_segment_label(self):
        assert self.msh[0] == "MSH"

    def test_sending_app(self):
        assert self.msh[2] == "CAPSULE"   # MSH.3

    def test_sending_facility(self):
        assert self.msh[3] == "EUHM"      # MSH.4

    def test_message_type_unparsed_at_level2(self):
        # level=2 → no component splitting
        assert self.msh[8] == "ORU^R01"   # MSH.9, still a plain string

    def test_control_id(self):
        assert self.msh[9] == "CTRL001"   # MSH.10


# ---------------------------------------------------------------------------
# tokenize_hl7_message — component parsing at default level
# ---------------------------------------------------------------------------

class TestComponentParsingDefaultLevel:
    def setup_method(self):
        self.segments, _ = tokenize_hl7_message(_WITH_OBR_OBX)

    def test_msh9_parsed_into_list(self):
        msh = self.segments[0]
        # "ORU^R01" → ['ORU', 'R01']
        assert isinstance(msh[8], list)
        assert msh[8][0] == "ORU"
        assert msh[8][1] == "R01"

    def test_obx3_parsed_into_list(self):
        obx = next(s for s in self.segments if s[0] == "OBX")
        # "MDC_ECG_LEAD_I^MDC_ECG_LEAD_I^Local" → list
        assert isinstance(obx[3], list)
        assert obx[3][0] == "MDC_ECG_LEAD_I"
        assert obx[3][2] == "Local"

    def test_obr4_parsed_into_list(self):
        obr = next(s for s in self.segments if s[0] == "OBR")
        # "69121^MDC_OBS_WAVE_CTS^MDC" → ['69121', 'MDC_OBS_WAVE_CTS', 'MDC']
        assert isinstance(obr[4], list)
        assert obr[4][0] == "69121"
        assert obr[4][1] == "MDC_OBS_WAVE_CTS"


# ---------------------------------------------------------------------------
# tokenize_hl7_message — repetition separator
# ---------------------------------------------------------------------------

class TestRepetitionSeparator:
    def test_tilde_produces_list(self):
        msg = (
            "MSH|^~\\&|CAPSULE|EUHM|R|H|20230615120000-0400||ORU^R01|C|P|2.3\n"
            "PID|1|PAT001~ALTPAT\n"
            "PV1|1|I\n"
        )
        segments, _ = tokenize_hl7_message(msg, level=3)
        pid = next(s for s in segments if s[0] == "PID")
        assert isinstance(pid[2], list)
        assert "PAT001" in pid[2]
        assert "ALTPAT" in pid[2]


# ---------------------------------------------------------------------------
# _segment_to_fields — direct
# ---------------------------------------------------------------------------

class TestSegmentToFields:
    def test_basic_pid(self):
        fields = _segment_to_fields("PID|1|PAT001|PAT001||DOE^JOHN||19800101|M", level=2)
        assert fields[0] == "PID"
        assert fields[1] == "1"
        assert fields[2] == "PAT001"

    def test_msh2_preserved(self):
        # '^~\\&' at MSH[1] must not be split further
        fields = _segment_to_fields("MSH|^~\\&|APP|FAC|R|H|20230615|S|ORU^R01|C|P|2.3")
        assert fields[1] == "^~\\&"

    def test_component_split_at_level4(self):
        fields = _segment_to_fields("OBX|1|NM|CODE^NAME^SYS||100|mV", level=4)
        assert isinstance(fields[3], list)
        assert fields[3] == ["CODE", "NAME", "SYS"]


# ---------------------------------------------------------------------------
# get_with_default
# ---------------------------------------------------------------------------

class TestGetWithDefault:
    def setup_method(self):
        self.segments, _ = tokenize_hl7_message(_MINIMAL_MSH, level=2)
        self.msh = self.segments[0]

    def test_known_index_in_range(self):
        val = get_with_default(self.msh, "msh", 2)  # MSH.3 sending app
        assert val == "CAPSULE"

    def test_index_beyond_segment_returns_missing(self):
        # MSH.21 (index 20) is not in our short 12-field MSH → missing value ''
        val = get_with_default(self.msh, "msh", 20)
        assert val == ""

    def test_unknown_seg_field_raises(self):
        with pytest.raises(ValueError):
            get_with_default(self.msh, "msh", 99)  # no mapping for index 99


# ---------------------------------------------------------------------------
# Helper functions — depth guards
# ---------------------------------------------------------------------------

class TestDepthHelpers:
    def test_component_to_subcomponents_below_level5(self):
        result = _component_to_subcomponents("A&B", level=4)
        assert result == "A&B"  # not split below level 5

    def test_component_to_subcomponents_at_level5(self):
        result = _component_to_subcomponents("A&B", level=5)
        assert result == ["A", "B"]

    def test_repetition_to_components_below_level4(self):
        result = _repetition_to_components("A^B", level=3)
        assert result == "A^B"  # not split below level 4

    def test_repetition_to_components_at_level4(self):
        result = _repetition_to_components("A^B", level=4)
        assert result == ["A", "B"]

    def test_field_to_repetitions_below_level3(self):
        result = _field_to_repetitions("A~B", level=2)
        assert result == "A~B"  # not split below level 3

    def test_field_to_repetitions_at_level3(self):
        result = _field_to_repetitions("A~B", level=3)
        assert result == ["A", "B"]
