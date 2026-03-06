"""Integration tests: tokenize → HierarchicalMessage → hl7_data_factory pipeline."""
import json
import pytest
from hl7lite.hl7_tokenizer import tokenize_hl7_message
from hl7lite.hl7_ds import (
    HierarchicalMessage,
    hl7_data_factory,
    HL7ORUData,
    HL7ADTData,
    HL7WaveformData,
)
from hl7lite.hl7_io import convert_msg_to_json


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------

class TestTokenizeORU:
    def test_all_expected_segments_present(self, oru_waveform_msg):
        _, seg_names = tokenize_hl7_message(oru_waveform_msg)
        assert {"MSH", "PID", "PV1", "OBR", "OBX"}.issubset(seg_names)

    def test_segment_count(self, oru_waveform_msg):
        segments, _ = tokenize_hl7_message(oru_waveform_msg)
        # MSH + PID + PV1 + OBR + 2×OBX = 6
        assert len(segments) == 6

    def test_msh_message_type_parsed(self, oru_waveform_msg):
        segments, _ = tokenize_hl7_message(oru_waveform_msg)
        msh = segments[0]
        # MSH.9 with component parsing → ['ORU', 'R01']
        assert isinstance(msh[8], list)
        assert msh[8][0] == "ORU"

    def test_obr4_waveform_code(self, oru_waveform_msg):
        segments, _ = tokenize_hl7_message(oru_waveform_msg)
        obr = next(s for s in segments if s[0] == "OBR")
        # "69121^MDC_OBS_WAVE_CTS^MDC" → ['69121', 'MDC_OBS_WAVE_CTS', 'MDC']
        assert obr[4][0] == "69121"
        assert obr[4][1] == "MDC_OBS_WAVE_CTS"


class TestTokenizeADT:
    def test_all_expected_segments_present(self, adt_msg):
        _, seg_names = tokenize_hl7_message(adt_msg)
        assert {"MSH", "EVN", "PID", "PV1"}.issubset(seg_names)

    def test_segment_count(self, adt_msg):
        segments, _ = tokenize_hl7_message(adt_msg)
        # MSH + EVN + PID + PV1 = 4
        assert len(segments) == 4

    def test_msh_message_type_parsed(self, adt_msg):
        segments, _ = tokenize_hl7_message(adt_msg)
        msh = segments[0]
        assert msh[8][0] == "ADT"


# ---------------------------------------------------------------------------
# HierarchicalMessage construction
# ---------------------------------------------------------------------------

class TestHierarchicalMessage:
    def test_oru_construction_succeeds(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert msg is not None

    def test_adt_construction_succeeds(self, adt_msg):
        parsed, seg_names = tokenize_hl7_message(adt_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert msg is not None

    def test_msh_attribute_set(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert msg.msh is not None
        assert msg.msh[0] == "MSH"

    def test_pid_attribute_set(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert msg.pid[0] == "PID"

    def test_pv1_attribute_set(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert msg.pv1[0] == "PV1"

    def test_obrs_list_populated(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert len(msg.obrs) == 1

    def test_obx_nested_under_obr(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert len(msg.obrs[0]["obx"]) == 2

    def test_evn_stored_for_adt(self, adt_msg):
        parsed, seg_names = tokenize_hl7_message(adt_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        assert msg.evn is not None
        assert msg.evn[0] == "EVN"

    def test_missing_pid_raises(self):
        bad_msg = (
            "MSH|^~\\&|CAPSULE|EUHM|R|H|20230615120000-0400||ORU^R01|C|P|2.3\n"
            "PV1|1|I|EUH-4TN-T434\n"
        )
        parsed, seg_names = tokenize_hl7_message(bad_msg)
        with pytest.raises(ValueError, match="PID"):
            HierarchicalMessage(parsed, seg_names)

    def test_missing_pv1_raises(self):
        bad_msg = (
            "MSH|^~\\&|CAPSULE|EUHM|R|H|20230615120000-0400||ORU^R01|C|P|2.3\n"
            "PID|1|PAT001\n"
        )
        parsed, seg_names = tokenize_hl7_message(bad_msg)
        with pytest.raises(ValueError, match="PV1"):
            HierarchicalMessage(parsed, seg_names)


# ---------------------------------------------------------------------------
# hl7_data_factory
# ---------------------------------------------------------------------------

class TestHL7DataFactory:
    def test_oru_waveform_returns_waveform_type(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        assert isinstance(data, HL7WaveformData)

    def test_adt_returns_adt_type(self, adt_msg):
        parsed, seg_names = tokenize_hl7_message(adt_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        assert isinstance(data, HL7ADTData)

    def test_waveform_is_subtype_of_oru(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        assert isinstance(data, HL7ORUData)

    def test_unknown_obr_type_raises(self):
        # OBR.4 with code '00000' is not registered; factory raises before bed extraction
        msg_str = "\n".join([
            "MSH|^~\\&|CAPSULE|EUHM|R|H|20230615120000-0400||ORU^R01|C|P|2.3|||NE|NE",
            "PID|1|PAT001|PAT001||DOE^JOHN||19800101|M",
            "PV1|1|I|EUH-4TN-T434",
            "OBR|1||OBS|00000^UNKNOWN^SYS|||20230615120000-0400|20230615120100-0400",
            "OBX|1|NM|CODE||100|mV",
        ]) + "\n"
        parsed, seg_names = tokenize_hl7_message(msg_str)
        msg = HierarchicalMessage(parsed, seg_names)
        with pytest.raises(ValueError):
            hl7_data_factory(msg)

    def test_control_id_extracted(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        assert data.control_id == "CTRL001"

    def test_to_row_dicts_returns_list(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        rows = data.to_row_dicts(time_as_epoch=False)
        assert isinstance(rows, list)

    def test_to_row_dicts_has_required_keys(self, oru_waveform_msg):
        parsed, seg_names = tokenize_hl7_message(oru_waveform_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        rows = data.to_row_dicts(time_as_epoch=False)
        assert len(rows) > 0
        for key in ("hospital", "bed_unit", "bed_id", "channel"):
            assert key in rows[0], f"Missing key '{key}' in row dict"

    def test_adt_to_row_dicts(self, adt_msg):
        parsed, seg_names = tokenize_hl7_message(adt_msg)
        msg = HierarchicalMessage(parsed, seg_names)
        data = hl7_data_factory(msg)
        rows = data.to_row_dicts(time_as_epoch=False)
        assert isinstance(rows, list)
        assert len(rows) > 0


# ---------------------------------------------------------------------------
# convert_msg_to_json
# ---------------------------------------------------------------------------

class TestConvertMsgToJson:
    def test_returns_string(self, oru_waveform_msg):
        result = convert_msg_to_json(oru_waveform_msg)
        assert isinstance(result, str)

    def test_result_is_valid_json(self, oru_waveform_msg):
        result = convert_msg_to_json(oru_waveform_msg)
        parsed = json.loads(result)
        assert isinstance(parsed, (dict, list))

    def test_adt_returns_valid_json(self, adt_msg):
        result = convert_msg_to_json(adt_msg)
        parsed = json.loads(result)
        assert isinstance(parsed, (dict, list))
