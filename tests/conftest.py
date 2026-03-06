"""Shared pytest fixtures for all test domains."""
import pytest

# Minimal ORU waveform message.
# OBR.4 = "69121^MDC_OBS_WAVE_CTS^MDC" → dispatches to HL7WaveformData.
# PV1.3 = "EUH-4TN-T434": dash-separated string handled by _extract_bed_id_str
#   (extract_bed_id supports list lengths 1,2,4,5,9,11 or a plain string; 3-part
#    caret-separated is NOT a supported form, so we use the string variant).
ORU_WAVEFORM_MSG = "\n".join([
    "MSH|^~\\&|CAPSULE|EUHM|RECEIVER|HOSPITAL|20230615120000-0400||ORU^R01|CTRL001|P|2.3|||NE|NE",
    "PID|1|PAT001|PAT001||DOE^JOHN||19800101|M",
    "PV1|1|I|EUH-4TN-T434",
    "OBR|1||OBS001|69121^MDC_OBS_WAVE_CTS^MDC|||20230615120000-0400|20230615120100-0400|||EUH-4TN-T434||||||||||||EUHM",
    "OBX|1|NM|MDC_ECG_LEAD_I^MDC_ECG_LEAD_I^Local||100|mV||N|||F|||20230615120000-0400",
    "OBX|2|NM|MDC_ECG_LEAD_II^MDC_ECG_LEAD_II^Local||90|mV||N|||F|||20230615120001-0400",
]) + "\n"

# Minimal ADT admit message.
ADT_MSG = "\n".join([
    "MSH|^~\\&|ADT|EUHM|RECEIVER|HOSPITAL|20230615090000-0400||ADT^A01|ADT001|P|2.3",
    "EVN|A01|20230615090000-0400",
    "PID|1|PAT001|PAT001||DOE^JOHN||19800101|M",
    "PV1|1|I|EUH-4TN-T434",
]) + "\n"


@pytest.fixture
def oru_waveform_msg():
    return ORU_WAVEFORM_MSG


@pytest.fixture
def adt_msg():
    return ADT_MSG


@pytest.fixture
def oru_file(tmp_path):
    f = tmp_path / "sample.hl7"
    f.write_text(ORU_WAVEFORM_MSG)
    return str(f)
