from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


def load_stream_emitter_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "containers"
        / "workflows"
        / "sample-stream-events-01"
        / "stream_emitter"
        / "app.py"
    )
    spec = importlib.util.spec_from_file_location("sample_stream_events_01_stream_emitter", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


stream_emitter = load_stream_emitter_module()


class StreamEmitterTests(unittest.TestCase):
    def test_derive_seed_is_deterministic(self):
        first = stream_emitter.derive_seed(
            workflow_name="sample-stream-events-01",
            preset_id="iot_sensor_benchmark",
            batch_started_at="2026-03-15T12:00:00Z",
            emission_index=4,
        )
        second = stream_emitter.derive_seed(
            workflow_name="sample-stream-events-01",
            preset_id="iot_sensor_benchmark",
            batch_started_at="2026-03-15T12:00:00Z",
            emission_index=4,
        )
        third = stream_emitter.derive_seed(
            workflow_name="sample-stream-events-01",
            preset_id="iot_sensor_benchmark",
            batch_started_at="2026-03-15T12:00:00Z",
            emission_index=5,
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, third)

    def test_build_stream_event_flattens_sample_row(self):
        event = stream_emitter.build_stream_event(
            workflow_name="sample-stream-events-01",
            preset_id="iot_sensor_benchmark",
            sample_payload={
                "schema_version": "1.0",
                "scenario_name": "iot_sensor_benchmark",
                "row": {
                    "device_id": "device-001",
                    "site_id": "site-1",
                    "device_type": "combo_sensor",
                    "temperature_c": 21.5,
                    "pressure_kpa": 101.2,
                    "device_status": "normal",
                },
            },
            source_seed=1234,
            batch_started_at="2026-03-15T12:00:00Z",
            emitted_at="2026-03-15T12:00:05Z",
            emission_index=2,
        )

        self.assertEqual(event["workflow_name"], "sample-stream-events-01")
        self.assertEqual(event["source_preset_id"], "iot_sensor_benchmark")
        self.assertEqual(event["device_id"], "device-001")
        self.assertEqual(event["temperature_c"], 21.5)
        self.assertEqual(event["emission_index"], 2)
        self.assertIn("emitter_event_id", event)

    def test_build_kinesis_record_is_newline_delimited_json(self):
        record = stream_emitter.build_kinesis_record(
            {
                "emitter_event_id": "evt-1",
                "device_id": "device-001",
                "temperature_c": 21.5,
            }
        )

        self.assertEqual(record["PartitionKey"], "device-001")
        self.assertTrue(record["Data"].endswith(b"\n"))


if __name__ == "__main__":
    unittest.main()
