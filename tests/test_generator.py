"""Generator contract tests — determinism, quality gate, volumes, types, chaos."""
from __future__ import annotations
import hashlib
from pathlib import Path
import pyarrow.parquet as pq
from generator.config import GeneratorConfig
from generator.simulate import generate_all
from generator.validate import validate
from generator.writers import write_all

# Small config for fast tests
CFG = GeneratorConfig(seed=7, n_robots=6, n_days=5, readings_per_robot_per_day=24, messy_output=False)

def _md5(p):
    return hashlib.md5(p.read_bytes()).hexdigest()

def test_quality_gate_passes():
    report = validate(generate_all(CFG))
    assert report["status"] == "PASS"
    assert all(s == "PASS" for _, s in report["checks"])

def test_row_counts_match_config():
    data = generate_all(CFG)
    assert len(data["robots"]) == CFG.n_robots
    assert len(data["robot_models"]) == CFG.n_models
    assert len(data["zones"]) == CFG.n_zones
    assert len(data["sensor_readings"]) == CFG.n_robots * CFG.steps_per_robot

def test_determinism_byte_identical(tmp_path):
    write_all(generate_all(CFG), str(tmp_path / "a"), CFG)
    write_all(generate_all(CFG), str(tmp_path / "b"), CFG)
    for f in ["sensor_readings.parquet", "robots.csv"]:
        assert _md5(tmp_path / "a" / f) == _md5(tmp_path / "b" / f), f"{f} not deterministic"

def test_nested_parquet_types(tmp_path):
    write_all(generate_all(CFG), str(tmp_path), CFG)
    schema = pq.read_schema(tmp_path / "sensor_readings.parquet")
    assert str(schema.field("battery").type).startswith("struct")
    assert str(schema.field("navigation").type).startswith("struct")
    assert str(schema.field("connectivity").type).startswith("struct")
    assert str(schema.field("error_codes").type).startswith("list")

def test_different_seed_differs():
    a = generate_all(GeneratorConfig(seed=1, n_robots=4, n_days=3, messy_output=False))
    b = generate_all(GeneratorConfig(seed=2, n_robots=4, n_days=3, messy_output=False))
    assert len(a["sensor_readings"]) == len(b["sensor_readings"])
    assert a["sensor_readings"][0].battery.soc_pct != b["sensor_readings"][0].battery.soc_pct

def test_chaos_injects_known_counts():
    from generator.chaos import inject_chaos
    chaos_cfg = GeneratorConfig(seed=7, n_robots=6, n_days=5, readings_per_robot_per_day=24,
                                 messy_output=False, chaos=True, chaos_dup_rate=0.02)
    data = generate_all(chaos_cfg)
    clean_count = len(data["sensor_readings"])
    manifest = inject_chaos(data, chaos_cfg)
    assert manifest["chaos"] is True
    n_dups = manifest["injected"]["duplicates_injected"]
    assert n_dups > 0
    assert len(data["sensor_readings"]) == clean_count + n_dups

def test_messy_output_adds_columns(tmp_path):
    """Messy CSV may have junk columns that clean Parquet doesn't."""
    messy_cfg = GeneratorConfig(seed=7, n_robots=4, n_days=3, readings_per_robot_per_day=24,
                                 messy_output=True, mess_rate=0.25)
    data = generate_all(messy_cfg)
    write_all(data, str(tmp_path), messy_cfg)
    import pandas as pd
    df = pd.read_csv(tmp_path / "sensor_readings_raw.csv")
    # messy CSV should have 35+ columns (base) and possibly junk columns
    assert len(df.columns) >= 35
    assert len(df) == messy_cfg.n_robots * messy_cfg.steps_per_robot

def test_wide_schema_column_count():
    """Each sensor reading flattens to 35+ columns."""
    data = generate_all(CFG)
    from generator.writers import _flatten_reading
    row = _flatten_reading(data["sensor_readings"][0])
    assert len(row) >= 35, f"Only {len(row)} columns, expected 35+"
