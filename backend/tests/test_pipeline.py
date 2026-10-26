"""Unit tests for industrial_pipeline.py (the original numeric pipeline)."""

import json

import numpy as np
import pandas as pd
import pytest

import industrial_pipeline as ip
from industrial_pipeline import (DataAnalyzer, DataCleaningStage, DataIngestionStage, DataVersionManager,
                                 FeatureEngineeringStage, IndustrialDataPipeline, NormalizationStage,
                                 OutlierDetectionStage, PipelineConfig, ValidationStage)


def sensors(n=120, seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=n, freq="s").astype(str),
        "machine_id": rng.choice(["M1", "M2", "M3"], n),
        "temperature": rng.normal(75, 5, n),
        "pressure": rng.normal(30, 3, n),
        "vibration": rng.exponential(2, n),
        "rpm": rng.normal(1500, 100, n),
    })


def cfg(**kw):
    base = dict(enable_versioning=False)
    base.update(kw)
    return PipelineConfig(**base)


# ------------------------------------------------------------ DataAnalyzer
class TestDataAnalyzer:
    def test_detects_column_kinds_and_features(self):
        a = DataAnalyzer.analyze_dataframe(sensors())
        assert {"temperature", "pressure", "vibration", "rpm"} <= set(a["numeric_columns"])
        assert "timestamp" in a["datetime_columns"]
        f = a["detected_features"]
        assert f["has_timestamp"] and f["has_sensor_data"] and f["has_temperature"] and f["is_time_series"]

    def test_missing_values_are_reported_as_fractions(self):
        df = sensors(10)
        df.loc[:4, "pressure"] = np.nan
        assert DataAnalyzer.analyze_dataframe(df)["missing_values"]["pressure"] == pytest.approx(0.5)

    def test_all_nan_numeric_column(self):
        df = sensors(10)
        df["empty"] = np.nan
        a = DataAnalyzer.analyze_dataframe(df)
        assert a["column_stats"]["empty"]["mean"] is None

    def test_empty_dataframe_does_not_crash(self):
        a = DataAnalyzer.analyze_dataframe(pd.DataFrame({"a": [], "b": []}))
        assert a["total_rows"] == 0

    def test_free_text_column_is_not_called_a_date(self):
        df = pd.DataFrame({"review": ["great movie", "bad plot", "ok"] * 5})
        a = DataAnalyzer.analyze_dataframe(df)
        assert a["categorical_columns"] == ["review"] and a["datetime_columns"] == []

    def test_suggestions_follow_the_data(self):
        s = DataAnalyzer.suggest_pipeline_config(DataAnalyzer.analyze_dataframe(sensors()))
        assert s["outlier_detection_enabled"] and s["normalization_enabled"] and s["feature_engineering_enabled"]

    def test_text_only_data_gets_minimal_suggestions(self):
        df = pd.DataFrame({"review": ["great movie", "bad plot"] * 10})
        s = DataAnalyzer.suggest_pipeline_config(DataAnalyzer.analyze_dataframe(df))
        assert not s["outlier_detection_enabled"] and not s["normalization_enabled"]

    def test_high_missing_data_raises_threshold(self):
        df = sensors(20)
        df.loc[:9, ["pressure", "rpm"]] = np.nan
        s = DataAnalyzer.suggest_pipeline_config(DataAnalyzer.analyze_dataframe(df))
        assert s["missing_value_threshold"] == 0.5


# ------------------------------------------------------------ stages
class TestValidation:
    def test_removes_infinite_values(self):
        df = sensors(50)
        df.loc[3, "temperature"] = np.inf
        out = ValidationStage().process(df, cfg())
        assert len(out) == 49 and np.isfinite(out["temperature"]).all()

    def test_quality_score_reflects_removed_rows(self):
        df = sensors(50)
        df.loc[:4, "rpm"] = np.inf
        stage = ValidationStage()
        stage.process(df, cfg())
        assert stage.quality_score == pytest.approx(45 / 50)

    def test_disabled_returns_input_unchanged(self):
        df = sensors(20)
        assert len(ValidationStage().process(df, cfg(validation_enabled=False))) == 20

    def test_keeps_nan_rows(self):
        # NaN is "missing", not "invalid": cleaning decides what to do with it
        df = sensors(30)
        df.loc[2, "pressure"] = np.nan
        assert len(ValidationStage().process(df, cfg())) == 30

    def test_unparseable_dates_become_nat_not_errors(self):
        df = sensors(10)
        df.loc[0, "timestamp"] = "not a date"
        out = ValidationStage().process(df, cfg())
        assert out["timestamp"].isna().sum() == 1


class TestCleaning:
    def test_median_fills_small_gaps(self):
        df = sensors(60)
        df.loc[5, "pressure"] = np.nan
        out = DataCleaningStage().process(df, cfg())
        assert out["pressure"].isna().sum() == 0

    def test_drops_columns_above_threshold(self):
        df = sensors(40)
        df.loc[:29, "rpm"] = np.nan
        out = DataCleaningStage().process(df, cfg(missing_value_threshold=0.3))
        assert "rpm" not in out.columns

    def test_removes_duplicate_rows(self):
        df = pd.concat([sensors(10), sensors(10)], ignore_index=True)
        assert len(DataCleaningStage().process(df, cfg())) == 10

    def test_single_row_frame(self):
        assert len(DataCleaningStage().process(sensors(1), cfg())) == 1


class TestOutliers:
    def test_removes_extreme_value(self):
        df = sensors(200)
        df.loc[10, "temperature"] = 10_000
        out = OutlierDetectionStage().process(df, cfg(outlier_std_threshold=3))
        assert 10_000 not in out["temperature"].values

    def test_constant_column_is_ignored(self):
        df = sensors(50)
        df["const"] = 5.0
        assert "const" in OutlierDetectionStage().process(df, cfg()).columns

    def test_disabled_keeps_everything(self):
        df = sensors(50)
        df.loc[0, "rpm"] = 1e9
        assert len(OutlierDetectionStage().process(df, cfg(outlier_detection_enabled=False))) == 50


class TestNormalization:
    def test_adds_zero_to_one_columns(self):
        out = NormalizationStage().process(sensors(50), cfg())
        col = out["temperature_normalized"]
        assert col.min() == pytest.approx(0) and col.max() == pytest.approx(1)

    def test_constant_column_skipped(self):
        df = sensors(20)
        df["const"] = 3
        assert "const_normalized" not in NormalizationStage().process(df, cfg()).columns

    def test_does_not_normalize_its_own_output_twice(self):
        stage = NormalizationStage()
        once = stage.process(sensors(30), cfg())
        assert not any(c.endswith("_normalized_normalized") for c in once.columns)


class TestFeatureEngineering:
    def test_time_features_from_string_dates(self):
        df = ValidationStage().process(sensors(40), cfg())
        out = FeatureEngineeringStage().process(df, cfg())
        assert {"timestamp_hour", "timestamp_day_of_week", "timestamp_is_weekend"} <= set(out.columns)

    def test_categorical_encoding(self):
        out = FeatureEngineeringStage().process(sensors(40), cfg())
        assert "machine_id_encoded" in out.columns

    def test_rolling_features_skipped_for_tiny_frames(self):
        out = FeatureEngineeringStage().process(sensors(5), cfg())
        assert not any(c.endswith("_rolling_mean") for c in out.columns)

    def test_text_only_frame(self):
        df = pd.DataFrame({"review": ["a b c", "d e f"] * 10})
        FeatureEngineeringStage().process(df, cfg())

    def test_ratio_columns_survive_zero_denominators(self):
        df = sensors(30)
        df["pressure"] = 0.0
        out = FeatureEngineeringStage().process(df, cfg())
        ratio = [c for c in out.columns if c.endswith("_ratio")]
        assert ratio and np.isfinite(out[ratio].to_numpy(dtype=float)).all()


class TestIngestion:
    def test_passes_real_data_through(self):
        df = sensors(10)
        assert DataIngestionStage().process(df, cfg()) is df

    def test_generates_sample_data_only_when_nothing_is_given(self):
        out = DataIngestionStage().process(pd.DataFrame(), cfg(batch_size=25))
        assert len(out) == 25 and "temperature" in out.columns

    def test_uploaded_header_only_file_is_not_replaced_by_fake_data(self):
        # An uploaded file with a header and no rows is "no data", not a request for sample data.
        empty = pd.DataFrame({"temperature": [], "pressure": []})
        out = DataIngestionStage().process(empty, cfg(batch_size=0))
        assert "machine_id" not in out.columns, "synthetic sensor data was silently substituted for the upload"


# ------------------------------------------------------------ version manager
class TestVersioning:
    def test_save_and_load_round_trip(self, tmp_path):
        vm = DataVersionManager(str(tmp_path))
        df = sensors(10)
        vid = vm.save_version(df, {"note": "x"})
        loaded, meta = vm.load_version(vid)
        assert len(loaded) == 10 and meta["note"] == "x" and vid in [v["version_id"] for v in vm.list_versions()]

    def test_manifest_survives_a_restart(self, tmp_path):
        DataVersionManager(str(tmp_path)).save_version(sensors(5), {})
        assert len(DataVersionManager(str(tmp_path)).list_versions()) == 1

    def test_different_data_gets_different_hash(self, tmp_path):
        vm = DataVersionManager(str(tmp_path))
        a = vm.save_version(pd.DataFrame({"t": ["alpha", "beta"]}), {})
        b = vm.save_version(pd.DataFrame({"t": ["gamma", "delta"]}), {})
        meta = {v["version_id"]: v["hash"] for v in vm.list_versions()}
        assert meta[a] != meta[b]

    def test_same_data_gets_same_hash(self, tmp_path):
        vm = DataVersionManager(str(tmp_path))
        a = vm.save_version(pd.DataFrame({"t": ["alpha", "beta"]}), {})
        b = vm.save_version(pd.DataFrame({"t": ["alpha", "beta"]}), {})
        meta = {v["version_id"]: v["hash"] for v in vm.list_versions()}
        assert meta[a] == meta[b]

    def test_loading_unknown_version(self, tmp_path):
        with pytest.raises(ValueError):
            DataVersionManager(str(tmp_path)).load_version("nope")

    def test_two_saves_in_the_same_second_do_not_overwrite_each_other(self, tmp_path):
        vm = DataVersionManager(str(tmp_path))
        ids = {vm.save_version(sensors(5, seed=s), {}) for s in range(3)}
        assert len(ids) == 3


# ------------------------------------------------------------ whole pipeline
class TestPipeline:
    def test_full_run(self, tmp_path):
        p = IndustrialDataPipeline(cfg(enable_versioning=True, version_storage_path=str(tmp_path)))
        out, m = p.run(sensors(200))
        assert m.total_records == 200 and m.valid_records == len(out) and 0 <= m.data_quality_score <= 1
        assert set(m.stage_times) == {"Data Ingestion", "Validation", "Data Cleaning", "Outlier Detection",
                                      "Normalization", "Feature Engineering"}
        assert len(p.version_manager.list_versions()) == 1

    def test_metrics_are_json_serialisable(self):
        _, m = IndustrialDataPipeline(cfg()).run(sensors(60))
        json.dumps({"stage_times": m.stage_times, "q": m.data_quality_score, "t": m.throughput})

    def test_invalid_record_count_is_consistent_with_output_size(self):
        out, m = IndustrialDataPipeline(cfg()).run(sensors(300))
        assert m.valid_records + m.invalid_records == m.total_records

    def test_quality_score_matches_removed_records(self):
        # data_quality_score is shown in the UI as "Data Quality": it should reflect what was thrown away
        df = sensors(300)
        df.loc[:29, "temperature"] = 99999          # extreme values the outlier stage will remove
        out, m = IndustrialDataPipeline(cfg(outlier_std_threshold=3)).run(df)
        assert m.invalid_records > 0
        assert m.data_quality_score < 1.0, "records were dropped but quality score still says 100%"

    def test_text_only_dataframe(self):
        df = pd.DataFrame({"review": [f"movie review number {i}" for i in range(30)]})
        out, m = IndustrialDataPipeline(cfg()).run(df)
        assert len(out) == 30

    def test_single_row(self):
        out, _ = IndustrialDataPipeline(cfg()).run(sensors(1))
        assert len(out) == 1

    def test_no_input_generates_sample_data(self):
        out, m = IndustrialDataPipeline(cfg(batch_size=50)).run()
        assert len(out) > 0 and m.total_records == 50

    def test_all_rows_invalid_does_not_crash(self):
        df = sensors(20)
        df["temperature"] = np.inf
        out, m = IndustrialDataPipeline(cfg()).run(df)
        assert len(out) == 0 and m.data_quality_score == 0

    def test_a_failing_stage_is_reported(self, monkeypatch):
        p = IndustrialDataPipeline(cfg())
        monkeypatch.setattr(p.stages[2], "process", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(RuntimeError, match="boom"):
            p.run(sensors(20))

    def test_run_twice_with_one_instance_does_not_double_count(self):
        p = IndustrialDataPipeline(cfg())
        p.run(sensors(40))
        _, m = p.run(sensors(40))
        assert m.error_count == 0 and m.total_records == 40

    def test_does_not_modify_the_callers_dataframe(self):
        df = sensors(60)
        before = df.copy()
        IndustrialDataPipeline(cfg()).run(df)
        pd.testing.assert_frame_equal(df, before)
