"""跨模块数据流业务行为测试：covering version tracking, failure recovery,
batch deduplication, watermark management, schema version evolution, and
cross-service data consumption chains (source_registry → ingest_batch →
raw_records → field_parser → parsed_entity → dedup_engine →
aggregation_view → data_export)."""
from __future__ import annotations

import json
import pytest

from app.store import store


class TestSourceRegistry:
    """数据源登记与验证测试。"""

    def test_register_new_source(self, pipeline):
        result = pipeline.register_source("app-error-log", "http", "beijing")
        assert result["ok"] is True
        assert result["entry"]["来源标识"] == "app-error-log"
        assert result["entry"]["status"] == "待验证"
        assert result["entry"]["source_version"] == 1

    def test_register_duplicate_source(self, pipeline):
        pipeline.register_source("duplicate-src", "http", "shanghai")
        result = pipeline.register_source("duplicate-src", "grpc", "beijing")
        assert result["ok"] is False
        assert "已登记" in result["message"]

    def test_validate_existing_source(self, pipeline):
        pipeline.register_source("to-validate", "http", "default")
        result = pipeline.validate_source("to-validate")
        assert result["ok"] is True
        assert result["entry"]["status"] == "已验证"
        assert result["entry"]["source_version"] == 2

    def test_validate_nonexistent_source(self, pipeline):
        result = pipeline.validate_source("ghost-source")
        assert result["ok"] is False

    def test_validate_increments_version(self, pipeline):
        pipeline.register_source("version-src", "http", "default")
        pipeline.validate_source("version-src")
        entry = pipeline.validate_source("version-src")["entry"]
        assert entry["source_version"] >= 2


class TestIngestBatch:
    """采集批次启动测试。"""

    def test_start_batch_unregistered_source(self, pipeline):
        result = pipeline.start_ingest_batch("missing-src", "2026Q1", [])
        assert result["ok"] is False

    def test_start_batch_unvalidated_source(self, pipeline):
        pipeline.register_source("unvalidated-src", "http", "default")
        result = pipeline.start_ingest_batch("unvalidated-src", "2026Q1", [])
        assert result["ok"] is False
        assert "未通过验证" in result["message"]

    def test_start_batch_success(self, pipeline, sample_raw_logs):
        pipeline.register_source("batch-src", "http", "default")
        pipeline.validate_source("batch-src")
        result = pipeline.start_ingest_batch("batch-src", "2026-01-15", sample_raw_logs)
        assert result["ok"] is True
        assert result["record_count"] == 5
        assert result["batch"]["status"] == "采集中"

    def test_batch_creates_raw_records(self, pipeline, sample_raw_logs):
        pipeline.register_source("record-src", "http", "default")
        pipeline.validate_source("record-src")
        result = pipeline.start_ingest_batch("record-src", "2026-01-15", sample_raw_logs)
        batch_no = result["batch"]["批次编号"]
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert len(batch_raw) == 5
        for r in batch_raw:
            assert r["status"] == "待解析"

    def test_no_duplicate_active_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("single-src", "http", "default")
        pipeline.validate_source("single-src")
        pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[:2])
        result = pipeline.start_ingest_batch("single-src", "2026Q1", sample_raw_logs[2:])
        assert result["ok"] is False
        assert "进行中的批次" in result["message"]

    def test_batch_records_source_version(self, pipeline, sample_raw_logs):
        pipeline.register_source("ver-track-src", "http", "default")
        pipeline.validate_source("ver-track-src")
        result = pipeline.start_ingest_batch("ver-track-src", "2026Q1", sample_raw_logs[:1])
        assert result["batch"]["source_version"] == 2


class TestFieldParser:
    """字段解析测试。"""

    def test_parse_batch_no_records(self, pipeline):
        result = pipeline.parse_batch("NONEXIST-BATCH")
        assert result["ok"] is False

    def test_parse_batch_no_parser_rule_uses_default(self, pipeline, sample_raw_logs):
        pipeline.register_source("parse-src", "http", "default")
        pipeline.validate_source("parse-src")
        batch = pipeline.start_ingest_batch("parse-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]

        result = pipeline.parse_batch(batch_no)
        assert result["ok"] is True
        assert result["success"] > 0

    def test_parse_batch_sets_raw_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("status-src", "http", "default")
        pipeline.validate_source("status-src")
        batch = pipeline.start_ingest_batch("status-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        raw = store.rows("raw_records")
        batch_raw = [r for r in raw if r.get("批次编号") == batch_no]
        assert all(r["status"] == "已解析" for r in batch_raw)

    def test_parse_creates_parsed_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("ent-src", "http", "default")
        pipeline.validate_source("ent-src")
        batch = pipeline.start_ingest_batch("ent-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        pipeline.parse_batch(batch_no)
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 2
        for p in parsed:
            assert p["status"] == "待校验"

    def test_parse_journals_created(self, pipeline, sample_raw_logs):
        pipeline.register_source("journal-src", "http", "default")
        pipeline.validate_source("journal-src")
        batch = pipeline.start_ingest_batch("journal-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]

        before = len(store.rows("parse_journal"))
        pipeline.parse_batch(batch_no)
        after = len(store.rows("parse_journal"))
        assert after > before


class TestValidation:
    """质量校验测试。"""

    def test_validate_entities(self, pipeline, sample_raw_logs):
        pipeline.register_source("valid-src", "http", "default")
        pipeline.validate_source("valid-src")
        batch = pipeline.start_ingest_batch("valid-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.validate_entities(batch_no)
        assert result["ok"] is True
        assert result["passed"] == 2

    def test_validate_sets_status(self, pipeline, sample_raw_logs):
        pipeline.register_source("vstat-src", "http", "default")
        pipeline.validate_source("vstat-src")
        batch = pipeline.start_ingest_batch("vstat-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert all(p["status"] == "校验通过" for p in parsed)


class TestDeduplication:
    """去重引擎测试。"""

    def test_dedup_no_duplicates(self, pipeline, sample_raw_logs):
        pipeline.register_source("nodup-src", "http", "default")
        pipeline.validate_source("nodup-src")
        batch = pipeline.start_ingest_batch("nodup-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["time", "level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] == 0

    def test_dedup_with_duplicates(self, pipeline):
        pipeline.register_source("dup-src", "http", "default")
        pipeline.validate_source("dup-src")
        # 提交相同日志
        same_logs = [
            {"body": "t1|INFO|msg1"},
            {"body": "t1|INFO|msg1"},  # duplicate
            {"body": "t1|WARN|msg2"},
        ]
        batch = pipeline.start_ingest_batch("dup-src", "2026Q1", same_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_first")
        assert result["ok"] is True
        assert result["duplicates_found"] > 0

    def test_dedup_keep_last(self, pipeline):
        pipeline.register_source("kl-src", "http", "default")
        pipeline.validate_source("kl-src")
        logs = [
            {"body": "t1|INFO|first"},
            {"body": "t1|INFO|second"},
        ]
        batch = pipeline.start_ingest_batch("kl-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.run_dedup(["level"], "batch", "keep_last")
        assert result["ok"] is True


class TestAggregation:
    """聚合视图测试。"""

    def test_refresh_aggregation(self, pipeline, sample_raw_logs):
        pipeline.register_source("agg-src", "http", "default")
        pipeline.validate_source("agg-src")
        batch = pipeline.start_ingest_batch("agg-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("test-view", "level", "time", "count")
        assert result["ok"] is True
        assert "view" in result
        assert result["dimensions"] > 0

    def test_aggregation_dimensions(self, pipeline, sample_raw_logs):
        pipeline.register_source("dim-src", "http", "default")
        pipeline.validate_source("dim-src")
        batch = pipeline.start_ingest_batch("dim-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.refresh_aggregation("dim-view", "level", "time", "count")
        results = result["view"]["results"]
        assert "INFO" in results
        assert results["INFO"]["count"] == 2  # two INFO logs


class TestDataExport:
    """数据导出与血缘测试。"""

    def test_build_export(self, pipeline, sample_raw_logs):
        pipeline.register_source("export-src", "http", "default")
        pipeline.validate_source("export-src")
        batch = pipeline.start_ingest_batch("export-src", "2026Q1", sample_raw_logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        result = pipeline.build_export(f"view-{batch_no}", "csv")
        assert result["ok"] is True
        assert result["export"]["status"] == "已完成"

    def test_export_creates_lineage(self, pipeline, sample_raw_logs):
        pipeline.register_source("lin-src", "http", "default")
        pipeline.validate_source("lin-src")
        batch = pipeline.start_ingest_batch("lin-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)
        pipeline.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")

        lineage_before = len(store.rows("data_lineage"))
        pipeline.build_export(f"view-{batch_no}", "csv")
        lineage_after = len(store.rows("data_lineage"))
        assert lineage_after > lineage_before
        last = store.rows("data_lineage")[-1]
        assert last["export_format"] == "csv"


class TestWatermarkManagement:
    """水位管理测试。"""

    def test_advance_watermark_new_source(self, pipeline):
        result = pipeline.advance_watermark("wm-src-1", "INGE-0001", "2026-01-01T00:00:00")
        assert result["ok"] is True
        assert result["watermark"]["数据源"] == "wm-src-1"

    def test_advance_watermark_progress(self, pipeline):
        pipeline.advance_watermark("wm-src-2", "INGE-0001", "2026-01-01T00:00:00")
        result = pipeline.advance_watermark("wm-src-2", "INGE-0002", "2026-01-02T00:00:00")
        assert result["ok"] is True

    def test_watermark_rollback_prevention(self, pipeline):
        pipeline.advance_watermark("wm-src-3", "INGE-0001", "2026-01-05T00:00:00")
        result = pipeline.advance_watermark("wm-src-3", "INGE-0002", "2026-01-01T00:00:00")
        assert result["ok"] is False
        assert "倒退" in result["message"]
        wm = result["message"]
        assert "阻止" in wm

    def test_watermark_rollback_flags_abnormal(self, pipeline):
        pipeline.advance_watermark("wm-src-4", "INGE-0001", "2026-02-01T00:00:00")
        pipeline.advance_watermark("wm-src-4", "INGE-0002", "2026-01-01T00:00:00")
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-src-4"][0]
        assert wm["status"] == "异常回退"
        assert wm["abnormal"] is True


class TestSchemaEvolution:
    """Schema 版本演进测试。"""

    def test_evolve_schema_new(self, pipeline):
        result = pipeline.evolve_schema("v2.0", "app-logs", ["time", "level", "message", "host"], True)
        assert result["ok"] is True
        assert result["schema"]["版本号"] == "v2.0"
        assert result["schema"]["status"] == "已发布"

    def test_evolve_schema_duplicate(self, pipeline):
        pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        result = pipeline.evolve_schema("v3.0", "app-logs", ["time", "msg"], True)
        assert result["ok"] is False
        assert "已存在" in result["message"]

    def test_evolve_schema_non_backward_compatible(self, pipeline):
        result = pipeline.evolve_schema("v4.0", "db-logs", ["timestamp", "query", "duration"], False)
        assert result["ok"] is True
        assert result["schema"]["向后兼容标记"] == "否"


class TestReplayEngine:
    """回溯重放引擎测试。"""

    def test_replay_batch(self, pipeline, sample_raw_logs):
        pipeline.register_source("replay-src", "http", "default")
        pipeline.validate_source("replay-src")
        batch = pipeline.start_ingest_batch("replay-src", "2026Q1", sample_raw_logs[:2])
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        pipeline.validate_entities(batch_no)

        result = pipeline.replay_batch(batch_no, "new-rule-v2")
        assert result["ok"] is True
        assert result["replayed_count"] > 0
        assert result["replay"]["status"] == "已完成"


class TestFailureRecovery:
    """失败恢复测试。"""

    def test_recover_batch(self, pipeline):
        pipeline.register_source("recv-src", "http", "default")
        pipeline.validate_source("recv-src")
        logs = [
            {"body": "bad|format"},  # 只有单字段，解析会失败
            {"body": "t1|INFO|msg"},
        ]
        batch = pipeline.start_ingest_batch("recv-src", "2026Q1", logs)
        batch_no = batch["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)

        result = pipeline.recover_batch(batch_no)
        assert result["ok"] is True
        assert isinstance(result["recovered"], int)


class TestFullPipeline:
    """全链路流水线测试。"""

    def test_run_full_pipeline(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "full-pipeline-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        assert result["ok"] is True
        steps = result["steps"]
        assert "source_registry" in steps
        assert "ingest_batch" in steps
        assert "field_parser" in steps
        assert "validate" in steps
        assert "dedup" in steps
        assert "aggregation" in steps
        assert "export" in steps
        assert "watermark" in steps

    def test_full_pipeline_data_lineage(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "lineage-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        lineages = store.rows("data_lineage")
        assert len(lineages) > 0

    def test_full_pipeline_parsed_entities_refer_to_batch(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "ref-check-src",
            "2026-01-15",
            sample_raw_logs[:3],
        )
        batch_no = result["batch_no"]
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 3

    def test_full_pipeline_watermark_advanced(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline(
            "wm-check-src",
            "2026-01-15",
            sample_raw_logs[:2],
        )
        assert result["ok"] is True
        wms = store.rows("watermark_mgr")
        wm = [w for w in wms if w["数据源"] == "wm-check-src"]
        assert len(wm) == 1
        assert wm[0]["status"] == "正常"


class TestCrossModuleSnapshot:
    """跨模块数据快照测试。"""

    def test_snapshot_contains_all_modules(self, pipeline, sample_raw_logs):
        pipeline.run_full_pipeline("snap-src", "2026Q1", sample_raw_logs[:3])

        from app.services.base import BaseModuleService
        from app.services import SERVICES
        snapshots = {key: svc.domain_snapshot() for key, svc in SERVICES.items()}

        assert "source_registry" in snapshots
        assert "ingest_batch" in snapshots
        assert "raw_records" in snapshots
        assert "parsed_entity" in snapshots
        assert "dedup_engine" in snapshots
        assert "aggregation_view" in snapshots
        assert "data_export" in snapshots
        assert "data_lineage" in snapshots
        assert "watermark_mgr" in snapshots

    def test_snapshot_data_consistency(self, pipeline, sample_raw_logs):
        result = pipeline.run_full_pipeline("consistency-src", "2026Q1", sample_raw_logs[:3])
        batch_no = result["batch_no"]

        from app.services import SERVICES
        parsed_snap = SERVICES["parsed_entity"].domain_snapshot()
        assert parsed_snap["count"] >= 3

        raw_snap = SERVICES["raw_records"].domain_snapshot()
        assert raw_snap["count"] > 0


class TestSourceRevalidation:
    """数据源复验与迟到日志补采测试：核对实际来源验证版本、
    已确认批次不回退、相同尾包只记一次、接收结果回写复验清单。"""

    def _receipts_of(self, source_id):
        return [r for r in store.rows("verification_receipt")
                if r.get("来源标识") == source_id]

    def _batch_of(self, batch_no):
        return next(b for b in store.rows("ingest_batch") if b.get("批次编号") == batch_no)

    def test_revalidate_with_changed_params_writes_receipt(self, pipeline):
        pipeline.register_source("rev-src", "http", "default")
        pipeline.validate_source("rev-src", {"endpoint": "http://old:9001/logs"})
        result = pipeline.validate_source("rev-src", {"endpoint": "http://new:9002/logs"})
        assert result["ok"] is True
        assert result["params_changed"] is True
        assert result["entry"]["source_version"] == 3
        assert result["entry"]["接入地址"] == "http://new:9002/logs"

        receipts = self._receipts_of("rev-src")
        assert len(receipts) == 2
        current = [r for r in receipts if r["status"] == "当前版本"]
        history = [r for r in receipts if r["status"] == "历史版本"]
        assert len(current) == 1 and current[0]["source_version"] == 3
        assert len(history) == 1 and history[0]["source_version"] == 2
        assert current[0]["接入参数摘要"] != history[0]["接入参数摘要"]

    def test_revalidate_without_param_change(self, pipeline):
        pipeline.register_source("stable-src", "http", "default")
        pipeline.validate_source("stable-src", {"endpoint": "http://a:1/logs"})
        result = pipeline.validate_source("stable-src", {"endpoint": "http://a:1/logs"})
        assert result["ok"] is True
        assert result["params_changed"] is False
        assert result["entry"]["source_version"] == 3

    def test_validate_keeps_backward_compatible_receipt(self, pipeline):
        """兼容已有验证路径：不带参数的验证同样写回执。"""
        pipeline.register_source("plain-src", "http", "default")
        result = pipeline.validate_source("plain-src")
        assert result["ok"] is True
        receipts = self._receipts_of("plain-src")
        assert len(receipts) == 1
        assert receipts[0]["source_version"] == 2
        assert receipts[0]["status"] == "当前版本"

    def test_receive_rejects_unmatched_version(self, pipeline):
        """复验与补采并发时，只接受来源和验证版本对得上的记录。"""
        pipeline.register_source("mis-src", "http", "default")
        pipeline.validate_source("mis-src")
        # 从未验证过的版本（登记时是 v1，验证后当前为 v2）
        result = pipeline.receive_buffered_logs("mis-src", 1, "2026-01-15", [{"body": "a|b|c"}])
        assert result["ok"] is False
        assert "未通过验证" in result["message"]
        # 超出当前版本的将来版本
        result = pipeline.receive_buffered_logs("mis-src", 99, "2026-01-15", [{"body": "a|b|c"}])
        assert result["ok"] is False
        assert "未通过验证" in result["message"]

    def test_receive_unregistered_source(self, pipeline):
        result = pipeline.receive_buffered_logs("ghost-src", 1, "w", [{"body": "x|y"}])
        assert result["ok"] is False
        assert "未登记" in result["message"]

    def test_late_tail_and_new_address_batches(self, pipeline):
        """主场景：先验证旧地址并完成一批，再改接入参数后补交旧地址的
        尾包和新地址的一批日志，检查两次接收状态和各批的来源版本。"""
        # 1) 验证旧地址并完成一批
        pipeline.register_source("switch-src", "http", "default")
        pipeline.validate_source("switch-src", {"endpoint": "http://old:9001/logs"})
        old_logs = [
            {"body": "2026-01-15T10:00:00|INFO|old-1"},
            {"body": "2026-01-15T10:01:00|WARN|old-2"},
        ]
        first = pipeline.start_ingest_batch("switch-src", "2026-01-15", old_logs)
        first_batch = first["batch"]["批次编号"]
        assert first["batch"]["source_version"] == 2
        pipeline.parse_batch(first_batch)
        assert self._batch_of(first_batch)["status"] == "已完成"

        # 2) 更改接入参数后重新验证（新地址通过）
        rev = pipeline.validate_source("switch-src", {"endpoint": "http://new:9002/logs"})
        assert rev["params_changed"] is True
        assert rev["entry"]["source_version"] == 3

        # 3) 旧地址积压的迟到尾包补交（仍用旧版本 v2）
        tail = [
            {"body": "2026-01-15T10:02:00|ERROR|old-tail-1"},
            {"body": "2026-01-15T10:03:00|INFO|old-tail-2"},
        ]
        late_result = pipeline.receive_buffered_logs(
            "switch-src", 2, "2026-01-15", tail, package_id="tail-old-1")
        assert late_result["ok"] is True
        assert late_result["late"] is True
        assert late_result["supplementary"] is True
        assert late_result["supplementary_of"] == first_batch
        assert late_result["accepted"] == 2
        assert late_result["source_version"] == 2
        assert late_result["current_source_version"] == 3
        late_batch = late_result["batch_no"]
        assert late_batch != first_batch

        # 已确认批次不允许回退
        assert self._batch_of(first_batch)["status"] == "已完成"

        # 4) 新地址的一批日志（当前版本 v3）
        new_result = pipeline.receive_buffered_logs(
            "switch-src", 3, "2026-01-15",
            [{"body": "2026-01-15T11:00:00|INFO|new-1"}], package_id="new-batch-1")
        assert new_result["ok"] is True
        assert new_result["late"] is False
        assert new_result["supplementary"] is False
        assert new_result["source_version"] == 3
        new_batch = new_result["batch_no"]

        # 5) 检查两次接收状态和各批的来源版本
        assert self._batch_of(first_batch)["source_version"] == 2
        assert self._batch_of(late_batch)["source_version"] == 2
        assert self._batch_of(late_batch)["late"] is True
        assert self._batch_of(new_batch)["source_version"] == 3

        raws = store.rows("raw_records")
        late_raws = [r for r in raws if r.get("批次编号") == late_batch]
        assert len(late_raws) == 2
        assert all(r["source_version"] == 2 for r in late_raws)
        new_raws = [r for r in raws if r.get("批次编号") == new_batch]
        assert len(new_raws) == 1
        assert all(r["source_version"] == 3 for r in new_raws)

    def test_duplicate_tail_package_recorded_once(self, pipeline):
        """再提交相同的尾包，只记一次原始行。"""
        pipeline.register_source("dup-pkg-src", "http", "default")
        pipeline.validate_source("dup-pkg-src")
        tail = [{"body": "t1|INFO|m1"}, {"body": "t2|WARN|m2"}]
        first = pipeline.receive_buffered_logs(
            "dup-pkg-src", 2, "2026-01-15", tail, package_id="tail-1")
        assert first["ok"] is True
        assert first["accepted"] == 2

        again = pipeline.receive_buffered_logs(
            "dup-pkg-src", 2, "2026-01-15", tail, package_id="tail-1")
        assert again["ok"] is True
        assert again["duplicate_package"] is True
        assert again["accepted"] == 0
        assert again["batch_no"] == first["batch_no"]

        # 未显式传 package_id 时，按内容摘要识别同一尾包
        third = pipeline.receive_buffered_logs("dup-pkg-src", 2, "2026-01-15", tail)
        assert third["duplicate_package"] is True

        raws = [r for r in store.rows("raw_records")
                if r.get("source_id") == "dup-pkg-src"]
        assert len(raws) == 2

    def test_receive_appends_to_open_batch_and_dedup_lines(self, pipeline):
        """同源同版本同窗口的进行中批次直接追加；行级去重。"""
        pipeline.register_source("append-src", "http", "default")
        pipeline.validate_source("append-src")
        started = pipeline.start_ingest_batch(
            "append-src", "2026-01-15", [{"body": "t1|INFO|m1"}])
        batch_no = started["batch"]["批次编号"]

        result = pipeline.receive_buffered_logs(
            "append-src", 2, "2026-01-15",
            [{"body": "t1|INFO|m1"}, {"body": "t2|WARN|m2"}], package_id="pkg-2")
        assert result["ok"] is True
        assert result["batch_no"] == batch_no
        assert result["supplementary"] is False
        assert result["accepted"] == 1
        assert result["skipped_duplicates"] == 1
        assert self._batch_of(batch_no)["记录总数"] == "2"
        # 行号连续递增
        raws = sorted(
            (r for r in store.rows("raw_records") if r.get("批次编号") == batch_no),
            key=lambda r: int(r["原始行号"]),
        )
        assert [r["原始行号"] for r in raws] == ["1", "2"]

    def test_confirmed_batch_never_rolled_back(self, pipeline):
        """补采不动已确认批次：状态、版本都保持。"""
        pipeline.register_source("noroll-src", "http", "default")
        pipeline.validate_source("noroll-src")
        started = pipeline.start_ingest_batch(
            "noroll-src", "2026-01-15", [{"body": "t1|INFO|m1"}])
        batch_no = started["batch"]["批次编号"]
        pipeline.parse_batch(batch_no)
        before = dict(self._batch_of(batch_no))

        pipeline.receive_buffered_logs(
            "noroll-src", 2, "2026-01-15", [{"body": "t2|WARN|late"}], package_id="late-1")
        after = self._batch_of(batch_no)
        assert after["status"] == before["status"] == "已完成"
        assert after["记录总数"] == before["记录总数"] == "1"

    def test_receive_writes_back_receipt(self, pipeline):
        """接收结果回写复验清单，旧地址和新地址的记录可分别排查。"""
        pipeline.register_source("ledger-src", "http", "default")
        pipeline.validate_source("ledger-src", {"endpoint": "http://old:1/logs"})
        started = pipeline.start_ingest_batch("ledger-src", "w1", [{"body": "a|b|c"}])
        pipeline.parse_batch(started["batch"]["批次编号"])  # 首批确认完成
        pipeline.validate_source("ledger-src", {"endpoint": "http://new:2/logs"})

        pipeline.receive_buffered_logs(
            "ledger-src", 2, "w1", [{"body": "d|e|f"}, {"body": "g|h|i"}], package_id="p1")
        pipeline.receive_buffered_logs(
            "ledger-src", 3, "w1", [{"body": "j|k|l"}], package_id="p2")

        receipts = {r["source_version"]: r for r in self._receipts_of("ledger-src")}
        assert receipts[2]["received_records"] == 3  # 旧地址：首批 1 行 + 迟到 2 行
        assert receipts[2]["received_batches"] == 2
        assert receipts[2]["last_received_at"] != ""
        assert receipts[3]["received_records"] == 1  # 新地址：1 行
        assert receipts[3]["received_batches"] == 1

    def test_raw_records_carry_source_version(self, pipeline):
        """采集批次创建原始记录时写下这批实际使用的来源验证版本。"""
        pipeline.register_source("carry-src", "http", "default")
        pipeline.validate_source("carry-src")
        result = pipeline.start_ingest_batch(
            "carry-src", "w1", [{"body": "a|b|c"}, {"body": "d|e|f"}])
        batch_no = result["batch"]["批次编号"]
        raws = [r for r in store.rows("raw_records") if r.get("批次编号") == batch_no]
        assert len(raws) == 2
        assert all(r["source_version"] == 2 for r in raws)

    def test_legacy_validated_source_backfills_receipt(self, pipeline):
        """兼容已有验证回执缺失的旧数据：按当前版本补建回执后正常接收。"""
        pipeline.register_source("legacy-src", "http", "default")
        source = next(s for s in store.rows("source_registry")
                      if s.get("来源标识") == "legacy-src")
        # 模拟旧数据：已验证但没有任何回执
        source["status"] = "已验证"
        source["source_version"] = 5

        result = pipeline.receive_buffered_logs(
            "legacy-src", 5, "w1", [{"body": "a|b|c"}], package_id="p1")
        assert result["ok"] is True
        receipts = self._receipts_of("legacy-src")
        assert len(receipts) == 1
        assert receipts[0]["backfilled"] is True
        assert receipts[0]["source_version"] == 5
        assert receipts[0]["received_records"] == 1

    def test_reception_overview_shows_parse_ready(self, pipeline):
        """值班端能看清各批哪些输入已经能进入解析。"""
        pipeline.register_source("duty-src", "http", "default")
        pipeline.validate_source("duty-src", {"endpoint": "http://old:1/logs"})
        started = pipeline.start_ingest_batch("duty-src", "w1", [{"body": "a|b|c"}])
        first_batch = started["batch"]["批次编号"]
        pipeline.parse_batch(first_batch)  # 首批确认完成，记录进入已解析
        pipeline.validate_source("duty-src", {"endpoint": "http://new:2/logs"})
        late = pipeline.receive_buffered_logs(
            "duty-src", 2, "w1", [{"body": "d|e|f"}], package_id="late-1")

        overview = pipeline.reception_overview("duty-src")
        assert overview["ok"] is True
        entry = overview["sources"][0]
        assert entry["当前验证版本"] == 3
        assert len(entry["复验清单"]) == 2
        batches = {b["批次编号"]: b for b in entry["批次"]}
        assert set(batches) == {first_batch, late["batch_no"]}

        confirmed = batches[first_batch]
        assert confirmed["status"] == "已完成"
        assert confirmed["可进入解析"] is False  # 已解析完毕，无待解析输入
        assert confirmed["source_version"] == 2

        supplementary = batches[late["batch_no"]]
        assert supplementary["可进入解析"] is True  # 迟到补采的输入可进入解析
        assert supplementary["待解析"] == 1
        assert supplementary["late"] is True
        assert supplementary["supplementary"] is True
        assert supplementary["source_version"] == 2