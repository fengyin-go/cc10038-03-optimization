"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。

状态流转梳理（数据源复验与批次接收）：

数据源（source_registry）：待验证 → 已验证 → 已停用。
  每次验证（含更改接入参数后的重新验证）使 source_version 递增，
  并写入一条验证回执（verification_receipt，即复验清单）：新回执为
  「当前版本」，旧回执转为「历史版本」，仍可作为迟到日志的接收依据。

采集批次（ingest_batch）：待采集 → 采集中 → 已完成 / 部分失败。
  已确认批次（已完成 / 部分失败）不允许回退；迟到缓冲日志走补采通道：
  同源同版本同窗口的进行中批次直接追加，否则新开补采批次并标记
  supplementary，原已确认批次保持原状态。

接收依据（receive_buffered_logs）：
  1. 来源已登记；
  2. 提交的来源验证版本命中该来源的验证回执——复验与补采并发时，
     只接受来源和验证版本对得上的记录；
  3. 相同尾包（package_id）重复提交只记一次原始行；
  4. 原始记录写下本批实际使用的来源验证版本，接收结果回写复验清单，
     便于排查旧地址和新地址的记录。
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any

from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def _find_source(self, source_id: str) -> dict[str, Any] | None:
        for s in store.rows("source_registry"):
            if s.get("来源标识") == source_id:
                return s
        return None

    @staticmethod
    def _params_digest(source_entry: dict[str, Any]) -> str:
        """接入参数摘要：协议 + 接入地址 + 连接凭证，用于识别参数变化。"""
        return _stable_digest({
            "protocol": source_entry.get("接入协议", ""),
            "endpoint": source_entry.get("接入地址", ""),
            "credential": source_entry.get("连接凭证", ""),
        })

    def _apply_access_params(
        self,
        source_entry: dict[str, Any],
        access_params: dict[str, Any],
    ) -> bool:
        """把新接入参数写入来源，返回参数是否发生变化。"""
        mapping = {"protocol": "接入协议", "endpoint": "接入地址", "credential": "连接凭证"}
        changed = False
        for key, field in mapping.items():
            value = access_params.get(key)
            if value is not None and str(value) != str(source_entry.get(field, "")):
                source_entry[field] = str(value)
                changed = True
        if changed:
            source_entry["params_digest"] = self._params_digest(source_entry)
        return changed

    # ----- 验证回执（复验清单） -----
    def _receipts(self) -> list[dict[str, Any]]:
        return store.rows("verification_receipt")

    def _find_receipt(self, source_id: str, source_version: int) -> dict[str, Any] | None:
        for r in self._receipts():
            if r.get("来源标识") == source_id and r.get("source_version") == source_version:
                return r
        return None

    def _write_receipt(
        self,
        source_entry: dict[str, Any],
        params_changed: bool,
        backfilled: bool = False,
    ) -> dict[str, Any]:
        """每次验证写一条回执；旧的「当前版本」回执转为「历史版本」。"""
        source_id = source_entry.get("来源标识")
        version = source_entry.get("source_version", 1)
        for r in self._receipts():
            if r.get("来源标识") == source_id and r.get("status") == "当前版本":
                r["status"] = "历史版本"
                r["回执状态"] = "历史版本"
        receipt = {
            "id": _next_id("verification_receipt"),
            "来源标识": source_id,
            "验证版本": f"v{version}",
            "接入参数摘要": source_entry.get("params_digest") or self._params_digest(source_entry),
            "验证时间": _now_iso(),
            "回执状态": "当前版本",
            "status": "当前版本",
            "pending": False,
            "abnormal": False,
            "source_version": version,
            "params_changed": params_changed,
            "backfilled": backfilled,
            "received_batches": 0,
            "received_records": 0,
            "last_received_at": "",
        }
        self._receipts().append(receipt)
        return receipt

    def _ensure_current_receipt(self, source_entry: dict[str, Any]) -> dict[str, Any] | None:
        """兼容已有验证数据：已验证来源缺少当前版本回执时按现状补建。"""
        if source_entry.get("status") != "已验证":
            return None
        receipt = self._find_receipt(
            source_entry.get("来源标识"), source_entry.get("source_version", 1))
        if receipt is None:
            receipt = self._write_receipt(source_entry, params_changed=False, backfilled=True)
        return receipt

    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "接入地址": "",
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        entry["params_digest"] = self._params_digest(entry)
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(
        self,
        source_id: str,
        access_params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """验证/复验数据源。提供 access_params 时按新接入参数重新验证：
        参数变化会更新来源接入配置；每次验证递增版本并写验证回执。"""
        source_entry = self._find_source(source_id)
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 不存在"}
        params_changed = self._apply_access_params(source_entry, access_params or {})
        source_entry["status"] = "已验证"
        source_entry["pending"] = False
        source_entry["abnormal"] = False
        source_entry["source_version"] = source_entry.get("source_version", 1) + 1
        source_entry["validated_at"] = _now_iso()
        receipt = self._write_receipt(source_entry, params_changed)
        message = f"数据源 {source_id} 已验证"
        if params_changed:
            message = f"数据源 {source_id} 已按新接入参数复验"
        return {
            "ok": True,
            "message": message,
            "entry": source_entry,
            "receipt": receipt,
            "params_changed": params_changed,
        }

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}
        # 兼容已有验证数据：确保当前版本有回执，接收结果稍后会回写
        receipt = self._ensure_current_receipt(source_entry)
        current_version = source_entry.get("source_version", 1)

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": current_version,
            "late": False,
            "supplementary": False,
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录：写下这批实际使用的来源验证版本
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "source_version": current_version,
                    "package_id": str(log.get("package_id", "")) if isinstance(log, dict) else "",
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        # 接收结果回写复验清单
        if receipt is not None:
            self._write_back_receipt(receipt, source_id, current_version)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段1.5：迟到缓冲日志接收（补采） ----------
    OPEN_BATCH_STATUS = ("待采集", "采集中")
    CONFIRMED_BATCH_STATUS = ("已完成", "部分失败")

    def receive_buffered_logs(
        self,
        source_id: str,
        source_version: int,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
        package_id: str | None = None,
    ) -> dict[str, Any]:
        """接收迟到的缓冲日志：核对实际来源验证版本后写入原始记录。

        只接受来源和验证版本对得上的记录；已确认批次不回退，需要补采时
        新开补采批次；相同尾包重复提交只记一次原始行。
        """
        source_entry = self._find_source(source_id)
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        # 兼容已有验证数据：已验证来源缺回执时先补建
        self._ensure_current_receipt(source_entry)

        try:
            version = int(source_version)
        except (TypeError, ValueError):
            return {"ok": False, "message": f"来源验证版本 {source_version!r} 无法识别，拒收"}

        # 核对实际来源验证版本：必须命中该来源的验证回执
        receipt = self._find_receipt(source_id, version)
        if receipt is None:
            current = source_entry.get("source_version", 1)
            return {
                "ok": False,
                "message": f"数据源 {source_id} 的验证版本 v{version} 未通过验证（当前 v{current}），拒收",
            }

        raw_logs = raw_logs or []
        current_version = source_entry.get("source_version", 1)
        late = version < current_version

        # 尾包幂等：相同尾包重复提交只记一次原始行。
        # 内容摘要始终计算：调用方未传 package_id 或前后两次传法不一致时，
        # 仍能按内容识别同一尾包。
        package_digest = "pkg-" + _stable_digest({
            "source": source_id, "version": version,
            "window": time_window, "logs": raw_logs,
        })
        if not package_id:
            package_id = package_digest
        existing = self._find_package_reception(source_id, version, package_id, package_digest)
        if existing is not None:
            existing.update({
                "ok": True,
                "duplicate_package": True,
                "current_source_version": current_version,
                "message": f"尾包 {package_id} 已接收过，本次不重复记录",
            })
            return existing

        # 批次归属：进行中批次直接追加；已确认批次不回退，新开补采批次
        batch = self._find_batch(source_id, version, time_window, self.OPEN_BATCH_STATUS)
        supplementary_of = ""
        if batch is None:
            confirmed = self._find_batch(source_id, version, time_window, self.CONFIRMED_BATCH_STATUS)
            if confirmed is not None:
                supplementary_of = confirmed.get("批次编号", "")
            batch = self._new_receive_batch(source_entry, version, time_window, late, supplementary_of)

        created_ids, skipped = self._append_raw_logs(
            batch, source_id, version, raw_logs, package_id, package_digest)
        self._refresh_batch_total(batch)
        # 接收结果回写复验清单
        self._write_back_receipt(receipt, source_id, version)

        batch_no = batch["批次编号"]
        return {
            "ok": True,
            "message": self._receive_message(batch_no, version, late, len(created_ids), skipped),
            "batch_no": batch_no,
            "source_version": version,
            "current_source_version": current_version,
            "late": late,
            "supplementary": bool(supplementary_of),
            "supplementary_of": supplementary_of,
            "package_id": package_id,
            "accepted": len(created_ids),
            "skipped_duplicates": skipped,
            "duplicate_package": False,
            "parse_ready": self._parse_ready_count(batch_no),
        }

    def _find_batch(
        self,
        source_id: str,
        source_version: int,
        time_window: str,
        statuses: tuple[str, ...],
    ) -> dict[str, Any] | None:
        for b in store.rows("ingest_batch"):
            if (b.get("所属数据源") == source_id
                    and b.get("source_version") == source_version
                    and b.get("采集时间窗口") == time_window
                    and b.get("status") in statuses):
                return b
        return None

    def _new_receive_batch(
        self,
        source_entry: dict[str, Any],
        source_version: int,
        time_window: str,
        late: bool,
        supplementary_of: str,
    ) -> dict[str, Any]:
        batches = store.rows("ingest_batch")
        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_entry.get("来源标识"),
            "采集时间窗口": time_window,
            "记录总数": "0",
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_version,
            "late": late,
            "supplementary": bool(supplementary_of),
            "supplementary_of": supplementary_of,
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)
        return batch_entry

    def _append_raw_logs(
        self,
        batch: dict[str, Any],
        source_id: str,
        source_version: int,
        raw_logs: list[dict[str, Any]],
        package_id: str,
        package_digest: str = "",
    ) -> tuple[list[int], int]:
        """向批次追加原始记录：行级去重，记录写下实际使用的来源验证版本。"""
        batch_no = batch["批次编号"]
        raws = store.rows("raw_records")
        batch_raw = [r for r in raws if r.get("批次编号") == batch_no]
        seen = {_stable_digest({"body": r.get("原始正文", "")}) for r in batch_raw}
        line_no = 0
        for r in batch_raw:
            try:
                line_no = max(line_no, int(r.get("原始行号", "0")))
            except (TypeError, ValueError):
                continue
        created_ids: list[int] = []
        skipped = 0
        for log in raw_logs:
            body = log.get("body", str(log)) if isinstance(log, dict) else str(log)
            digest = _stable_digest({"body": body})
            if digest in seen:
                skipped += 1
                continue
            seen.add(digest)
            line_no += 1
            raw_id = _next_id("raw_records")
            raws.append({
                "id": raw_id,
                "批次编号": batch_no,
                "原始行号": str(line_no),
                "采集时间戳": _now_iso(),
                "原始正文": body,
                "解析状态": "未解析",
                "status": "待解析",
                "pending": True,
                "abnormal": False,
                "batch_version": batch.get("batch_version", 1),
                "source_id": source_id,
                "source_version": source_version,
                "package_id": package_id,
                "package_digest": package_digest,
                "ingested_at": _now_iso(),
            })
            created_ids.append(raw_id)
        try:
            ids = json.loads(batch.get("raw_record_ids", "[]"))
        except (json.JSONDecodeError, TypeError):
            ids = []
        ids.extend(created_ids)
        batch["raw_record_ids"] = json.dumps(ids)
        return created_ids, skipped

    def _refresh_batch_total(self, batch: dict[str, Any]) -> None:
        total = sum(1 for r in store.rows("raw_records")
                    if r.get("批次编号") == batch["批次编号"])
        batch["记录总数"] = str(total)

    def _parse_ready_count(self, batch_no: str) -> int:
        """值班端视角：该批次已进入待解析、可以被解析的记录数。"""
        return sum(1 for r in store.rows("raw_records")
                   if r.get("批次编号") == batch_no and r.get("status") == "待解析")

    def _find_package_reception(
        self,
        source_id: str,
        source_version: int,
        package_id: str,
        package_digest: str = "",
    ) -> dict[str, Any] | None:
        """按尾包标识或内容摘要查找已接收记录，命中即视为重复提交。"""
        records = [
            r for r in store.rows("raw_records")
            if r.get("source_id") == source_id
            and r.get("source_version") == source_version
            and (r.get("package_id") == package_id
                 or (package_digest and r.get("package_digest") == package_digest))
        ]
        if not records:
            return None
        batch_no = records[0].get("批次编号")
        batch = next(
            (b for b in store.rows("ingest_batch") if b.get("批次编号") == batch_no), {})
        return {
            "batch_no": batch_no,
            "source_version": source_version,
            "late": bool(batch.get("late")),
            "supplementary": bool(batch.get("supplementary")),
            "supplementary_of": batch.get("supplementary_of", ""),
            "package_id": package_id,
            "accepted": 0,
            "skipped_duplicates": 0,
            "parse_ready": self._parse_ready_count(batch_no),
        }

    @staticmethod
    def _receive_message(
        batch_no: str, version: int, late: bool, accepted: int, skipped: int,
    ) -> str:
        tag = "迟到补采" if late else "接收"
        message = f"批次 {batch_no} {tag}完成：版本 v{version} 接收 {accepted} 行"
        if skipped:
            message += f"，跳过重复 {skipped} 行"
        return message

    def _batch_source(self, batch_no: str) -> str | None:
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                return b.get("所属数据源")
        return None

    def _raw_version(self, raw: dict[str, Any]) -> int | None:
        """兼容旧记录：缺 source_version 时回退到所属批次的来源版本。"""
        version = raw.get("source_version")
        if version is not None:
            return version
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == raw.get("批次编号"):
                return b.get("source_version")
        return None

    def _write_back_receipt(
        self,
        receipt: dict[str, Any],
        source_id: str,
        source_version: int,
    ) -> None:
        """接收结果回写复验清单：按原始记录实际归属重算该版本的接收统计。"""
        matched = []
        for r in store.rows("raw_records"):
            if self._raw_version(r) != source_version:
                continue
            if r.get("source_id") == source_id or self._batch_source(r.get("批次编号", "")) == source_id:
                matched.append(r)
        receipt["received_batches"] = len({r.get("批次编号") for r in matched})
        receipt["received_records"] = len(matched)
        receipt["last_received_at"] = _now_iso()

    # ---------- 值班端接收总览 ----------
    def reception_overview(self, source_id: str | None = None) -> dict[str, Any]:
        """值班端视图：各来源的验证版本、复验清单接收统计与批次可解析状态。"""
        sources = store.rows("source_registry")
        receipts = store.rows("verification_receipt")
        batches = store.rows("ingest_batch")
        raws = store.rows("raw_records")

        if source_id:
            source_ids = [source_id]
        else:
            source_ids = sorted(
                {str(s.get("来源标识")) for s in sources if s.get("来源标识")}
                | {str(b.get("所属数据源")) for b in batches if b.get("所属数据源")}
            )

        overview: list[dict[str, Any]] = []
        for sid in source_ids:
            source_entry = self._find_source(sid)
            batch_views = []
            for b in batches:
                if b.get("所属数据源") != sid:
                    continue
                batch_no = b.get("批次编号")
                b_raw = [r for r in raws if r.get("批次编号") == batch_no]
                parse_ready = sum(1 for r in b_raw if r.get("status") == "待解析")
                batch_views.append({
                    "批次编号": batch_no,
                    "source_version": b.get("source_version"),
                    "status": b.get("status"),
                    "记录总数": len(b_raw),
                    "待解析": parse_ready,
                    "可进入解析": parse_ready > 0,
                    "late": bool(b.get("late")),
                    "supplementary": bool(b.get("supplementary")),
                })
            receipt_views = [
                {
                    "验证版本": r.get("验证版本"),
                    "source_version": r.get("source_version"),
                    "回执状态": r.get("status"),
                    "接入参数摘要": r.get("接入参数摘要"),
                    "验证时间": r.get("验证时间"),
                    "received_batches": r.get("received_batches", 0),
                    "received_records": r.get("received_records", 0),
                    "last_received_at": r.get("last_received_at", ""),
                }
                for r in receipts if r.get("来源标识") == sid
            ]
            overview.append({
                "来源标识": sid,
                "来源状态": source_entry.get("status") if source_entry else "未登记",
                "当前验证版本": source_entry.get("source_version") if source_entry else None,
                "复验清单": receipt_views,
                "批次": batch_views,
            })
        return {"ok": True, "sources": overview}

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()