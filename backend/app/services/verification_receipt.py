"""验证回执领域服务：复验清单的查询骨架由基类复用，回执由采集流水线写入。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class VerificationReceiptService(BaseModuleService):
    MODULE = "verification_receipt"
    ENTITY = "验证回执"
    OPERATION_TERM = "复验"
    LIST_FIELDS = ["来源标识", "验证版本", "接入参数摘要", "验证时间", "回执状态"]
    REQUIRED_FIELDS = ["来源标识", "验证版本"]
    STATUS_ORDER = ["当前版本", "历史版本"]
    ACTION_RULES = {}
    NEGATIVE_ACTIONS = []
