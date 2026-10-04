"""测试日志辅助：会话开头打印“业务 -> 用例”覆盖清单，结尾打印各业务通过情况。

用法：uv run pytest -v -s
  - 每个用例内部用 [CASE] 打印输入与判定依据；
  - pytest -v 逐行给出 PASSED/FAILED；
  - 会话开头的“业务覆盖”列出覆盖的业务与对应用例，结尾“业务结论”汇总各业务结果。
"""

from __future__ import annotations

BUSINESS_GROUPS: list[tuple[str, list[str]]] = [
    ("A 重复删除：删除后窗口内恢复，再用同一重试标识重复删除（成功必真删、历史不乱）",
     ["test_delete_retry_after_recover_actually_deletes"]),
    ("B 重复删除被拒：恢复后仍在保留期（retention_violation，状态原样）",
     ["test_delete_retry_within_retention_rejected_state_unchanged"]),
    ("C 重复回滚：回滚后又写入新内容，同键重试（读到的必为目标、正常写入不丢）",
     ["test_rollback_retry_after_new_write_matches_target"]),
    ("D 重复回滚被拒：对象已被删除 / 重试时超配额（拒绝且状态不变）",
     ["test_rollback_retry_when_deleted_rejected_state_unchanged",
      "test_rollback_retry_quota_exceeded_state_unchanged"]),
    ("E 重试标识作用域：同一对象两个上传会话共用同一标识，各得各的结果、互不顶替",
     ["test_two_upload_sessions_same_idem_key_isolated"]),
    ("F 重复恢复被拒：超出恢复窗口 / 对象已存活（状态不变）",
     ["test_recover_retry_after_window_expired_state_unchanged",
      "test_recover_retry_when_live_state_unchanged"]),
    ("G 并发：回滚/删除/恢复/写入混合，每次读取都对应某一完整版本",
     ["test_concurrent_mixed_ops_reads_always_complete_version"]),
]

REGRESSION_GROUPS: list[tuple[str, str]] = [
    ("分块上传：完整流程/缺失/重复/越界分片、校验和、中止与已完成会话",
     "tests/test_storage.py::test_multipart_* / test_*part* / test_abort_* / test_completed_*"),
    ("版本与并发：覆盖可见性、删除墓碑、并发写入无污染、并发读写不撕裂",
     "tests/test_storage.py::test_overwrite_* / test_delete_tombstone_* / test_concurrent_*"),
    ("配额与保留：容量/数量配额、保留期拦截",
     "tests/test_storage.py::test_quota_* / test_retention_*"),
    ("幂等基础：写入/合并/创建上传的幂等重放",
     "tests/test_storage.py::test_idempotent_*"),
    ("回滚/恢复原有语义、跨对象隔离、跨操作冲突、重复删除结论",
     "tests/test_recovery.py 全部用例"),
    ("HTTP 入口错误结构与结论一致",
     "tests/test_api.py::test_http_*"),
]


def pytest_report_header(config):
    lines = ["", "================ 重试语义专项：业务覆盖清单 ================"]
    for title, cases in BUSINESS_GROUPS:
        lines.append(f"  {title}")
        for name in cases:
            lines.append(f"      - tests/test_idempotent_retry.py::{name}")
    lines.append("  既有业务回归保护：")
    for title, where in REGRESSION_GROUPS:
        lines.append(f"      * {title}  [{where}]")
    lines.append("==========================================================")
    return lines


def _outcome(terminal_reporter, case_name: str) -> str:
    if terminal_reporter is None:
        return "?"
    stats = terminal_reporter.stats
    for name in ("passed", "failed", "error", "skipped"):
        for report in stats.get(name, []):
            if report.nodeid.endswith(case_name):
                return name.upper()
    return "未执行"


def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    print("\n================ 重试语义专项：各业务结论 ================")
    for title, cases in BUSINESS_GROUPS:
        outcomes = "/".join(_outcome(reporter, c) for c in cases)
        print(f"  [{outcomes}] {title}")
    print("========================================================")
