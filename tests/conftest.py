"""pytest 公共夹具与日志辅助：测试日志统一打印输入与判定依据。"""

from __future__ import annotations

import logging

import pytest

logger = logging.getLogger("object-storage-tests")


@pytest.fixture
def log_case():
    def _log(title: str, **fields) -> None:
        rendered = " | ".join(f"{k}={v!r}" for k, v in fields.items())
        logger.info("CASE %s :: %s", title, rendered)

    return _log
