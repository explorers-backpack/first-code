"""共享 LLM 端口；装配层注册实例，业务层不导入应用入口。"""
from typing import Any

_llm: Any = None


def configure(llm: Any) -> None:
    global _llm
    _llm = llm


def get_llm() -> Any:
    if _llm is None:
        raise RuntimeError("LLM facade has not been configured")
    return _llm
