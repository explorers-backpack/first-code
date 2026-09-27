# -*- coding: utf-8 -*-
"""AI 模拟面试 · 知识检索接口（RAG 扩展点）自检

无需 pytest，直接运行：
    python backend/tests/test_knowledge_retriever.py

覆盖范围
--------
- **值对象契约**：``KnowledgeChunk`` 的三键结构、默认值、不可变性、
  ``to_dict`` / ``from_dict`` 往返、非法入参被拒
- **无知识返回 ``[]``**：基类 ``KnowledgeRetriever`` 的默认语义（要求 1）
- **Mock 返回正常**：固定片段、每次新建对象、忽略入参、可记录调用（要求 2）
- **零依赖**：不依赖数据库 / 向量库 / LLM，不依赖 HTTP（要求 2 / 3 / 4）
  —— 用 **AST 解析 import** + **子进程剥掉 ``DATABASE_URL``** 取证
- **未修改 InterviewAgent**：无任何生产调用方、Agent 未 import 本模块、
  ``generate_question`` 签名未变（要求 5）

本测试**不需要数据库**，也不启动 HTTP 服务。
"""

import ast
import asyncio
import inspect
import os
import pathlib
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from services import interview_agent  # noqa: E402
from services.knowledge_retriever import (  # noqa: E402
    KNOWLEDGE_CHUNK_FIELDS,
    MOCK_CHUNKS,
    InvalidChunkError,
    KnowledgeChunk,
    KnowledgeRetriever,
    KnowledgeRetrieverError,
    MockKnowledgeRetriever,
)

BACKEND_DIR = pathlib.Path(__file__).resolve().parents[1]

_PASSED = 0
_FAILED = 0


def _check(name: str, cond: bool, detail: str = "") -> bool:
    global _PASSED, _FAILED
    if cond:
        _PASSED += 1
    else:
        _FAILED += 1
    print(
        ("  [PASS] " if cond else "  [FAIL] ")
        + name
        + (f"  -> {detail}" if detail and not cond else "")
    )
    return cond


def _imported_modules(source: str):
    """AST 收集模块的 import 目标名。

    **不要用子串匹配**——本项目的 docstring 会大量提到模块名
    （本模块的文档就写了「不依赖 chromadb / faiss / LLM」），子串匹配必然误报。
    """
    names = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                names.update(f"{node.module}.{a.name}" for a in node.names)
    return names


def _production_sources():
    """backend 下除 tests / __pycache__ 之外的全部 .py（即「生产代码」）。"""
    for path in BACKEND_DIR.rglob("*.py"):
        parts = set(path.parts)
        if "tests" in parts or "__pycache__" in parts:
            continue
        yield path


class _FakeJob:
    """只有属性、没有 Mapping 接口的对象（模拟 ORM 行 / 任意自定义对象）。"""

    job_name = "后端开发工程师"
    skills = "Python,Redis,Kafka"
    city = "深圳"


# ============================================================
# 主流程
# ============================================================
async def run() -> bool:
    print("=" * 68)
    print("AI 模拟面试 · 知识检索接口（RAG 扩展点）自检")
    print("=" * 68)

    # ------------------------------------------------------------
    # [1] 值对象契约：KnowledgeChunk
    # ------------------------------------------------------------
    print("\n[1] 值对象契约（KnowledgeChunk）")
    _check("字段契约恰为 content / source / metadata",
           KNOWLEDGE_CHUNK_FIELDS == ("content", "source", "metadata"),
           str(KNOWLEDGE_CHUNK_FIELDS))

    empty = KnowledgeChunk()
    _check("默认值：content / source 为空串、metadata 为空 dict",
           empty.content == "" and empty.source == "" and empty.metadata == {})
    _check("to_dict() 恒为三键且顺序一致",
           list(empty.to_dict()) == list(KNOWLEDGE_CHUNK_FIELDS), str(empty.to_dict()))
    _check("  └ 与需求给定结构一致 {'content':'','source':'','metadata':{}}",
           empty.to_dict() == {"content": "", "source": "", "metadata": {}},
           str(empty.to_dict()))

    a, b = KnowledgeChunk(), KnowledgeChunk()
    _check("metadata 用 default_factory（两实例不共享同一个 dict）",
           a.metadata is not b.metadata)

    _check("frozen：属性不可重新绑定",
           _raises(setattr, a, "content", "x"))

    chunk = KnowledgeChunk(content="正文", source="mock://x", metadata={"topic": "Redis"})
    dumped = chunk.to_dict()
    dumped["metadata"]["topic"] = "被改坏了"
    _check("to_dict() 的 metadata 是浅拷贝（改它不影响原对象）",
           chunk.metadata["topic"] == "Redis", str(chunk.metadata))

    _check("from_dict 往返一致",
           KnowledgeChunk.from_dict(chunk.to_dict()) == chunk)
    _check("  └ 未知键被忽略（额外信息应放 metadata）",
           KnowledgeChunk.from_dict(
               {"content": "c", "source": "s", "metadata": {}, "extra": 1}
           ).to_dict() == {"content": "c", "source": "s", "metadata": {}})
    _check("  └ 缺失键取默认值",
           KnowledgeChunk.from_dict({}).to_dict()
           == {"content": "", "source": "", "metadata": {}})
    _check("  └ metadata=None 归一为 {}",
           KnowledgeChunk.from_dict({"metadata": None}).metadata == {})

    _check("from_dict 拒绝非 Mapping 入参", _raises(KnowledgeChunk.from_dict, ["not-a-dict"]))
    _check("from_dict 拒绝 content 非 str", _raises(KnowledgeChunk.from_dict, {"content": 123}))
    _check("from_dict 拒绝 source 非 str", _raises(KnowledgeChunk.from_dict, {"source": 1}))
    _check("from_dict 拒绝 metadata 非 Mapping",
           _raises(KnowledgeChunk.from_dict, {"metadata": "oops"}))
    _check("异常继承 KnowledgeRetrieverError + ValueError",
           issubclass(InvalidChunkError, KnowledgeRetrieverError)
           and issubclass(InvalidChunkError, ValueError))

    # ------------------------------------------------------------
    # [2] 无知识时返回 []（要求 1）
    # ------------------------------------------------------------
    print("\n[2] 无知识时返回 []（基类默认语义）")
    base = KnowledgeRetriever()
    _check("基类可直接实例化（默认语义 = 不提供任何知识）",
           isinstance(base, KnowledgeRetriever))
    _check("source_name 为 'none'", base.source_name == "none", base.source_name)

    for label, args in (
        ("普通 dict 入参", ({"job_name": "后端开发工程师"}, "Redis 持久化", {"stage": "technical"})),
        ("None 入参", (None, None, None)),
        ("空 dict 入参", ({}, "", {})),
        ("ORM 风格对象入参", (_FakeJob(), "微服务拆分", _FakeJob())),
    ):
        result = await base.retrieve(*args)
        _check(f"{label} → []", result == [], repr(result))
        _check("  └ 返回类型是 list（不是 None）", isinstance(result, list), type(result).__name__)

    params = list(inspect.signature(KnowledgeRetriever.retrieve).parameters)
    _check("接口签名恰为 (self, job_info, topic, context) —— 不含 db",
           params == ["self", "job_info", "topic", "context"], str(params))
    _check("retrieve 是协程函数（真实检索需访问外部服务，签名提前异步化）",
           inspect.iscoroutinefunction(KnowledgeRetriever.retrieve))

    # ------------------------------------------------------------
    # [3] Mock 返回正常（要求 2）
    # ------------------------------------------------------------
    print("\n[3] Mock 返回正常（MockKnowledgeRetriever）")
    mock = MockKnowledgeRetriever()
    _check("source_name 为 'mock'", mock.source_name == "mock", mock.source_name)

    chunks = await mock.retrieve({"job_name": "后端"}, "Redis 持久化", {"stage": "technical"})
    _check("默认返回固定片段（条数 == len(MOCK_CHUNKS)）",
           len(chunks) == len(MOCK_CHUNKS) == 3, f"{len(chunks)} vs {len(MOCK_CHUNKS)}")
    _check("  └ 元素都是 KnowledgeChunk",
           all(isinstance(c, KnowledgeChunk) for c in chunks))
    _check("  └ 内容与 MOCK_CHUNKS 逐条一致",
           [c.to_dict() for c in chunks] == [dict(item) for item in MOCK_CHUNKS],
           str([c.to_dict() for c in chunks])[:160])
    _check("  └ 三键结构完整",
           all(set(c.to_dict()) == set(KNOWLEDGE_CHUNK_FIELDS) for c in chunks))

    again = await mock.retrieve(None, None, None)
    _check("每次调用返回新建对象（list 与元素都不共享）",
           again is not chunks and again[0] is not chunks[0])
    chunks[0].metadata["injected"] = True
    third = await mock.retrieve(None, None, None)
    _check("  └ 调用方改返回值不污染检索器内部状态",
           "injected" not in third[0].metadata, str(third[0].metadata))

    _check("忽略入参：不同 job/topic 结果完全相同",
           (await mock.retrieve({"job_name": "A"}, "题 A", {}))[0].content
           == (await mock.retrieve({"job_name": "B"}, "题 B", {}))[0].content)
    _check("记录调用入参（calls）", len(mock.calls) == 5, str(len(mock.calls)))
    _check("  └ 记录的是原始入参", mock.calls[0][1] == "Redis 持久化")

    _check("chunks 属性返回副本", mock.chunks is not mock._chunks)
    mock.chunks.clear()
    _check("  └ 改副本不影响内部配置", len(await mock.retrieve(None, None, None)) == 3)

    _check("传 chunks=[] 可模拟无知识 → []",
           await MockKnowledgeRetriever([]).retrieve(None, None, None) == [])

    custom = MockKnowledgeRetriever([
        {"content": "来自 dict 的片段", "source": "custom://a", "metadata": {"k": 1}},
        KnowledgeChunk(content="来自对象的片段", source="custom://b"),
    ])
    custom_out = await custom.retrieve(None, None, None)
    _check("自定义 chunks：dict 与 KnowledgeChunk 混用都被归一",
           len(custom_out) == 2 and all(isinstance(c, KnowledgeChunk) for c in custom_out))
    _check("  └ 内容正确", custom_out[0].content == "来自 dict 的片段"
           and custom_out[1].content == "来自对象的片段")

    overridden = MockKnowledgeRetriever(source="mock://overridden")
    _check("source 可覆盖（便于测试区分来源）",
           all(c.source == "mock://overridden" for c in await overridden.retrieve(None, None, None)))

    # ------------------------------------------------------------
    # [4] 零依赖：不依赖数据库 / 向量库 / LLM（要求 2 / 3 / 4）
    # ------------------------------------------------------------
    print("\n[4] 零依赖（无数据库 / 无向量库 / 不调 LLM）")
    from services import knowledge_retriever as kr

    source = pathlib.Path(kr.__file__).read_text(encoding="utf-8")
    imports = _imported_modules(source)
    top_level = {name.split(".")[0] for name in imports}

    _check("只 import 标准库（无任何第三方包）",
           top_level <= set(sys.stdlib_module_names) | {"__future__"}, str(sorted(top_level)))
    _check("  └ 顶层依赖恰为 __future__ / collections / dataclasses / typing",
           top_level == {"__future__", "collections", "dataclasses", "typing"},
           str(sorted(top_level)))

    forbidden = {
        "sqlalchemy", "aiosqlite", "aiomysql", "fastapi", "pydantic",  # 数据库 / HTTP 框架
        "chromadb", "faiss", "milvus", "weaviate", "pinecone", "qdrant",  # 向量库
        "numpy", "scipy", "sentence_transformers", "transformers",       # 向量化 / 模型
        "openai", "requests", "websocket", "websockets", "aiohttp",      # LLM / 网络
        "main", "models", "database", "deps",                            # 本项目基础设施
    }
    hit = forbidden & {name.split(".")[0] for name in imports}
    _check("不 import 数据库 / 向量库 / LLM / 本项目基础设施",
           hit == set(), str(sorted(hit)))

    # 子进程取证：剥掉 DATABASE_URL 也能导入，且没有第三方包被拉进 sys.modules
    env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
    probe = (
        "import sys; sys.path.insert(0, '.');"
        "from services.knowledge_retriever import KnowledgeRetriever, MockKnowledgeRetriever;"
        "leak=[m for m in ('sqlalchemy','fastapi','main','models','database','pydantic',"
        "'chromadb','faiss','numpy','openai','requests','websocket') if m in sys.modules];"
        "print('LEAK:' + ','.join(leak))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(BACKEND_DIR), env=env, capture_output=True, text=True,
    )
    _check("子进程（无 DATABASE_URL）可导入", proc.returncode == 0,
           f"{proc.returncode} {proc.stderr.strip()[:160]}")
    _check("  └ 导入后无第三方 / 基础设施模块泄漏进 sys.modules",
           "LEAK:" in proc.stdout and proc.stdout.strip().endswith("LEAK:"),
           proc.stdout.strip()[-120:])

    # ------------------------------------------------------------
    # [5] 接线点唯一：只有 interview_core（Agent 仍不 import 本模块）
    # ------------------------------------------------------------
    print("\n[5] 接线点唯一（只有 interview_core 调检索；Agent 只接收知识）")
    # **实现方**（实现本接口的模块）天然要 import 本模块——那是「实现」而不是「接线」，
    # 因此按路径排除；其余模块则用**精确模块匹配**判定（不用
    # ``endswith("knowledge_retriever")``，否则 ``services.knowledge_rag`` 这类
    # 名字相近的模块会被误判）。
    INTERFACE = "services.knowledge_retriever"
    IMPLEMENTATIONS = {"services/vector_knowledge_retriever.py"}

    def _imports_interface(mods: set) -> bool:
        return any(m == INTERFACE or m.startswith(INTERFACE + ".") for m in mods)

    offenders = []
    for path in _production_sources():
        rel = path.relative_to(BACKEND_DIR).as_posix()
        if rel in IMPLEMENTATIONS:
            continue
        if _imports_interface(_imported_modules(path.read_text(encoding="utf-8"))):
            offenders.append(rel)
    _check("面试流程侧**唯一**接线点是 interview_core（实现方不计；精确匹配接口模块）",
           offenders == ["services/interview_core.py"], str(offenders))

    impl_path = BACKEND_DIR / "services" / "vector_knowledge_retriever.py"
    _check("真实检索器存在（实现本接口，正向依赖）", impl_path.exists())
    impl_imports = _imported_modules(impl_path.read_text(encoding="utf-8"))
    _check("  └ 它 import 本接口（实现方）", _imports_interface(impl_imports),
           str(sorted(impl_imports)))
    _check("  └ 但**不**反向 import 面试流程（interview_* / main）",
           not any("interview" in name or name == "main" for name in impl_imports),
           str(sorted(impl_imports)))

    rag_path = BACKEND_DIR / "services" / "knowledge_rag.py"
    _check("组装器 knowledge_rag 存在（唯一一处知道用哪个 Embedding / 向量后端）",
           rag_path.exists())
    rag_imports = _imported_modules(rag_path.read_text(encoding="utf-8"))
    _check("  └ 它**不**直接 import 接口（只组装实现方），也不反向 import 面试流程",
           not _imports_interface(rag_imports)
           and not any("interview" in name or name == "main" for name in rag_imports),
           str(sorted(rag_imports)))

    agent_src = pathlib.Path(interview_agent.__file__).read_text(encoding="utf-8")
    agent_imports = _imported_modules(agent_src)
    _check("InterviewAgent 未 import 本模块（知识由调用方传入，Agent 不检索）",
           not any(n.endswith("knowledge_retriever") for n in agent_imports),
           str(sorted(agent_imports)))
    agent_names = {
        node.id for node in ast.walk(ast.parse(agent_src)) if isinstance(node, ast.Name)
    } | {
        node.attr for node in ast.walk(ast.parse(agent_src)) if isinstance(node, ast.Attribute)
    }
    _check("  └ 源码中也不出现 KnowledgeRetriever 标识符（AST 取证，非子串匹配）",
           "KnowledgeRetriever" not in agent_names)

    sig = inspect.signature(interview_agent.generate_question)
    _check("generate_question 已新增 knowledge_context（知识注入点）",
           list(sig.parameters) == [
               "context", "plan", "resume", "job", "knowledge_context", "spark",
           ],
           str(list(sig.parameters)))
    _check("  └ knowledge_context 是 keyword-only（不会错位既有位置参数）",
           sig.parameters["knowledge_context"].kind is inspect.Parameter.KEYWORD_ONLY,
           str(sig.parameters["knowledge_context"].kind))
    _check("  └ 仍无 retriever / chunks 参数（Agent 不持有检索器）",
           not any(k in sig.parameters for k in ("retriever", "chunks")))

    print("\n" + "=" * 68)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 68)
    return _FAILED == 0


def _raises(func, *args) -> bool:
    try:
        func(*args)
    except Exception:  # noqa: BLE001
        return True
    return False


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run()) else 1)
