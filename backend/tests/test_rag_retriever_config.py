# -*- coding: utf-8 -*-
"""RAG 检索默认参数 · 配置注入 · 自检（脚本式，非 pytest）。

运行：``python backend/tests/test_rag_retriever_config.py``

为什么需要这一套件
------------------
任务 70 · R1 让 ``retriever_kwargs`` 能从调用方**显式**透传到组装器，
但**生产链路本身**仍然没有任何地方会传它：

- ``interview_core.resolve_retriever(db, None, use_rag=True)`` 调
  ``build_vector_retriever(db)`` 时**一个参数都不给**；
- ``api/interview.py`` 的 7 个路由**没有任何一处**调用 ``generate_next_question``
  （``start_session`` 走纯规则 ``build_question_plan``）。

⇒ 「配了阈值」与「阈值生效」仍是两件事：默认口径恒为
``top_k=5 / min_score=None``，低分 chunk 照样进 ``knowledge_context``、白占 Prompt 预算。

本套件锁死这次的修复：在**组装器**（全项目唯一知道「该配哪个 Embedding / 哪个后端」
的地方）引入**配置注入**——``RAG_TOP_K`` / ``RAG_MIN_SCORE`` 提供**默认值**，
调用方显式传入的参数**覆盖**它。

本套件验证什么
--------------
[1] **解析语义**——``resolve_retriever_defaults`` 不设 / 空串 / 单变量 / 双变量；
    ``env=`` 参数**真的**替代 ``os.environ``（不是「读了环境变量又假装读了入参」）。
[2] **非法值必须报错**（``RetrieverConfigError``，且是 ``ValueError``）——
    **刻意不静默退回默认**：配错了却「看起来正常地不过滤」比报错更难排查。
    含 ``0`` / ``1.5`` / ``1_0`` / ``++8`` / ``²`` / ``25``（越界）/ ``nan`` / ``inf``。
[3] **组装层**——不配环境变量 ⇒ 与 ``VectorKnowledgeRetriever(embedder, store)``
    **逐字段等价**（默认行为逐字节不变）；配了 ⇒ ``top_k`` / ``min_score``
    真的落到检索器上；**显式传参始终优先**，且是**合并**不是整体替换。
[4] **端到端接线**——``interview_core.resolve_retriever(use_rag=True)`` 在配了
    ``RAG_MIN_SCORE`` 后自动带上阈值；非法配置按既有约定**静默降级为「无知识」**；
    注入 ``retriever`` / ``use_rag=False`` 两条分支**完全不读**这两个变量。
[5] **不越层 / 接口冻结**（AST + 签名守卫）——组装器顶层仍只有标准库；
    ``RAG_*`` 两个变量名在全项目**只被组装器读**；``VectorStore`` 抽象方法集与
    ``search`` 参数表未变；``VectorKnowledgeRetriever.__init__`` 参数表未变；
    组装器**没有**把「按分数过滤」搬进来（过滤仍由 ``VectorStore`` 承担）。

.. note::
    本套件**只读**生产代码，不写任何业务文件、不改任何业务逻辑。
    **不需要数据库**：配置注入是**纯组装**（只构造对象、不发起 IO），
    因此 ``db`` 用一个**哨兵对象**即可——这本身就是「组装层不碰 DB」的取证。
    内存 SQLite 仅出现在 ``tests/`` 下（项目硬约束），本套件连它都不用。
"""

from __future__ import annotations

import ast
import inspect
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

# ``services.knowledge_rag`` 是**延迟导入** vector_store_sql → models → database 的，
# 而 ``database`` 在 import 期就要求 DATABASE_URL。本套件不建连接，只为让 import 链通过。
import regression_env  # noqa: E402,F401  钉住离线 Embedding + RAG 阈值（回归不受 .env 影响）
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

import models  # noqa: E402,F401  确保全部模型注册到 Base.metadata
from services import interview_core, knowledge_rag  # noqa: E402
from services.knowledge_rag import (  # noqa: E402
    ENV_RAG_MIN_SCORE,
    ENV_RAG_MIN_SCORE_RATIO,
    ENV_RAG_TOP_K,
    build_vector_retriever,
    default_embedder,
    resolve_retriever_defaults,
)
from services.vector_knowledge_retriever import (  # noqa: E402
    RetrieverConfigError,
    VectorKnowledgeRetriever,
)
from services.vector_store import (  # noqa: E402
    DEFAULT_TOP_K,
    VectorMatch,
    VectorStore,
)
from services.vector_store_sql import SqlAlchemyVectorStore  # noqa: E402

# ============================================================
# 一、常量
# ============================================================
#: 组装器**允许**的顶层 import（只有标准库）。多一个都不行——
#: 顶层一旦拉进 ``models`` → ``database``，本模块就变成「没有 DATABASE_URL
#: 就无法 import」，连带破坏 ``interview_core`` 的零耦合性质。
ALLOWED_TOP_LEVEL_IMPORTS = {"__future__", "math", "os", "collections.abc", "typing"}

#: ``VectorStore`` 接口的抽象方法集（**冻结**：本次改动一行都不该动它）。
FROZEN_VECTOR_STORE_METHODS = {"add", "search", "count"}

#: ``VectorStore.search`` 的参数表（**冻结**）。
FROZEN_SEARCH_PARAMS = ["self", "query_vector", "top_k", "model", "document_id",
                        "category", "min_score"]

#: ``VectorKnowledgeRetriever.__init__`` 的参数表（**冻结**）。
#: 任务 81 追加了 ``min_score_ratio``（**keyword-only、追加在最后** ⇒ 既有位置参数
#: 顺序与数量一个未变，老调用方零影响）。
FROZEN_RETRIEVER_PARAMS = ["self", "embedder", "store", "top_k", "min_score",
                           "category", "document_id", "model", "dedup",
                           "min_score_ratio"]

#: ``RAG_TOP_K`` 的非法取值（必须报错，不得静默退回默认）。
#: 注意**不含** ``" "``：纯空白会被 ``strip()`` 成空串，按约定等价于「不设」（见 [1]）。
#: ``²`` 一条是刻意的：``"²".isdigit()`` 为真但 ``int("²")`` 会抛 ``ValueError``，
#: 用 ``isdigit()`` 守卫会从「配置非法」退化成「未捕获异常」。
BAD_TOP_K = ("0", "-1", "1.5", "1_0", "x", "++8", "8.0", "²", "٨_٨")

#: ``RAG_MIN_SCORE`` 的非法取值。
BAD_MIN_SCORE = ("high", "nan", "inf", "-inf", "25", "-1.5", "1.5", "1_0")

#: ``RAG_MIN_SCORE_RATIO`` 的非法取值（合法域是**比例** ``(0, 1]``，不是余弦域 ``[-1, 1]``）。
#: ``"0"`` / ``"-1"`` 在余弦口径下合法、在比例口径下非法 ⇒ 这两条专门验证
#: 「没有错误地复用 ``require_min_score`` 的校验口径」。
BAD_MIN_SCORE_RATIO = ("high", "nan", "inf", "-inf", "0", "-1", "1.5", "99.5", "1_0")

_PASSED = 0
_FAILED = 0


def _section(title: str) -> None:
    print("\n" + "-" * 74)
    print(title)
    print("-" * 74)


def _check(name: str, condition: Any, detail: str = "") -> bool:
    global _PASSED, _FAILED
    ok = bool(condition)
    if ok:
        _PASSED += 1
    else:
        _FAILED += 1
    tail = f"  -> {detail}" if detail else ""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{tail}")
    return ok


# ============================================================
# 二、工具：环境变量隔离 / 哨兵 db / 配置快照
# ============================================================
@contextmanager
def _envvars(**pairs: Optional[str]) -> Iterator[None]:
    """临时设置环境变量，退出时**精确恢复**（``None`` 表示删除）。"""
    saved: Dict[str, Optional[str]] = {}
    for key, value in pairs.items():
        saved[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, old in saved.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


class _StubDb:
    """哨兵 ``db``：证明**配置注入是纯组装**——不读库、不建连接、不发 IO。

    ``build_vector_store`` 只校验 ``db is not None``，随后把它原样交给后端构造函数
    （``SqlAlchemyVectorStore.__init__`` 也只是存起来）。因此本套件不需要数据库。
    """

    def __repr__(self) -> str:  # pragma: no cover - 便于调试打印
        return "<_StubDb（哨兵，不接触数据库）>"


def _config(retriever: Any) -> Tuple[Any, ...]:
    """检索器的**可观测配置快照**（用于「逐字段等价」断言）。"""
    return (
        retriever.top_k,
        retriever.min_score,
        retriever.category,
        retriever.document_id,
        retriever.dedup,
        retriever.model_name,
        type(retriever.store).__name__,
    )


class ExplodingRetriever:
    """一旦被调用就炸——用来证明「注入了 retriever 时不走组装分支」。"""

    def __init__(self) -> None:
        self.calls = 0

    async def retrieve(self, job_info: Any, topic: Any, context: Any) -> List[Any]:
        self.calls += 1
        raise AssertionError("ExplodingRetriever 不该被调用")


# ============================================================
# 三、静态检查工具（AST / 签名）
# ============================================================
def _module_ast(module: Any) -> ast.Module:
    return ast.parse(inspect.getsource(module))


def _ast_func(module: Any, func_name: str) -> Optional[ast.AST]:
    for node in _module_ast(module).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and node.name == func_name:
            return node
    return None


def _top_level_imports(module: Any) -> set:
    """模块**顶层**（不在任何函数体内）import 的模块名集合。

    只看 ``ast.parse(src).body``：函数体内的延迟导入不算——那正是本项目
    「组装器顶层保持零业务依赖」的手段，必须与顶层区分开。
    """
    found: set = set()
    for node in _module_ast(module).body:
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            found.add(node.module or "")
    return found


def _callee_name(call: ast.Call) -> Optional[str]:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _assembler_call_kwargs(node: Optional[ast.AST]) -> Optional[List[Optional[str]]]:
    """``node`` 体内对 ``VectorKnowledgeRetriever(...)`` 的**关键字**参数名列表。

    ``**merged`` 这类解包的关键字 ``arg`` 是 ``None``——保留 ``None`` 正是为了
    让「所有参数都来自 merged」与「有人硬编码了 ``min_score=0.25``」可区分。
    """
    if node is None:
        return None
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and _callee_name(sub) == "VectorKnowledgeRetriever":
            return [kw.arg for kw in sub.keywords]
    return None


def _assembler_call_positional(node: Optional[ast.AST]) -> Optional[int]:
    """``node`` 体内 ``VectorKnowledgeRetriever(...)`` 的**位置参数**个数。"""
    if node is None:
        return None
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and _callee_name(sub) == "VectorKnowledgeRetriever":
            return len(sub.args)
    return None


def _count_name(node: Optional[ast.AST], name: str) -> int:
    if node is None:
        return 0
    return sum(1 for n in ast.walk(node)
               if isinstance(n, ast.Name) and n.id == name)


def _files_containing(tokens: Tuple[str, ...], roots: Tuple[Path, ...]) -> Dict[str, List[str]]:
    """在 ``roots`` 下的 ``*.py`` 里找哪些文件**出现**了这些 token（按原文匹配）。

    .. note::
       **本函数不得把 ``tests/`` 也扫进去**——否则它会匹配到本文件里
       ``ENV_RAG_TOP_K`` 之类的字样（「守卫在源码里写出自己要找的 token」这一坑）。
       扫的是 ``services/`` 与 ``api/``，即**生产代码**。
    """
    hits: Dict[str, List[str]] = {}
    for root in roots:
        for path in sorted(root.glob("*.py")):
            text = path.read_text(encoding="utf-8")
            # 匹配**带引号的字面量**而不是裸词：``RAG_MIN_SCORE`` 是
            # ``RAG_MIN_SCORE_RATIO`` 的子串，裸词匹配会让「只读 RATIO 的文件」
            # 被误报成「也读了 MIN_SCORE」——那是**静默的错误答案**。
            found = [token for token in tokens if f'"{token}"' in text]
            if found:
                hits[path.name] = found
    return hits


# ============================================================
# [1] 解析语义
# ============================================================
def check_resolve_semantics() -> None:
    _section("[1] 解析语义（不配 = {} = 行为不变；env= 真的替代 os.environ）")

    _check("不设任何变量 → {}（默认行为逐字节不变）",
           resolve_retriever_defaults({}) == {}, str(resolve_retriever_defaults({})))
    _check("空串 / 纯空白 → {}（不产生任何键；与 VECTOR_STORE 同一约定）",
           resolve_retriever_defaults({ENV_RAG_TOP_K: "",
                                       ENV_RAG_MIN_SCORE: "   "}) == {}
           and resolve_retriever_defaults({ENV_RAG_TOP_K: "  "}) == {})
    _check("只设 RAG_TOP_K → {'top_k': 8}",
           resolve_retriever_defaults({ENV_RAG_TOP_K: "8"}) == {"top_k": 8})
    _check("只设 RAG_MIN_SCORE → {'min_score': 0.25}",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE: "0.25"}) == {"min_score": 0.25})
    _check("两者都设 → 两个键都在",
           resolve_retriever_defaults({ENV_RAG_TOP_K: "8",
                                       ENV_RAG_MIN_SCORE: "0.25"})
           == {"top_k": 8, "min_score": 0.25})
    _check("返回值只含已知键（不夹带任何其它东西）",
           set(resolve_retriever_defaults({ENV_RAG_TOP_K: "8",
                                           ENV_RAG_MIN_SCORE: "0.1"}))
           == {"top_k", "min_score"})

    # --- 相对阈值 RAG_MIN_SCORE_RATIO（任务 81）---
    _check("只设 RAG_MIN_SCORE_RATIO → {'min_score_ratio': 0.9947}",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: "0.9947"})
           == {"min_score_ratio": 0.9947})
    _check("RAG_MIN_SCORE_RATIO 空串 / 纯空白 → 不产生键（等价「不设」）",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: ""}) == {}
           and resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: "  "}) == {})
    _check("三个都设 → 三个键都在",
           resolve_retriever_defaults({ENV_RAG_TOP_K: "8",
                                       ENV_RAG_MIN_SCORE: "0.768",
                                       ENV_RAG_MIN_SCORE_RATIO: "0.9947"})
           == {"top_k": 8, "min_score": 0.768, "min_score_ratio": 0.9947})
    _check("  └ 键集合恰为 {top_k, min_score, min_score_ratio}",
           set(resolve_retriever_defaults({ENV_RAG_TOP_K: "8",
                                           ENV_RAG_MIN_SCORE: "0.1",
                                           ENV_RAG_MIN_SCORE_RATIO: "0.99"}))
           == {"top_k", "min_score", "min_score_ratio"})
    _check("min_score_ratio 保留全精度（0.994748 不被舍入）",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: "0.994748"})
           ["min_score_ratio"] == 0.994748)
    _check("min_score_ratio 接受上边界 1（含等号）",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: "1"})
           ["min_score_ratio"] == 1.0)
    _check("★ min_score_ratio 与 min_score 是**两把尺子**（0 在余弦域合法、在比例域非法）",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE: "0"})["min_score"] == 0.0
           and _raises(lambda: resolve_retriever_defaults(
               {ENV_RAG_MIN_SCORE_RATIO: "0"})))

    # --- 取值归一 ---
    _check("值两侧空白被 strip（' 8 ' → 8）",
           resolve_retriever_defaults({ENV_RAG_TOP_K: " 8 "}) == {"top_k": 8})
    _check("允许一个前导 +（'+8' → 8）",
           resolve_retriever_defaults({ENV_RAG_TOP_K: "+8"}) == {"top_k": 8})
    _check("min_score 保留全精度（0.267 不被舍入）",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE: "0.267"})["min_score"] == 0.267)
    _check("min_score 接受域内边界 0 / -1 / 1",
           [resolve_retriever_defaults({ENV_RAG_MIN_SCORE: v})["min_score"]
            for v in ("0", "-1", "1")] == [0.0, -1.0, 1.0])

    # --- env= 是否真的替代 os.environ（防「读了环境变量又假装读了入参」）---
    with _envvars(**{ENV_RAG_TOP_K: "99", ENV_RAG_MIN_SCORE: "0.9"}):
        _check("★ 显式 env={} ⇒ 完全**不读** os.environ（进程里有值也是 {}）",
               resolve_retriever_defaults({}) == {},
               str(resolve_retriever_defaults({})))
        _check("  └ 而 env=None ⇒ 读 os.environ（拿到 99 / 0.9）",
               resolve_retriever_defaults(None) == {"top_k": 99, "min_score": 0.9},
               str(resolve_retriever_defaults(None)))
        _check("  └ 局部 env 覆盖进程环境（只给 RAG_TOP_K 时 min_score 不出现）",
               resolve_retriever_defaults({ENV_RAG_TOP_K: "3"}) == {"top_k": 3})


# ============================================================
# [2] 非法值必须报错
# ============================================================
def check_invalid_values() -> None:
    _section("[2] 非法值必须报错（RetrieverConfigError ⊂ ValueError；不静默退回默认）")

    _check("守卫自检：合法值**不**报错（证明下面的循环不是在恒真地通过）",
           resolve_retriever_defaults({ENV_RAG_TOP_K: "8",
                                       ENV_RAG_MIN_SCORE: "0.25"})
           == {"top_k": 8, "min_score": 0.25})

    for raw in BAD_TOP_K:
        label = repr(raw)
        try:
            got = resolve_retriever_defaults({ENV_RAG_TOP_K: raw})
            _check(f"★ RAG_TOP_K={label} ⇒ 报错", False, f"竟然返回 {got!r}")
        except RetrieverConfigError as exc:
            _check(f"★ RAG_TOP_K={label} ⇒ RetrieverConfigError", True,
                   str(exc)[:70])
        except Exception as exc:  # noqa: BLE001
            _check(f"★ RAG_TOP_K={label} ⇒ RetrieverConfigError", False,
                   f"抛的是 {type(exc).__name__}: {exc}")

    for raw in BAD_MIN_SCORE:
        label = repr(raw)
        try:
            got = resolve_retriever_defaults({ENV_RAG_MIN_SCORE: raw})
            _check(f"★ RAG_MIN_SCORE={label} ⇒ 报错", False, f"竟然返回 {got!r}")
        except RetrieverConfigError as exc:
            _check(f"★ RAG_MIN_SCORE={label} ⇒ RetrieverConfigError", True,
                   str(exc)[:70])
        except Exception as exc:  # noqa: BLE001
            _check(f"★ RAG_MIN_SCORE={label} ⇒ RetrieverConfigError", False,
                   f"抛的是 {type(exc).__name__}: {exc}")

    _check("守卫自检：RAG_MIN_SCORE_RATIO=0.9947 合法、**不**报错",
           resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: "0.9947"})
           == {"min_score_ratio": 0.9947})

    for raw in BAD_MIN_SCORE_RATIO:
        label = repr(raw)
        try:
            got = resolve_retriever_defaults({ENV_RAG_MIN_SCORE_RATIO: raw})
            _check(f"★ RAG_MIN_SCORE_RATIO={label} ⇒ 报错", False, f"竟然返回 {got!r}")
        except RetrieverConfigError as exc:
            _check(f"★ RAG_MIN_SCORE_RATIO={label} ⇒ RetrieverConfigError", True,
                   str(exc)[:70])
        except Exception as exc:  # noqa: BLE001
            _check(f"★ RAG_MIN_SCORE_RATIO={label} ⇒ RetrieverConfigError", False,
                   f"抛的是 {type(exc).__name__}: {exc}")

    _check("  └ 百分数写法的提示会点出「请写小数」",
           "小数" in str(_capture_error(ENV_RAG_MIN_SCORE_RATIO, "99.5")))

    _check("RetrieverConfigError 是 ValueError 子类（调用方可以只 catch ValueError）",
           issubclass(RetrieverConfigError, ValueError))
    _check("★ 报错信息里带上了变量名与收到的值（可排查）",
           ENV_RAG_TOP_K in str(_capture_error(ENV_RAG_TOP_K, "x"))
           and "'x'" in str(_capture_error(ENV_RAG_TOP_K, "x")))
    _check("  └ 越界值 25 的提示会点出「想不过滤就别设本变量」",
           "不要设置" in str(_capture_error(ENV_RAG_MIN_SCORE, "25")))


def _capture_error(key: str, value: str) -> BaseException:
    try:
        resolve_retriever_defaults({key: value})
    except BaseException as exc:  # noqa: BLE001
        return exc
    raise AssertionError(f"{key}={value!r} 竟然没有报错")


def _raises(fn: Any) -> bool:
    """``fn()`` 是否抛 ``RetrieverConfigError``（其它异常一律算**没抛对**）。"""
    try:
        fn()
    except RetrieverConfigError:
        return True
    except Exception:  # noqa: BLE001
        return False
    return False


# ============================================================
# [3] 组装层：环境变量提供默认值，显式传参覆盖
# ============================================================
def check_assembler(db: Any) -> None:
    _section("[3] 组装层（不配 ⇒ 逐字段等价；配了 ⇒ 真的生效；显式传参优先）")

    embedder = default_embedder()

    with _envvars(**{ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: None}):
        base = build_vector_retriever(db)
        manual = VectorKnowledgeRetriever(embedder, SqlAlchemyVectorStore(db))
        _check("★ 不配环境变量 ⇒ 与直接构造检索器**逐字段等价**（默认行为不变）",
               _config(base) == _config(manual),
               f"{_config(base)} vs {_config(manual)}")
        _check("  └ 默认口径 = top_k 取默认值、min_score is None（与任务 69 实测一致）",
               base.top_k == DEFAULT_TOP_K and base.min_score is None,
               f"top_k={base.top_k} min_score={base.min_score}")
        _check("  └ 返回的仍是 VectorKnowledgeRetriever（接口未变）",
               isinstance(base, VectorKnowledgeRetriever))
        _check("  └ 传入 **{} 与不传等价（空映射不改变任何东西）",
               _config(build_vector_retriever(db, **{})) == _config(base))
        _check("  └ 显式 model= 仍只进向量库（未被配置注入影响）",
               build_vector_retriever(db, model="m1").store.model == "m1")

    with _envvars(**{ENV_RAG_TOP_K: "8"}):
        r = build_vector_retriever(db)
        _check("RAG_TOP_K=8 ⇒ retriever.top_k == 8（其余取默认）",
               r.top_k == 8 and r.min_score is None and r.dedup is True,
               f"top_k={r.top_k} min_score={r.min_score}")

    with _envvars(**{ENV_RAG_MIN_SCORE: "0.25"}):
        r = build_vector_retriever(db)
        _check("★ RAG_MIN_SCORE=0.25 ⇒ retriever.min_score == 0.25（本任务的核心）",
               r.min_score == 0.25 and r.top_k == DEFAULT_TOP_K,
               f"top_k={r.top_k} min_score={r.min_score}")

    with _envvars(**{ENV_RAG_TOP_K: "8", ENV_RAG_MIN_SCORE: "0.25"}):
        r = build_vector_retriever(db)
        _check("两者都设 ⇒ 两个都生效",
               r.top_k == 8 and r.min_score == 0.25, f"{_config(r)}")

        # --- 显式传参优先（R1 透传链与基准/单测依赖这条）---
        _check("★ 显式 min_score=None 覆盖环境变量 0.25（显式传参优先）",
               build_vector_retriever(db, min_score=None).min_score is None)
        _check("  └ 显式 min_score=0.5 覆盖环境变量 0.25",
               build_vector_retriever(db, min_score=0.5).min_score == 0.5)
        _check("  └ 显式 top_k=3 覆盖环境变量 8",
               build_vector_retriever(db, top_k=3).top_k == 3)
        merged = build_vector_retriever(db, top_k=3, dedup=False, category="technical")
        _check("★ 是**合并**不是整体替换：显式三项 + 未提及的 min_score 仍来自环境变量",
               merged.top_k == 3 and merged.dedup is False
               and merged.category == "technical" and merged.min_score == 0.25,
               f"{_config(merged)}")

    # --- 非法环境变量：组装器必须抛，不静默退回默认 ---
    with _envvars(**{ENV_RAG_MIN_SCORE: "25"}):
        try:
            got = build_vector_retriever(db)
            _check("★ 非法环境变量 ⇒ 组装器抛 RetrieverConfigError", False,
                   f"竟然成功构造出 min_score={got.min_score}")
        except RetrieverConfigError as exc:
            _check("★ 非法环境变量 ⇒ 组装器抛 RetrieverConfigError", True,
                   str(exc)[:70])

    # --- 报错不留副作用：修好之后立刻正常 ---
    with _envvars(**{ENV_RAG_MIN_SCORE: "0.25"}):
        _check("  └ 非法值不留下任何状态（改成合法值即恢复正常）",
               build_vector_retriever(db).min_score == 0.25)


# ============================================================
# [4] 端到端：生产组装路径（resolve_retriever）自动带上配置
# ============================================================
def check_core_wiring(db: Any) -> None:
    _section("[4] 端到端（interview_core.resolve_retriever(use_rag=True) 带上配置）")

    with _envvars(**{ENV_RAG_TOP_K: None, ENV_RAG_MIN_SCORE: None}):
        r, w = interview_core.resolve_retriever(db, None, True)
        _check("不配 ⇒ 默认口径（min_score is None、无 warning）",
               r is not None and r.min_score is None and w == [],
               f"min_score={getattr(r, 'min_score', None)} warns={w}")

    with _envvars(**{ENV_RAG_MIN_SCORE: "0.25"}):
        r, w = interview_core.resolve_retriever(db, None, True)
        _check("★ 配了 RAG_MIN_SCORE ⇒ **生产组装路径**上 min_score 真的生效",
               isinstance(r, VectorKnowledgeRetriever) and r.min_score == 0.25
               and w == [], f"min_score={getattr(r, 'min_score', None)} warns={w}")
        r2, w2 = interview_core.resolve_retriever(
            db, None, True, retriever_kwargs={"min_score": None})
        _check("  └ 显式 retriever_kwargs 仍**优先于**环境变量（R1 透传链不受影响）",
               r2.min_score is None and w2 == [],
               f"min_score={getattr(r2, 'min_score', None)}")
        r3, _w3 = interview_core.resolve_retriever(db, None, True, retriever_kwargs={})
        _check("  └ 空映射 retriever_kwargs={} 与 None 等价（环境变量照样生效）",
               r3.min_score == 0.25, f"min_score={getattr(r3, 'min_score', None)}")

    with _envvars(**{ENV_RAG_TOP_K: "8"}):
        r, w = interview_core.resolve_retriever(db, None, True)
        _check("★ RAG_TOP_K=8 同样在生产路径上生效",
               r.top_k == 8 and r.min_score is None and w == [],
               f"top_k={getattr(r, 'top_k', None)}")

    with _envvars(**{ENV_RAG_MIN_SCORE: "25"}):
        r, w = interview_core.resolve_retriever(db, None, True)
        _check("★ 非法配置 ⇒ 与既有 RAG 失败语义一致：静默降级「无知识」+ warning",
               r is None and w == [interview_core.WARNING_KNOWLEDGE_FAILED],
               f"r={r!r} warns={w}")

    with _envvars(**{ENV_RAG_MIN_SCORE: "25"}):
        probe = ExplodingRetriever()
        r, w = interview_core.resolve_retriever(db, probe, True)
        _check("★ 注入 retriever ⇒ 完全不读环境变量（非法值也不会降级/报错）",
               r is probe and w == [] and probe.calls == 0)

        r, w = interview_core.resolve_retriever(db, None, False)
        _check("★ use_rag=False ⇒ 仍返回 None（配置不会悄悄打开 RAG）",
               r is None and w == [])


# ============================================================
# [5] 不越层 / 接口冻结
# ============================================================
def check_boundaries() -> None:
    _section("[5] 不越层 / 接口冻结（AST + 签名守卫）")

    # --- 5.1 组装器顶层仍只有标准库 ---
    top = _top_level_imports(knowledge_rag)
    _check("★ knowledge_rag 顶层 import 恰好是允许集合（无 sqlalchemy / models / database）",
           top == ALLOWED_TOP_LEVEL_IMPORTS,
           f"{sorted(top)}")
    _check("  └ 顶层确实没有 services.* （协作者一律函数内延迟导入）",
           not any(name.startswith("services") for name in top))

    # --- 5.2 RAG_* 三个变量名全项目只有组装器在读 ---
    # token 运行时取自模块常量，不在源码里硬写字面量（避免「守卫匹配到自己」）。
    tokens = (knowledge_rag.ENV_RAG_TOP_K, knowledge_rag.ENV_RAG_MIN_SCORE,
              knowledge_rag.ENV_RAG_MIN_SCORE_RATIO)
    hits = _files_containing(tokens, (BACKEND_DIR / "services", BACKEND_DIR / "api"))
    _check("★ RAG_TOP_K / RAG_MIN_SCORE / RAG_MIN_SCORE_RATIO 在生产代码里"
           "**只**被 knowledge_rag.py 读",
           set(hits) == {"knowledge_rag.py"}, str(sorted(hits)))
    _check("  └ 守卫自检：扫描器能发现「有」（非空命中 ⇒ 不是在恒真地通过）",
           hits.get("knowledge_rag.py") == list(tokens), str(hits.get("knowledge_rag.py")))
    _check("  └ 检索器 / 向量层 / 面试层都不读它们（配置只在组装层收口）",
           not any(name in hits for name in (
               "vector_knowledge_retriever.py", "vector_store.py",
               "vector_store_sql.py", "vector_store_chroma.py",
               "interview_core.py", "interview_service.py", "interview.py")),
           str(sorted(hits)))

    # --- 5.3 VectorStore 接口冻结（一行都不该动）---
    _check("★ VectorStore 抽象方法集未变",
           set(VectorStore.__abstractmethods__) == FROZEN_VECTOR_STORE_METHODS,
           str(sorted(VectorStore.__abstractmethods__)))
    search_params = inspect.signature(VectorStore.search).parameters
    _check("★ VectorStore.search 参数表未变（含 keyword-only 的 5 个）",
           list(search_params) == FROZEN_SEARCH_PARAMS, str(list(search_params)))
    _check("  └ min_score 仍是 keyword-only、默认 None",
           search_params["min_score"].kind is inspect.Parameter.KEYWORD_ONLY
           and search_params["min_score"].default is None)

    # --- 5.4 Retriever 构造签名冻结 ---
    init_params = inspect.signature(VectorKnowledgeRetriever.__init__).parameters
    _check("★ VectorKnowledgeRetriever.__init__ 参数表 = 冻结表（任务 81 只在**末尾**"
           "追加 min_score_ratio）",
           list(init_params) == FROZEN_RETRIEVER_PARAMS, str(list(init_params)))
    _check("  └ 既有 9 个参数的**顺序与默认值**一个未变（老调用方零影响）",
           FROZEN_RETRIEVER_PARAMS[:9]
           == ["self", "embedder", "store", "top_k", "min_score",
               "category", "document_id", "model", "dedup"]
           and init_params["category"].default is None
           and init_params["document_id"].default is None
           and init_params["model"].default is None
           and init_params["dedup"].default is True)
    _check("  └ top_k / min_score 仍是 keyword-only、默认 5 / None",
           init_params["top_k"].kind is inspect.Parameter.KEYWORD_ONLY
           and init_params["top_k"].default == DEFAULT_TOP_K
           and init_params["min_score"].kind is inspect.Parameter.KEYWORD_ONLY
           and init_params["min_score"].default is None)
    _check("  └ min_score_ratio 也是 keyword-only、默认 None（不配 ⇒ 行为逐字节不变）",
           init_params["min_score_ratio"].kind is inspect.Parameter.KEYWORD_ONLY
           and init_params["min_score_ratio"].default is None)

    # --- 5.5 组装器没有把「按分数过滤」搬进来 ---
    body = _ast_func(knowledge_rag, "build_vector_retriever")
    _check("取到 build_vector_retriever 的 AST 节点", body is not None)
    keywords = _assembler_call_kwargs(body)
    positional = _assembler_call_positional(body)
    _check("★ 组装器构造检索器时**没有硬编码任何关键字参数**（除 **merged 解包）",
           keywords == [None] and positional == 2,
           f"keywords={keywords} positional={positional}")
    _check("  └ 守卫自检：同一工具对「硬编码 min_score」的调用返回 ['min_score']（能区分有/无）",
           _assembler_call_kwargs(
               ast.parse("VectorKnowledgeRetriever(embedder, store, min_score=0.25)")
               .body[0].value) == ["min_score"],
           str(_assembler_call_kwargs(
               ast.parse("VectorKnowledgeRetriever(embedder, store, min_score=0.25)")
               .body[0].value)))
    _check("★ 组装器体内**不自己过滤/截断**结果（过滤仍由 VectorStore 承担）",
           not ({_callee_name(n) for n in ast.walk(body) if isinstance(n, ast.Call)}
                & {"filter", "sorted"}),
           str(sorted({_callee_name(n) for n in ast.walk(body)
                       if isinstance(n, ast.Call)})))
    _check("  └ 配置注入点唯一（体内恰好调用一次 resolve_retriever_defaults）",
           _count_name(body, "resolve_retriever_defaults") == 1,
           str(_count_name(body, "resolve_retriever_defaults")))
    _check("  └ 且体内没有对检索结果的删除类调用（del / pop / remove）",
           not ({_callee_name(n) for n in ast.walk(body) if isinstance(n, ast.Call)}
                & {"pop", "remove"})
           and not any(isinstance(n, ast.Delete) for n in ast.walk(body)))


# ============================================================
# [6] 相对阈值 α（min_score_ratio）的行为
# ============================================================
class _FakeEmbedder:
    """最小 embedder：只要能 ``await embed(text) -> List[float]``。"""

    name = "fake"
    dimension = 3

    async def embed(self, text: str) -> List[float]:
        return [1.0, 0.0, 0.0]


class _FakeStore:
    """最小 store：**复刻 ``select_matches`` 的顺序**（排序 → ``min_score`` → ``top_k``），
    并记录每次收到的 ``min_score``（用来证明 α 没有把绝对下限挤掉）。"""

    def __init__(self, matches: List[VectorMatch]) -> None:
        self.matches = matches
        self.seen_min_score: List[Any] = []

    async def search(self, query_vector: Any, *, top_k: int = DEFAULT_TOP_K,
                     model: Any = None, document_id: Any = None,
                     category: Any = None, min_score: Any = None) -> List[VectorMatch]:
        self.seen_min_score.append(min_score)
        rows = [m for m in self.matches
                if min_score is None or m.score >= min_score]
        rows.sort(key=lambda m: -m.score)
        return rows[:top_k]


def _matches(*scores: float) -> List[VectorMatch]:
    return [
        VectorMatch(chunk_id=index, document_id=1, content=f"正文-{index}",
                    metadata={"score": score}, model="fake", score=score)
        for index, score in enumerate(scores)
    ]


def _scores_of(chunks: List[Any]) -> List[float]:
    return [float(chunk.metadata["score"]) for chunk in chunks]


def check_ratio_filter() -> None:
    _section("[6] 相对阈值 α：score >= α × top1（与 min_score 是 AND）")

    import asyncio

    def _build(scores: Tuple[float, ...], **kwargs: Any) -> Tuple[Any, _FakeStore]:
        store = _FakeStore(_matches(*scores))
        retriever = VectorKnowledgeRetriever(_FakeEmbedder(), store, **kwargs)
        return retriever, store

    def _retrieve(retriever: Any) -> List[Any]:
        return asyncio.run(retriever.retrieve(None, "topic", None))

    scores = (0.80, 0.79, 0.70, 0.60)

    base, _ = _build(scores)
    _check("不配 α ⇒ min_score_ratio is None（默认关闭）",
           base.min_score_ratio is None, repr(base.min_score_ratio))
    _check("  └ 不配 α ⇒ 结果原样返回（4 条全在，行为逐字节不变）",
           _scores_of(_retrieve(base)) == [0.80, 0.79, 0.70, 0.60],
           str(_scores_of(_retrieve(base))))

    r98, _ = _build(scores, min_score_ratio=0.98)
    kept = _retrieve(r98)
    _check("★ α=0.98、top1=0.80 ⇒ 阈值 0.784 ⇒ 保留 0.80 / 0.79（2 条）",
           _scores_of(kept) == [0.80, 0.79], str(_scores_of(kept)))

    r99, _ = _build(scores, min_score_ratio=0.99)
    kept = _retrieve(r99)
    _check("★ α=0.99 ⇒ 阈值 0.792 ⇒ 只留 top1 自己（1 条）",
           _scores_of(kept) == [0.80], str(_scores_of(kept)))

    r100, _ = _build(scores, min_score_ratio=1.0)
    _check("α=1.0（上边界）⇒ 只留 top1，且不报错",
           _scores_of(_retrieve(r100)) == [0.80], str(_scores_of(_retrieve(r100))))

    r99b, _ = _build(scores, min_score_ratio=0.999)
    _check("★ 边界含等号：α×top1 == 0.7992 时 0.7992 也保留（用 >= 而非 >）",
           _scores_of(_retrieve(r99b)) == [0.80], str(_scores_of(_retrieve(r99b))))

    # --- 与 min_score 的 AND 关系 ---
    both, store = _build(scores, min_score=0.795, min_score_ratio=0.98)
    kept = _retrieve(both)
    _check("★ min_score=0.795 且 α=0.98 ⇒ 两个条件都生效（只剩 0.80）",
           _scores_of(kept) == [0.80], str(_scores_of(kept)))
    _check("  └ α **没有**把绝对下限挤掉：store 实际收到 min_score=0.795",
           store.seen_min_score == [0.795], str(store.seen_min_score))

    # --- 空结果 / 单条 ---
    empty, _ = _build((), min_score_ratio=0.98)
    _check("空结果 + α ⇒ 返回 []（不报错、不除零）",
           _retrieve(empty) == [], str(_retrieve(empty)))
    single, _ = _build((0.70,), min_score_ratio=0.90)
    _check("单条 + α ⇒ 恒保留（top1 自己必然满足 α×top1 <= top1）",
           _scores_of(_retrieve(single)) == [0.70], str(_scores_of(_retrieve(single))))

    # --- 构造期校验（非法 α 必须报错，不得静默放行）---
    bad = (0, 0.0, -0.5, 1.5, 99.5, float("nan"), float("inf"), True, "0.9")
    for value in bad:
        try:
            VectorKnowledgeRetriever(_FakeEmbedder(), _FakeStore([]),
                                     min_score_ratio=value)
            _check(f"★ min_score_ratio={value!r} ⇒ 构造期报错", False, "竟然构造成功")
        except RetrieverConfigError as exc:
            _check(f"★ min_score_ratio={value!r} ⇒ RetrieverConfigError", True,
                   str(exc)[:60])
        except Exception as exc:  # noqa: BLE001
            _check(f"★ min_score_ratio={value!r} ⇒ RetrieverConfigError", False,
                   f"抛的是 {type(exc).__name__}: {exc}")
    _check("  └ 守卫自检：合法 α=0.9947 构造成功（证明上面的循环不是在恒真地通过）",
           VectorKnowledgeRetriever(_FakeEmbedder(), _FakeStore([]),
                                    min_score_ratio=0.9947).min_score_ratio == 0.9947)

    # --- 组装链：环境变量 → 组装器 → 检索器 ---
    _check("★ RAG_MIN_SCORE_RATIO=0.9947 ⇒ 组装出的检索器 min_score_ratio == 0.9947",
           _with_env(**{ENV_RAG_MIN_SCORE_RATIO: "0.9947"},
                     call=lambda: build_vector_retriever(
                         _StubDb()).min_score_ratio) == 0.9947)
    _check("  └ 显式传参优先于环境变量",
           _with_env(**{ENV_RAG_MIN_SCORE_RATIO: "0.5"},
                     call=lambda: build_vector_retriever(
                         _StubDb(), min_score_ratio=0.99).min_score_ratio) == 0.99)


def _with_env(call: Any, **pairs: str) -> Any:
    """在临时环境变量下执行 ``call()``（用完即恢复）。"""
    saved = {key: os.environ.get(key) for key in pairs}
    os.environ.update(pairs)
    try:
        return call()
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


# ============================================================
# 主流程
# ============================================================
def run() -> bool:
    # 环境干净性：本套件用 env= 注入，但 [1] 有一条会读 os.environ，
    # 因此先把这三个变量从进程环境里摘掉（并在结束时提示）。
    ambient = [k for k in (ENV_RAG_TOP_K, ENV_RAG_MIN_SCORE,
                           ENV_RAG_MIN_SCORE_RATIO) if k in os.environ]
    for key in ambient:
        os.environ.pop(key, None)
    if ambient:
        print(f"[提示] 已从进程环境移除 {ambient}（本套件自行注入，避免干扰）")

    check_resolve_semantics()
    check_invalid_values()
    check_assembler(_StubDb())
    check_core_wiring(_StubDb())
    check_boundaries()
    check_ratio_filter()

    print("\n" + "=" * 74)
    print(f"结果: 通过 {_PASSED} 项，失败 {_FAILED} 项")
    print("=" * 74)
    return _FAILED == 0


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
