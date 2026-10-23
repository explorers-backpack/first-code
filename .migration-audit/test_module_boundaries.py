# -*- coding: utf-8 -*-
"""模块化单体边界守卫 · 自检（**棘轮式**）

要防的那件事
------------
「模块化单体」的价值全在**依赖方向**上。一旦方向破了，目录长得再整齐也只是
把意大利面切成了五盘。而方向是最容易被无声破坏的东西：

* 某个路由文件顺手 ``db.execute(select(...))`` —— 编译、测试、功能全都正常，
  只是「路由层只做协议转换」这条约定没了；
* 某个 service 顺手 ``raise HTTPException(404)`` —— 行为正确，但领域层从此
  绑死在 FastAPI 上，换 CLI / 定时任务 / 消息消费都要拖一个 web 框架；
* 某个 ``models`` 顺手 import 了 ``services`` —— 环出现了，import 顺序开始玄学。

这些**没有任何现有测试会红**，所以必须单独建守卫。

为什么是「棘轮」而不是「一刀切」
--------------------------------
现状里已经有 4 处领域层 ``from fastapi import HTTPException``（其中 2 处在
**FROZEN 指纹**锁定的文件里，改一行整个回归就红）。如果一上来就要求「零违规」，
唯一的出路是**大规模重写**——而这恰恰是本轮重构明令禁止的（「不要一次性大规模
移动文件导致难以定位回归问题」）。

所以 R7 / R8 用**棘轮（ratchet）**：

* 每条规则带一个 ``KNOWN_ALLOWED`` 白名单，每条都写清**为什么允许**、
  **什么时候清掉**；
* 出现**新**违规 ⇒ 红（防止边修边破）；
* 白名单里某条**已经不再违规却没删** ⇒ **也红**（强制名单只能变小）。

第二条是本守卫的核心。只写「新违规变红」的话，白名单会变成永久的垃圾桶：
修好了也没人删，半年后没人知道这些条目还有没有意义。让「已修复但没删名单」
也失败，白名单就成了一份**待办清单**，只会越来越短。

规则清单（R1–R11）
------------------
====  ==================================================================
R1    严格单向分层：低层不得依赖高层（``models`` < ``utils`` < ``services`` < ``api``）
R2    ``api/*`` 不得被反向 import（除 ``main`` 装配层）
R3    service 层不得 import ``api.*``（R2 的细化，独立报告便于定位）
R4    ``models`` / ``schemas`` 不得 import 任何上层
R5    AI 适配层（``modules.ai.*``）不得 import 路由
R6    硬环 = 0（顶层互相 import；软环走白名单）
R7    路由层不写 SQL / 不 import SQLAlchemy
R8    领域层不 import FastAPI 的 HTTP 对象
R9    ``modules.*.service`` 完全禁止 import ``fastapi``（新模块零包袱）
R10   ``modules.*.api`` 不得 import ``models``（路由层不碰 ORM）
R11   ``modules.*`` 结构完整 + ``main.py`` 只做装配
====  ==================================================================

★ 判据全部走 **AST**，绝不用裸子串。这一条已经踩过多次：
``"_ticket_store"`` 是 ``ws_ticket_store`` 的子串；``"text("`` 命中
``send_text(``；``"delete("`` 命中 ``@router.delete(``。子串判据的假阳性会让
守卫变成噪音，最后所有人都学会无视它。

运行：``python backend/tests/test_module_boundaries.py``
退出码 0 = 全绿；1 = 有失败项。
"""

import ast
import pathlib
import sys
from typing import Dict, Iterable, List, Set, Tuple

_BACKEND = pathlib.Path(__file__).resolve().parents[1]

#: 盘点范围之外（测试/脚本自成体系，prompts 是纯文本模板）
EXCLUDED_DIRS = {"tests", "scripts", "__pycache__", "instance", "prompts"}

#: 不带包的单文件内部模块
ROOT_MODULES = {"main", "database", "deps", "security"}

#: 顶层包（其下模块一律视为内部依赖）
TOP_PACKAGES = {"api", "services", "schemas", "models", "utils", "modules"}

#: 层号（越大越靠上）。``main`` 与 ``modules.X``（业务模块门面）是装配层，豁免 R1。
_LAYER = {
    "models": 0, "schemas": 0, "database": 0,
    "utils": 1, "security": 1, "deps": 1,
    "services": 2, "modules": 2,
    # ``common`` 与 services 同层：``common.health`` 要**聚合**各业务层的运行状态
    # （embedding / 向量后端 / 数字人 / AI 模式），本质上是个横向汇总点。
    # 若把它压到 L0/L1，这些聚合边会被判成「低层依赖高层」，是判据错而非代码错。
    "common": 2,
    "api": 3,
}
_ASSEMBLY_LAYER = 9

#: 领域层判定时用到的 FastAPI HTTP 对象符号
HTTP_SYMBOLS = {
    "Request", "Response", "WebSocket", "HTTPException",
    "JSONResponse", "StreamingResponse", "FileResponse", "BackgroundTasks",
    "APIRouter", "Depends",
}

#: SQL 构造器（``select(...)`` / ``text(...)`` 这类**裸名字**调用）
SQL_NAME_CALLS = {
    "select", "insert", "update", "delete", "text", "literal_column",
    "and_", "or_", "func", "exists", "union", "case",
}

#: 会话 / 连接上的方法：出现即「在跑查询或改数据」
_SESSION_METHODS = {
    "execute", "scalars", "scalar", "stream",
    "commit", "flush", "refresh", "rollback",
    "add", "add_all", "merge",
}

#: 这些方法名在**路由器**上也会出现（``@router.delete``），必须限定调用者
_AMBIGUOUS_METHODS = {"delete"}

#: 数据库会话的常见变量名
_SESSION_NAMES = {"db", "session", "conn", "sess"}

#: 允许 ``sqlalchemy.select(...)`` 这种点号写法的模块别名
_SQL_ALIASES = {"sqlalchemy", "sa"}

#: ``@app.<method>`` 里的方法名（用于在 main.py 中定位路由函数）
_APP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "websocket"}

# ============================================================
# ★★ 棘轮白名单（只能变小，不能变大）
# ============================================================
#: R7：允许在路由层出现 SQL 的位置
#: 键 = 模块名（``api.x``）或 ``main:<函数名>``（``main.py`` 里 ``@app.*`` 装饰的函数）
R7_KNOWN_ALLOWED: Dict[str, str] = {
    "api.interview_ws": (
        "WS 握手阶段要用 ticket 反查 UserSession / User，而 ticket 消费必须在 "
        "accept() 之前完成（先 accept 再校验会短暂放行未鉴权连接）。这段"
        "「协议层鉴权」目前只能内联在 ws 端点里。"
        "清掉时机：P5 抽 modules/interview/api_ws.py 时，把查库下沉到 "
        "ws_ticket_store.consume(ticket, session_id) 的返回值里。"
    ),
    "main:get_user_logs": (
        "管理员审计日志。main.py 的 admin 段还没迁出（P4）。"
        "清掉时机：P4 把 /api/admin/* 拆成 modules/admin/api.py + service.py。"
    ),
    "main:add_job": (
        "管理员新增岗位。清掉时机：同上（P4）。岗位写入本就该落在 career 模块的 "
        "repository 里，而不是路由函数里 db.add()。"
    ),
    "main:kick_user_sessions": (
        "管理员强制下线。★ 这段同时写 UserSession.update + ws_revocation 广播，"
        "是「会话安全」的写入口，P4 迁出时必须连同撤销广播一起下沉到 "
        "modules/auth/service.py，否则多实例失效。清掉时机：P4。"
    ),
}

#: R8：允许在领域层 import 的 (模块, 符号)
R8_KNOWN_ALLOWED: Dict[Tuple[str, str], str] = {
    ("services.interview_context", "HTTPException"): (
        "★★ FROZEN_SOURCES 指纹锁定（test_agent_full_input.py），改一行整个回归就红。"
        "清掉时机：需与指纹守卫同步解冻，属 P5 范围，不在本轮。"
    ),
    ("services.interview_core", "HTTPException"): (
        "★★ FROZEN_SOURCES 指纹锁定，同上。这是四层里最不该有 HTTP 的一层，"
        "但解冻成本最高（另两个套件也锁了它）。"
    ),
    ("services.interview_service", "HTTPException"): (
        "非冻结。参照 modules/auth 的做法：service 抛领域异常，api 层统一转换。"
        "清掉时机：P5 迁 interview 模块时一并处理。"
    ),
    ("services.knowledge_document_service", "HTTPException"): (
        "非冻结。清掉时机：P6 迁 knowledge 模块时一并处理。"
    ),
}

#: R1：已知的**反向依赖**（低层 -> 高层），必须逐条说明
#:
#: 键 = ``(源模块, 目标模块)``。这些是**真违规**，只是暂时清不掉。
R1_KNOWN_ALLOWED: Dict[Tuple[str, str], str] = {
    ("services.interview_agent", "main"): (
        "★★ FROZEN_SOURCES 指纹锁定。函数内 `from main import spark_api`（第 239 行），"
        "用延迟导入绕开了环，但方向确实是 services(L2) -> main(L9)。"
        "清掉时机：P5 迁 interview 时，把 LLM 门面改成参数注入（参照 modules/career/ports.py），"
        "需与指纹守卫同步解冻。"
    ),
    ("services.interview_planner", "main"): (
        "★★ FROZEN_SOURCES 指纹锁定，同上（第 568 行）。"
    ),
}

#: R6：软环数量上限（**棘轮：只许调小**）。
#:
#: 为什么不逐个登记软环：软环 = 「A 顶层 import B，B 在函数内 import A」，
#: 这是 Python 里最常见的解环手法，本项目有二十多个。逐个精确匹配（frozenset）
#: 会让**任何**新模块引入的新软环都变红，而其中绝大多数是无害的；
#: 反过来，一旦为了让它变绿而不断往白名单里加，名单就失去意义。
#:
#: 所以只钉**数量**：新增软环会让数字变大 ⇒ 红，逼人看一眼「这个环是不是必要的」。
#: 修掉一个环后要把这个常量改小，否则棘轮失效。
R6_MAX_SOFT_CYCLES = 23

#: 已迁移模块：模块名 -> (必须存在的文件, main.py 里不得再出现的实现标记, 挂载变量名)
#:
#: ★ 这是 R11 的**棘轮**：每迁完一个模块就往这里加一条。禁止清单只增不减，
#: 意味着「搬走的实现不许以任何形式搬回 main.py」。
_MIGRATED_MODULES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...], str]] = {
    "auth": (
        ("__init__.py", "api.py", "service.py", "schemas.py"),
        (
            "_hash_password", "def register(", "def login(",
            "throttle_check", "throttle_record", "throttle_reset",
        ),
        "auth_router",
    ),
    "career": (
        ("__init__.py", "api.py", "service.py", "schemas.py", "prompts.py", "ports.py"),
        (
            "def get_all_jobs(", "def filter_jobs_by_keywords(", "def clean_jobs_to_text(",
            "RAG_PROMPT = ", "EMPTY_PROMPT = ", "RESUME_ANALYSIS_PROMPT = ",
            "def _decode_resume(", "def analyze_resume(", "def parse_resume(",
        ),
        "career_router",
    ),
}


# ============================================================
# AST 工具（不 import 任何项目模块）
# ============================================================
def _iter_modules() -> List[pathlib.Path]:
    """``backend/`` 下的业务 .py 文件（排除 tests / scripts / prompts）。"""
    out: List[pathlib.Path] = []
    for path in sorted(_BACKEND.rglob("*.py")):
        parts = set(path.relative_to(_BACKEND).parts[:-1])
        if parts & EXCLUDED_DIRS:
            continue
        out.append(path)
    return out


def _module_name(path: pathlib.Path) -> str:
    rel = path.relative_to(_BACKEND).with_suffix("")
    parts = list(rel.parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else "<root>"


def _is_internal(dotted: str) -> bool:
    if dotted in ROOT_MODULES:
        return True
    return dotted.split(".", 1)[0] in TOP_PACKAGES


def _collect_imports(nodes: Iterable[ast.AST], known: Set[str]) -> List[Tuple[str, int]]:
    """从一组 AST 节点里抽出内部 import，返回 ``[(模块名, 行号)]``。

    ★ ``from pkg import sub`` 必须**展开**成 ``pkg.sub``：只记 ``pkg`` 的话，
    ``services.interview_service`` 这类模块的「被依赖数」会恒为 0，环检测也
    会漏掉经由 ``from services import X`` 建立的边。
    ★ 但只有 ``pkg.sub`` **确实是模块**时才展开。``from modules.ai.llm import
    LlmProvider`` 里的 ``modules.ai.llm.LlmProvider`` 是**符号**不是模块，
    按内部前缀放行会把它当模块，白名单和环都会出现幽灵条目。
    """
    found: List[Tuple[str, int]] = []
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_internal(alias.name):
                    found.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                # 相对导入：本项目统一绝对导入，出现即记录（供环检测还原）
                found.append(("." * node.level + (node.module or ""), node.lineno))
                continue
            module = node.module or ""
            for alias in node.names:
                candidate = f"{module}.{alias.name}" if module else alias.name
                if candidate in known:
                    found.append((candidate, node.lineno))
            if module and (module in known or _is_internal(module)):
                found.append((module, node.lineno))
    return found


class Unit:
    """一个模块的解析结果。"""

    __slots__ = ("name", "path", "top", "lazy", "tree")

    def __init__(self, name: str, path: pathlib.Path, top: List[Tuple[str, int]],
                 lazy: List[Tuple[str, int]], tree: ast.Module) -> None:
        self.name = name
        self.path = path
        self.top = top
        self.lazy = lazy
        self.tree = tree

    @property
    def all_imports(self) -> List[Tuple[str, int]]:
        return self.top + self.lazy


def _load_units() -> Dict[str, Unit]:
    paths = _iter_modules()
    known = {_module_name(p) for p in paths}
    units: Dict[str, Unit] = {}
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        top = _collect_imports(tree.body, known)
        # ★★ 延迟导入必须用 `ast.walk(tree)` 收集，**不能**只看函数体第一层。
        #   踩过的坑：`services/interview_agent.py:239` 的
        #   `from main import spark_api` 包在 `try:` 里面，
        #   早期版本用 `for func: _collect_imports(func.body)` —— 只遍历函数体的
        #   **直接子语句**，于是这条 `services -> main` 的反向依赖被漏掉，
        #   R1 报「层方向违规 = 0」，而实际上是 3 条。
        #   判据漏检比判据误报危险得多：误报会被无视，漏检会让人以为没问题。
        all_imports = _collect_imports(list(ast.walk(tree)), known)
        lazy = [item for item in all_imports if item not in top]
        units[_module_name(path)] = Unit(_module_name(path), path, top, lazy, tree)
    return units


# ============================================================
# 通用
# ============================================================
_FAILURES: List[str] = []


def _check(name: str, cond: bool, detail: str = "") -> bool:
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (f"\n         -> {detail}" if detail and not cond else ""))
    if not cond:
        _FAILURES.append(name)
    return cond


def _section(title: str) -> None:
    print("\n" + title)


#: 包门面（``modules.X`` 这种两段名）里，**本身是能力层**而非业务模块门面的
#:
#: ``modules.auth`` / ``modules.career`` 的 ``__init__.py`` 只做一件事：
#: ``from modules.X.api import router``（门面）。它没有自己的逻辑，算装配层。
#:
#: ``modules.ai`` 不一样：它的 ``__init__.py`` 导出了 12 个能力符号
#: （``LlmProvider`` / ``ai_call_scope`` / ``build_legacy_spark_facade`` …），
#: 是**可以被其他业务模块依赖的能力包**。把它算成装配层会让
#: ``modules.career.service -> modules.ai`` 这条完全正常的边被判成「L2 依赖 L9」。
_CAPABILITY_PACKAGES = {"ai"}


def _layer_of(name: str) -> int:
    parts = name.split(".")
    head = parts[0]
    if head == "main":
        return _ASSEMBLY_LAYER
    if head == "modules":
        if len(parts) < 3:
            # 两段名：能力包算 L2，业务模块门面算装配层
            if len(parts) == 2 and parts[1] in _CAPABILITY_PACKAGES:
                return 2
            return _ASSEMBLY_LAYER
        return {"schemas": 0, "service": 2, "api": 3}.get(parts[2], 2)
    return _LAYER.get(head, 0)


def _is_domain(name: str) -> bool:
    """领域层 = ``services.*`` 或 ``modules.*.service``。"""
    if name.startswith("services.") or name == "services":
        return True
    parts = name.split(".")
    return len(parts) == 3 and parts[0] == "modules" and parts[2] == "service"


def _is_router(name: str) -> bool:
    """路由层 = ``api.*`` 或 ``modules.*.api``。"""
    if name == "api" or name.startswith("api."):
        return True
    parts = name.split(".")
    return len(parts) == 3 and parts[0] == "modules" and parts[2] == "api"


def _same_module_package(a: str, b: str) -> bool:
    """``modules.X`` 与 ``modules.X.<file>`` 是否属于同一个业务模块。

    同一个模块内部的互相引用（``modules.auth.api`` → ``modules.auth.service``）
    **不参与 R1 层判定**：包内聚优先，具体层由文件名决定，而「包门面」本身
    不是一层。若把包门面算成装配层，``from modules.auth import service`` 这种
    完全正常的同包引用就会被误判成「L3 依赖 L9」。

    ★ 但 R6 的环检测**不豁免**：同包内顶层互相 import 一样是硬环，
    只是修法不同（改成 ``import modules.auth.service as service``）。
    """
    pa, pb = a.split("."), b.split(".")
    return (len(pa) >= 2 and len(pb) >= 2
            and pa[0] == "modules" and pb[0] == "modules" and pa[1] == pb[1])


def _sql_hits_nodes(nodes: Iterable[ast.AST]) -> List[str]:
    """在一组 AST 节点里找 SQL 构造 / 执行痕迹（**AST 判据**）。

    命中三种形式，各自对应一种真实写法：

    * ``select(...)`` / ``text(...)`` —— ``ast.Name`` 调用（裸名字）
    * ``db.execute(...)`` —— ``ast.Attribute`` 且 attr 是 ``execute`` / ``scalars`` / ``scalar``
    * ``from sqlalchemy import select`` —— 从 **``sqlalchemy`` 顶层**导入 SQL 构造器

    ★ 刻意**不**命中下面这些，它们是「ORM 的类型与异常」，不是「写 SQL」：

    * ``from sqlalchemy.ext.asyncio import AsyncSession`` —— 路由层 ``db: AsyncSession
      = Depends(get_db)`` 的类型注解，删了会让整个项目失去类型检查；
    * ``from sqlalchemy.exc import IntegrityError`` —— 异常类型。

    把这两类算成违规，等于要求路由层「不许给数据库会话加类型注解」，是
    **判据错**而不是代码错。守卫一旦开始误报，大家就会学会无视它 —— 那比没有
    守卫更糟。

    ★ 也刻意不用 ``"delete("`` 这类**裸子串**：``@router.delete(...)`` 是
    ``ast.Attribute``（value 是 ``router``），与 SQL 的 ``delete()`` 完全不同；
    同理 ``send_text(`` 会被 ``"text("`` 命中。子串判据的假阳性已经踩过多次。
    """
    hits: List[str] = []
    for node in nodes:
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in SQL_NAME_CALLS:
                hits.append(f"L{node.lineno} {func.id}(...)")
            elif isinstance(func, ast.Attribute):
                if func.attr in _SESSION_METHODS:
                    hits.append(f"L{node.lineno} .{func.attr}(...)")
                elif (func.attr in _AMBIGUOUS_METHODS
                      and isinstance(func.value, ast.Name)
                      and func.value.id in _SESSION_NAMES):
                    # ``db.delete(x)`` 是写操作，但 ``@router.delete(...)``
                    # （value 是 ``router``）不是。必须靠**调用者**区分。
                    hits.append(f"L{node.lineno} .{func.attr}(...)")
                elif (func.attr in SQL_NAME_CALLS
                      and isinstance(func.value, ast.Name)
                      and func.value.id in _SQL_ALIASES):
                    # ``sqlalchemy.select(...)`` 点号写法。★ 必须限定 value 是
                    # sqlalchemy 别名，否则 ``@router.delete(...)``（value=router）
                    # 会被当成 SQL。
                    hits.append(f"L{node.lineno} {func.value.id}.{func.attr}(...)")
        elif isinstance(node, ast.ImportFrom):
            # 只有 `from sqlalchemy import <构造器>` 才算；`sqlalchemy.ext.*` /
            # `sqlalchemy.exc` 是类型与异常，放行。
            if (node.module or "") == "sqlalchemy":
                for alias in node.names:
                    if alias.name in SQL_NAME_CALLS:
                        hits.append(f"L{node.lineno} from sqlalchemy import {alias.name}")
        # ★ `import sqlalchemy` 本身不算：可能只用 `sqlalchemy.types`。
        #   真写 SQL 一定会出现构造器调用，由上面两条捕获。
    return hits


def _sql_hits(unit: Unit) -> List[str]:
    return _sql_hits_nodes(ast.walk(unit.tree))


def _app_routes(tree: ast.Module) -> List[Tuple[str, ast.AST]]:
    """``main.py`` 里被 ``@app.<method>(...)`` 装饰的函数：``[(函数名, 函数节点)]``。

    ★ 为什么要单独抓这个：``main.py`` 是**装配层**，里面既有路由函数，也有
    普通业务函数（``get_all_jobs`` / ``embedding_status``）。如果对整个模块做
    SQL 检查，白名单粒度就是「整个 main.py」—— 迁走一个端点也无法收紧，
    棘轮就失效了。按**装饰器**定位路由函数，白名单才能精确到
    ``main:get_user_logs`` 这种粒度，迁一个删一条。
    """
    out: List[Tuple[str, ast.AST]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute)
                    and dec.func.attr in _APP_METHODS
                    and isinstance(dec.func.value, ast.Name)
                    and dec.func.value.id == "app"):
                out.append((node.name, node))
                break
    return out


def _http_symbols(unit: Unit) -> Set[str]:
    """领域层从 fastapi 拿到的 HTTP 相关符号（**AST 判据**）。"""
    got: Set[str] = set()
    for node in ast.walk(unit.tree):
        if isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] != "fastapi":
                continue
            for alias in node.names:
                if alias.name in HTTP_SYMBOLS:
                    got.add(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "fastapi":
                    got.add("<module fastapi>")
    return got


def _cycles(units: Dict[str, Unit], *, include_lazy: bool) -> List[List[str]]:
    """找简单环（长度 <= 6）。

    ``include_lazy=False`` ⇒ 只看**顶层** import = **硬环**（真问题：
    import 期就会半初始化）。``include_lazy=True`` 多出来的就是**软环**
    （环上至少一跳在函数体内，是刻意的解环手法）。
    """
    adjacency: Dict[str, Set[str]] = {}
    for name, unit in units.items():
        targets = {t for t, _ in (unit.all_imports if include_lazy else unit.top)}
        adjacency[name] = {t for t in targets if not t.startswith(".") and t != name}

    found: List[List[str]] = []
    seen: Set[frozenset] = set()

    def _walk(start: str, node: str, path: List[str], limit: int) -> None:
        if limit == 0:
            return
        for nxt in sorted(adjacency.get(node, ())):
            if nxt == start:
                key = frozenset(path)
                if key not in seen:
                    seen.add(key)
                    found.append(path + [nxt])
                continue
            if nxt in path:
                continue
            _walk(start, nxt, path + [nxt], limit - 1)

    for name in sorted(adjacency):
        _walk(name, name, [name], 6)
    return found


# ============================================================
# R1–R6：依赖方向与环
# ============================================================
def check_r1_layering(units: Dict[str, Unit]) -> bool:
    _section("[R1] 严格单向分层：低层不得依赖高层")
    print("     层号 models/schemas/database=0 < utils/security/deps=1 "
          "< services/modules/common=2 < api=3；main 与业务模块门面是装配层（豁免）")
    violations: List[str] = []
    actual: Set[Tuple[str, str]] = set()
    for name, unit in sorted(units.items()):
        if _layer_of(name) >= _ASSEMBLY_LAYER:
            continue
        for target, lineno in unit.all_imports:
            if target.startswith("."):
                continue
            if _same_module_package(name, target):
                continue
            if _layer_of(target) > _layer_of(name):
                actual.add((name, target))
                if (name, target) not in R1_KNOWN_ALLOWED:
                    violations.append(
                        f"{name}:{lineno} -> {target}  "
                        f"(L{_layer_of(name)} 依赖 L{_layer_of(target)})"
                    )
    for v in violations:
        print("       " + v)
    ok = _check(f"新增反向依赖 = 0（白名单外，实际 {len(violations)}）", not violations)

    stale = sorted(set(R1_KNOWN_ALLOWED) - actual)
    for src, tgt in stale:
        print(f"       ★ 已修复但仍挂在白名单：{src} -> {tgt}")
    ok &= _check(
        f"★ 棘轮：R1 白名单无过期条目（实际 {len(stale)}）", not stale,
        "该反向依赖已清掉，请从 R1_KNOWN_ALLOWED 删除",
    )
    for key in sorted(R1_KNOWN_ALLOWED):
        print(f"       白名单理由 [{key[0]} -> {key[1]}]：{R1_KNOWN_ALLOWED[key]}")
    return ok


def check_r2_api_not_reverse_imported(units: Dict[str, Unit]) -> bool:
    _section("[R2] api/* 不得被反向 import（main 装配层除外）")
    violations: List[str] = []
    for name, unit in sorted(units.items()):
        if name == "main" or _is_router(name):
            continue
        for target, lineno in unit.all_imports:
            if target == "api" or target.startswith("api."):
                violations.append(f"{name}:{lineno} -> {target}")
    for v in violations:
        print("       " + v)
    return _check(f"反向 import 路由 = 0（实际 {len(violations)}）", not violations)


def check_r3_service_not_import_api(units: Dict[str, Unit]) -> bool:
    _section("[R3] service 层不得 import api.*（R2 的细化）")
    violations: List[str] = []
    for name, unit in sorted(units.items()):
        if not (name.startswith("services.") or
                (name.startswith("modules.") and name.endswith(".service"))):
            continue
        for target, lineno in unit.all_imports:
            if target == "api" or target.startswith("api."):
                violations.append(f"{name}:{lineno} -> {target}")
    for v in violations:
        print("       " + v)
    return _check(f"service -> api = 0（实际 {len(violations)}）", not violations)


def check_r4_low_level_is_leaf(units: Dict[str, Unit]) -> bool:
    _section("[R4] models / schemas 不得 import 任何上层")
    violations: List[str] = []
    for name, unit in sorted(units.items()):
        if not (name.startswith("models") or name.startswith("schemas")):
            continue
        for target, lineno in unit.all_imports:
            if target.startswith("."):
                continue
            if _layer_of(target) > 0:
                violations.append(f"{name}:{lineno} -> {target}")
    for v in violations:
        print("       " + v)
    return _check(f"models/schemas 依赖上层 = 0（实际 {len(violations)}）", not violations)


def check_r5_ai_not_import_router(units: Dict[str, Unit]) -> bool:
    _section("[R5] AI 适配层不得反向依赖路由")
    violations: List[str] = []
    for name, unit in sorted(units.items()):
        if not name.startswith("modules.ai"):
            continue
        for target, lineno in unit.all_imports:
            if _is_router(target) or target in ("main", "deps"):
                violations.append(f"{name}:{lineno} -> {target}")
    for v in violations:
        print("       " + v)
    ok = _check(f"AI 层 -> 路由 = 0（实际 {len(violations)}）", not violations)

    # 反向证据：业务模块确实通过明确接口调用 AI，而不是自己搓一份
    consumers = sorted({
        name for name, unit in units.items()
        if not name.startswith("modules.ai")
        and any(t.startswith("modules.ai") for t, _ in unit.all_imports)
    })
    print(f"     AI 能力的业务消费方：{consumers}")
    return ok and _check("AI 层至少被 1 个业务模块使用（不是死代码）", len(consumers) >= 1)


def check_r6_cycles(units: Dict[str, Unit]) -> bool:
    _section("[R6] 循环依赖：硬环必须 0，软环只钉数量")
    hard = _cycles(units, include_lazy=False)
    for cycle in hard:
        print("       硬环 " + " -> ".join(cycle))
    ok = _check(f"硬环 = 0（实际 {len(hard)}）", not hard)

    soft = _cycles(units, include_lazy=True)
    hard_keys = {frozenset(c[:-1]) for c in hard}
    soft_keys = {frozenset(c[:-1]) for c in soft} - hard_keys
    print(f"     软环 {len(soft_keys)} 个（环上有函数内延迟导入，刻意的解环手法）：")
    for key in sorted(soft_keys, key=lambda s: sorted(s))[:10]:
        print("       " + " <-> ".join(sorted(key)))
    if len(soft_keys) > 10:
        print(f"       …（其余 {len(soft_keys) - 10} 个略）")

    ok &= _check(
        f"软环数不超过上限 {R6_MAX_SOFT_CYCLES}（实际 {len(soft_keys)}）",
        len(soft_keys) <= R6_MAX_SOFT_CYCLES,
        "新增软环说明又有一对模块互相依赖；确认是必要的解环手法后把 "
        "R6_MAX_SOFT_CYCLES 调大，否则请拆模块",
    )
    if len(soft_keys) < R6_MAX_SOFT_CYCLES:
        print(f"       ★ 棘轮：实际 {len(soft_keys)} < 上限 {R6_MAX_SOFT_CYCLES}，"
              f"请把 R6_MAX_SOFT_CYCLES 改成 {len(soft_keys)}")
        ok &= _check(
            "★ 棘轮：上限与实测一致（修掉软环后要同步调小）",
            False,
            "上限虚高会让「新增软环」不被发现",
        )
    return ok


# ============================================================
# R7–R10：层内约定（棘轮）
# ============================================================
def check_r7_router_no_sql(units: Dict[str, Unit]) -> bool:
    _section("[R7] 路由层不写 SQL / 不 import SQLAlchemy（棘轮）")
    print("     路由模块按整文件查；main.py 只查被 @app.<method> 装饰的**路由函数**"
          "（白名单才能精确到函数，迁一个删一条）。")
    actual: Dict[str, List[str]] = {}
    for name, unit in sorted(units.items()):
        if not _is_router(name):
            continue
        hits = _sql_hits(unit)
        if hits:
            actual[name] = hits

    main_unit = units.get("main")
    if main_unit is not None:
        for func_name, func_node in _app_routes(main_unit.tree):
            hits = _sql_hits_nodes(ast.walk(func_node))
            if hits:
                actual[f"main:{func_name}"] = hits

    for name, hits in sorted(actual.items()):
        mark = "白名单" if name in R7_KNOWN_ALLOWED else "★ 新增违规"
        print(f"       [{mark}] {name}: {hits}")

    new = sorted(set(actual) - set(R7_KNOWN_ALLOWED))
    ok = _check(f"新增 SQL 路由模块 = 0（实际 {len(new)}）", not new,
                "路由层只做协议/校验/鉴权/响应转换；查库下沉到 service")
    stale = sorted(set(R7_KNOWN_ALLOWED) - set(actual))
    for name in stale:
        print(f"       ★ 已清理但仍挂在白名单：{name}")
    ok &= _check(
        f"★ 棘轮：R7 白名单无过期条目（实际 {len(stale)}）", not stale,
        "该模块已经不再写 SQL，请从 R7_KNOWN_ALLOWED 删除",
    )
    for name in sorted(R7_KNOWN_ALLOWED):
        print(f"       白名单理由 [{name}]：{R7_KNOWN_ALLOWED[name]}")
    return ok


def check_r8_domain_no_http(units: Dict[str, Unit]) -> bool:
    _section("[R8] 领域层不 import FastAPI 的 HTTP 对象（棘轮）")
    actual: Dict[Tuple[str, str], int] = {}
    for name, unit in sorted(units.items()):
        if not _is_domain(name):
            continue
        for symbol in sorted(_http_symbols(unit)):
            actual[(name, symbol)] = 1
    for (name, symbol) in sorted(actual):
        mark = "白名单" if (name, symbol) in R8_KNOWN_ALLOWED else "★ 新增违规"
        print(f"       [{mark}] {name} -> fastapi.{symbol}")

    new = sorted(set(actual) - set(R8_KNOWN_ALLOWED))
    ok = _check(f"新增领域层 HTTP 依赖 = 0（实际 {len(new)}）", not new,
                "领域层抛领域异常，由 api 层统一转成 HTTPException（见 modules/auth）")
    stale = sorted(set(R8_KNOWN_ALLOWED) - set(actual))
    for key in stale:
        print(f"       ★ 已清理但仍挂在白名单：{key[0]} -> {key[1]}")
    ok &= _check(
        f"★ 棘轮：R8 白名单无过期条目（实际 {len(stale)}）", not stale,
        "该模块已经不再 import 该符号，请从 R8_KNOWN_ALLOWED 删除",
    )
    for key in sorted(R8_KNOWN_ALLOWED):
        print(f"       白名单理由 [{key[0]} -> {key[1]}]：{R8_KNOWN_ALLOWED[key]}")
    return ok


def check_r9_module_service_no_fastapi(units: Dict[str, Unit]) -> bool:
    _section("[R9] modules.*.service 完全禁止 import fastapi")
    print("     新模块没有历史包袱，所以比 R8 更严：连 HTTPException 都不许有。")
    violations: List[str] = []
    for name, unit in sorted(units.items()):
        if not (name.startswith("modules.") and name.endswith(".service")):
            continue
        for node in ast.walk(unit.tree):
            if isinstance(node, ast.ImportFrom):
                if (node.module or "").split(".")[0] == "fastapi":
                    violations.append(f"{name}:{node.lineno} from {node.module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] == "fastapi":
                        violations.append(f"{name}:{node.lineno} import {alias.name}")
    for v in violations:
        print("       " + v)
    return _check(f"modules.*.service -> fastapi = 0（实际 {len(violations)}）", not violations)


def check_r10_module_api_no_models(units: Dict[str, Unit]) -> bool:
    _section("[R10] modules.*.api 不得 import models（路由层不碰 ORM）")
    violations: List[str] = []
    for name, unit in sorted(units.items()):
        if not (name.startswith("modules.") and name.endswith(".api")):
            continue
        for target, lineno in unit.all_imports:
            if target == "models" or target.startswith("models."):
                violations.append(f"{name}:{lineno} -> {target}")
    for v in violations:
        print("       " + v)
    return _check(f"modules.*.api -> models = 0（实际 {len(violations)}）", not violations)


# ============================================================
# R11：迁移完成的模块结构 + main.py 只做装配
# ============================================================
def check_r11_module_structure(units: Dict[str, Unit]) -> bool:
    _section("[R11] 已迁移模块的结构完整性 + main.py 只做装配")
    ok = True

    main_path = _BACKEND / "main.py"
    main_src = main_path.read_text(encoding="utf-8")

    # 装配真的发生了（AST 判据，不是子串）
    main_tree = ast.parse(main_src)
    mounted: Set[str] = set()
    for node in ast.walk(main_tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "include_router"):
            for arg in node.args:
                if isinstance(arg, ast.Name):
                    mounted.add(arg.id)

    for module, (files, forbidden, router_name) in _MIGRATED_MODULES.items():
        print(f"\n     modules/{module}")
        module_dir = _BACKEND / "modules" / module
        for filename in files:
            ok &= _check(f"modules/{module}/{filename} 存在", (module_dir / filename).is_file())

        api_path = module_dir / "api.py"
        service_path = module_dir / "service.py"
        if api_path.is_file() and service_path.is_file():
            api_src = api_path.read_text(encoding="utf-8")
            service_src = service_path.read_text(encoding="utf-8")
            ok &= _check(
                f"modules/{module}/api.py 是路由层（有 APIRouter）",
                "APIRouter(" in api_src,
            )
            ok &= _check(
                f"modules/{module}/service.py 不是路由层（无 APIRouter）",
                "APIRouter" not in service_src,
            )
            # 路由层真的调用 service（不是建了个文件没人用）
            ok &= _check(
                f"modules/{module}/api.py 真的调用 service",
                "service." in api_src,
            )

        init_path = module_dir / "__init__.py"
        if init_path.is_file():
            init_src = init_path.read_text(encoding="utf-8")
            ok &= _check(
                f"modules/{module}/__init__.py re-export 了 router",
                "router" in init_src and f"from modules.{module}.api" in init_src,
            )

        for token in forbidden:
            ok &= _check(f"main.py 不再包含 {module} 实现 {token!r}", token not in main_src)

        ok &= _check(
            f"main.py 通过 app.include_router 挂载了 {router_name}",
            router_name in mounted,
            f"实际挂载：{sorted(mounted)}",
        )

    # ---- ai 模块（P3）：平迁，文件结构与其他业务模块不同（没有 api/service/schemas）----
    print("\n     modules/ai（平迁自 services/ai）")
    ai_dir = _BACKEND / "modules" / "ai"
    ok &= _check("modules/ai 是包（有 __init__.py）", (ai_dir / "__init__.py").is_file())
    ok &= _check(
        "services/ai 已迁走（旧位置不应存在）",
        not (_BACKEND / "services" / "ai").exists(),
        "两个位置并存时 import 走哪一份取决于 sys.path 与缓存顺序，是玄学",
    )
    for filename in ("llm.py", "factory.py", "modes.py", "policy.py", "telemetry.py",
                     "errors.py", "boundary.py", "compat.py"):
        ok &= _check(f"modules/ai/{filename} 存在", (ai_dir / filename).is_file())
    for token in ("class LegacySparkFacade", "def build_legacy_spark_facade",
                  "class LlmProvider", "def ai_call_scope"):
        ok &= _check(f"main.py 不再包含 AI 层实现 {token!r}", token not in main_src)

    # ---- common（P4）：共享基础设施 ----
    print("\n     common（共享基础设施）")
    common_dir = _BACKEND / "common"
    ok &= _check("common/__init__.py 存在", (common_dir / "__init__.py").is_file())
    ok &= _check("common/health.py 存在", (common_dir / "health.py").is_file())
    for token in ("def embedding_status(", "def vector_status(", "def threshold_status(",
                  "def avatar_status(", "def production_status(",
                  "def _report_ai_status(", "def _report_production_status(",
                  "def _count_knowledge_chunks("):
        ok &= _check(f"main.py 不再包含状态上报实现 {token!r}", token not in main_src)
    # ★ 别名不是风格问题：main.py 有个 `async def health()` 端点，同名会覆盖 import
    ok &= _check(
        "main.py 用别名导入 common.health（端点函数同名，直接 import 会被覆盖）",
        "from common import health as health_report" in main_src,
        "改成 `from common import health` 会让 health_report.* 调用在运行期 AttributeError",
    )

    # 认证模块额外：路由层把领域异常转成 HTTP —— 这是它存在的理由
    auth_api = _BACKEND / "modules" / "auth" / "api.py"
    if auth_api.is_file():
        auth_src = auth_api.read_text(encoding="utf-8")
        ok &= _check(
            "modules/auth/api.py 把 AuthError 转成 HTTPException",
            "AuthError" in auth_src and "HTTPException" in auth_src,
        )
    return ok


# ============================================================
# R12：兼容命名空间
# ============================================================
#: ``main.py`` 必须保留的重导出：名字 -> 外部消费方
#:
#: ★ 这一条是**真实事故**换来的（P2 迁移时踩到）：
#:   搬走 ``get_all_jobs`` 时顺手删掉了 ``from database import async_session``
#:   —— 因为 main.py 里确实没人用它了。但 ``scripts/create_admin.py:78`` 写着
#:   ``async with main.async_session() as session``，``tests/test_admin_role.py``
#:   还靠 ``main.async_session = session_factory`` 把会话指到测试库。
#:   结果：**脚本在生产里已经坏了**，而 78 个套件里只有 1 个红。
#:
#:   这类名字的特点是「本文件没有使用点」，所以任何 linter 都会建议删掉；
#:   但它们是**对外契约**（脚本、测试、运维习惯都在用）。要删必须先改消费方。
_COMPAT_NAMESPACE: Dict[str, str] = {
    "async_session": "scripts/create_admin.py + tests/test_admin_role.py 注入测试会话工厂",
    "engine": "scripts/create_admin.py 退出前 dispose",
    "User": "scripts/create_admin.py 构造管理员行",
    "DATABASE_URL": "与抽取前的模块命名空间保持一致（运维排障时会 print 它）",
    "Base": "测试通过 main.Base 建表",
}


def check_r12_compat_namespace() -> bool:
    _section("[R12] main.py 的兼容命名空间（有外部消费方，但本文件不用）")
    print("     这些名字在 main.py 里没有使用点，linter 会建议删——但它们是对外契约。")
    tree = ast.parse((_BACKEND / "main.py").read_text(encoding="utf-8"))
    exported: Set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                exported.add(alias.asname or alias.name.split(".")[0])
    ok = True
    for name, consumer in sorted(_COMPAT_NAMESPACE.items()):
        ok &= _check(
            f"main.{name} 仍可访问（消费方：{consumer}）",
            name in exported,
            "删掉它会让消费方在**运行期** AttributeError，而多数套件不会红",
        )
    return ok


# ============================================================
# 判据自检（守卫的守卫）
# ============================================================
def _fake_unit(name: str, src: str, *, top: List[Tuple[str, int]] = None,
               lazy: List[Tuple[str, int]] = None) -> Unit:
    """在内存里造一个 Unit，不碰磁盘（所以这段自检能离线跑）。"""
    return Unit(name, pathlib.Path("<mem>"), list(top or []), list(lazy or []),
                ast.parse(src))


def check_judgement_selftest() -> bool:
    _section("[12] 判据自检：守卫真的能红（防止恒真断言）")
    print("     每条判据都喂**正例与反例**。只过正例的判据是坏的；"
          "两边都过的同样是坏的（恒真）。")
    ok = True

    print("\n     R7（路由层 SQL）—— 反例必须放行，否则守卫会变成噪音：")
    for label, src, expect in [
        ("@router.delete 装饰器", "@router.delete('/a')\ndef f(): pass\n", False),
        ("ws.send_text(...)", "await ws.send_text('a')\n", False),
        ("AsyncSession 类型注解", "from sqlalchemy.ext.asyncio import AsyncSession\n", False),
        ("IntegrityError 异常", "from sqlalchemy.exc import IntegrityError\n", False),
        ("select(...)", "q = select(User)\n", True),
        ("db.execute(...)", "await db.execute(q)\n", True),
        ("db.scalars(...)", "rows = await db.scalars(q)\n", True),
        ("db.add(...) 会话写操作", "db.add(row)\n", True),
        ("db.commit() 事务提交", "await db.commit()\n", True),
        ("db.delete(...) 会话删除", "await db.delete(row)\n", True),
        ("from sqlalchemy import select", "from sqlalchemy import select\n", True),
        ("sqlalchemy.select(...)", "q = sqlalchemy.select(User)\n", True),
    ]:
        hits = _sql_hits(_fake_unit("api.sample", src))
        ok &= _check(
            f"R7 判据 {label} -> {'命中' if expect else '放行'}",
            bool(hits) == expect, f"实际 hits={hits}",
        )

    print("\n     R8（领域层 HTTP 对象）：")
    for label, src, expect in [
        ("from fastapi import HTTPException", "from fastapi import HTTPException\n", True),
        ("from fastapi import APIRouter", "from fastapi import APIRouter\n", True),
        ("from fastapi import Depends", "from fastapi import Depends\n", True),
        ("import fastapi", "import fastapi\n", True),
        ("from fastapi import status（常量，放行）", "from fastapi import status\n", False),
        ("import logging", "import logging\n", False),
    ]:
        got = _http_symbols(_fake_unit("services.sample", src))
        ok &= _check(
            f"R8 判据 {label} -> {'命中' if expect else '放行'}",
            bool(got) == expect, f"实际 {sorted(got)}",
        )

    print("\n     R1（层号）与同包豁免：")
    for label, got, expect in [
        ("models", _layer_of("models"), 0),
        ("models.interview", _layer_of("models.interview"), 0),
        ("utils.x", _layer_of("utils.x"), 1),
        ("deps", _layer_of("deps"), 1),
        ("services.x", _layer_of("services.x"), 2),
        ("api.x", _layer_of("api.x"), 3),
        ("modules.auth.api", _layer_of("modules.auth.api"), 3),
        ("modules.auth.service", _layer_of("modules.auth.service"), 2),
        ("modules.auth.schemas", _layer_of("modules.auth.schemas"), 0),
        ("modules.auth（包门面 = 装配层）", _layer_of("modules.auth"), 9),
        ("modules.ai（能力包 = L2）", _layer_of("modules.ai"), 2),
        ("modules.ai.llm", _layer_of("modules.ai.llm"), 2),
        ("main（装配层）", _layer_of("main"), 9),
    ]:
        ok &= _check(f"R1 层号 {label} = {expect}", got == expect, f"实际 {got}")

    ok &= _check("R1 同包豁免：modules.auth.api -> modules.auth",
                 _same_module_package("modules.auth.api", "modules.auth"))
    ok &= _check("R1 跨包不豁免：modules.auth.api -> modules.career.api",
                 not _same_module_package("modules.auth.api", "modules.career.api"))
    ok &= _check("R1 非 modules 包不豁免：services.a -> services.b",
                 not _same_module_package("services.a", "services.b"))

    print("\n     import 收集（嵌套在 try/if 里的延迟导入）：")
    nested_src = (
        "def f():\n"
        "    try:\n"
        "        from services.job_matching import keyword_match\n"
        "    except ImportError:\n"
        "        pass\n"
    )
    nested_tree = ast.parse(nested_src)
    got = _collect_imports(list(ast.walk(nested_tree)), {"services.job_matching"})
    ok &= _check(
        "收集器能看见 try 块里的延迟导入",
        any(t == "services.job_matching" for t, _ in got),
        f"实际 {got}",
    )
    # ★ 对照组：证明上面那条不是恒真。只看顶层语句时**必须**看不见它，
    #   否则说明「能看见」是因为别的路径，而不是因为 walk 覆盖了嵌套。
    top_only = _collect_imports(nested_tree.body, {"services.job_matching"})
    ok &= _check(
        "对照：只看顶层语句时看不见（证明上一条不是恒真）",
        not top_only,
        f"实际 {top_only}",
    )
    print("       （这条自检来自真实漏检：interview_agent/planner 的 "
          "`from main import spark_api` 在 try 里，早期判据漏了 3 条反向依赖）")

    print("\n     R6（环检测）—— 硬环 / 软环必须分开：")
    hard = {
        "a": _fake_unit("a", "import b\n", top=[("b", 1)]),
        "b": _fake_unit("b", "import a\n", top=[("a", 1)]),
    }
    ok &= _check("R6 判据：顶层互相 import 检出硬环",
                 len(_cycles(hard, include_lazy=False)) == 1)
    soft = {
        "a": _fake_unit("a", "import b\n", top=[("b", 1)]),
        "b": _fake_unit("b", "def f():\n    import a\n", lazy=[("a", 2)]),
    }
    ok &= _check("R6 判据：函数内延迟导入**不算**硬环",
                 len(_cycles(soft, include_lazy=False)) == 0)
    ok &= _check("R6 判据：但它构成软环",
                 len(_cycles(soft, include_lazy=True)) == 1)
    acyclic = {
        "a": _fake_unit("a", "import b\n", top=[("b", 1)]),
        "b": _fake_unit("b", "pass\n"),
    }
    ok &= _check("R6 判据：无环时返回空（不是永远返回非空）",
                 len(_cycles(acyclic, include_lazy=True)) == 0)
    return ok


# ============================================================
# 零依赖自检
# ============================================================
def check_pure_ast() -> bool:
    _section("[0] 本套件零项目 import（纯 AST，不触发数据库/网络）")
    leaked = sorted(
        name for name in sys.modules
        if name in ("main", "database", "deps", "security")
        or name.split(".")[0] in ("api", "services", "schemas", "models", "modules")
    )
    for name in leaked:
        print(f"       已加载：{name}")
    return _check(
        f"未 import 任何项目模块（实际 {len(leaked)}）", not leaked,
        "守卫必须能在没有 MySQL / 没有 .env 的机器上跑；导入项目模块会把环境依赖带进来",
    )


# ============================================================
def main() -> int:
    print("=" * 72)
    print("模块化单体边界守卫 · 自检（棘轮）")
    print("=" * 72)
    units = _load_units()
    print(f"扫描 {len(units)} 个业务模块")

    ok = True
    ok &= check_pure_ast()
    ok &= check_r1_layering(units)
    ok &= check_r2_api_not_reverse_imported(units)
    ok &= check_r3_service_not_import_api(units)
    ok &= check_r4_low_level_is_leaf(units)
    ok &= check_r5_ai_not_import_router(units)
    ok &= check_r6_cycles(units)
    ok &= check_r7_router_no_sql(units)
    ok &= check_r8_domain_no_http(units)
    ok &= check_r9_module_service_no_fastapi(units)
    ok &= check_r10_module_api_no_models(units)
    ok &= check_r11_module_structure(units)
    ok &= check_r12_compat_namespace()
    ok &= check_judgement_selftest()

    print("\n" + "=" * 72)
    if ok:
        print("结果: 全部通过")
    else:
        print(f"结果: {len(_FAILURES)} 项失败")
        for name in _FAILURES:
            print(f"  - {name}")
    print("=" * 72)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
