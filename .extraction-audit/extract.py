from pathlib import Path
import ast,hashlib,re
root=Path('backend'); backup=Path('.extraction-audit/before')
def write(rel,s):
 p=root/rel
 if p.exists():
  b=backup/rel;b.parent.mkdir(parents=True,exist_ok=True)
  if not b.exists():b.write_bytes(p.read_bytes())
 p.parent.mkdir(parents=True,exist_ok=True);p.write_text(s,encoding='utf-8')
s=(root/'main.py').read_text(encoding='utf-8')
start=s.index('try:\n    import websocket as _ws');end=s.index('# ============================================================\n# AI 能力接入层装配',start)
transport=s[start:end]
write('modules/ai/spark_transport.py','"""Spark X1 传输：签名、连接、协议帧和同步/异步收发。\n\n保留原有请求参数、响应拼接和错误字符串契约；不依赖应用入口。\n"""\nimport os\nimport json\nimport asyncio\nimport hashlib\nimport hmac\nimport base64\nfrom datetime import datetime\nfrom urllib.parse import urlencode\n\n'+transport)
s=s[:start]+'from modules.ai.spark_transport import SparkAPI, _make_url  # compatibility exports\n\n\n'+s[end:]
a=s.index('class JobCreate(');b=s.index('# ============================================================',a)
write('modules/admin/schemas.py','"""管理员请求契约。"""\nfrom typing import List\nfrom pydantic import BaseModel, Field\n\n\n'+s[a:b])
s=s[:a]+'from modules.admin.schemas import JobCreate  # compatibility export\n\n\n'+s[b:]
a=s.index('@app.get("/api/admin/user-logs"');b=s.index('# ============================================================\n# 健康检查',a)
block=s[a:b];tree=ast.parse(block);lines=block.splitlines(keepends=True)
service='"""管理员用例：查询、岗位写入和会话撤销；事务顺序保持不变。"""\nimport logging\nfrom datetime import datetime\nfrom sqlalchemy import select, update\nfrom sqlalchemy.ext.asyncio import AsyncSession\nfrom models import User, UserLoginLog, UserSession, Job\nfrom services import ws_ticket_store, ws_revocation\nfrom modules.admin.schemas import JobCreate\n\n_ticket_log = logging.getLogger("careerai.ws_ticket")\n\nclass UserNotFound(Exception):\n    pass\n\n'
api='"""管理员 HTTP 路由：原路径、鉴权依赖、参数和响应不变。"""\nfrom fastapi import APIRouter, Depends, HTTPException\nfrom sqlalchemy.ext.asyncio import AsyncSession\nfrom database import get_db\nfrom deps import get_current_admin\nfrom modules.admin.schemas import JobCreate\nimport modules.admin.service as service\n\nrouter = APIRouter()\n\n'
for node in tree.body:
 if not isinstance(node,ast.AsyncFunctionDef):continue
 body=''.join(lines[node.body[0].lineno-1:node.end_lineno])
 body=body.replace('raise HTTPException(status_code=404, detail="用户不存在")','raise UserNotFound("用户不存在")')
 args={'get_user_logs':'db: AsyncSession','add_job':'data: JobCreate, db: AsyncSession','kick_user_sessions':'user_id: int, db: AsyncSession'}[node.name]
 service+=f'\nasync def {node.name}({args}):\n'+body+'\n\n'
 header=''.join(lines[node.decorator_list[0].lineno-1:node.body[0].lineno-1]).replace('@app.','@router.')
 # Keep docstring exactly, so OpenAPI descriptions remain stable.
 doc=node.body[0];docsrc=''.join(lines[doc.lineno-1:doc.end_lineno])
 call={'get_user_logs':'db','add_job':'data, db','kick_user_sessions':'user_id, db'}[node.name]
 api+=header+docsrc
 if node.name=='kick_user_sessions':
  api+='    try:\n        return await service.kick_user_sessions(user_id, db)\n    except service.UserNotFound as exc:\n        raise HTTPException(status_code=404, detail=str(exc)) from None\n\n\n'
 else:api+=f'    return await service.{node.name}({call})\n\n\n'
write('modules/admin/api.py',api);write('modules/admin/service.py',service)
write('modules/admin/__init__.py','"""管理员模块。"""\nfrom modules.admin.api import router\n')
s=s[:a]+'from modules.admin import router as admin_router\nfrom modules.admin.api import get_user_logs, add_job, kick_user_sessions  # compatibility exports\n\napp.include_router(admin_router)\n\n\n'+s[b:]
s=s.replace('spark_api = build_legacy_spark_facade()','spark_api = build_legacy_spark_facade()\nfrom common.llm import configure as configure_interview_llm\n\nconfigure_interview_llm(spark_api)')
# Transport-only imports no longer belong to the entrypoint.
for line in ['import json\n','import asyncio\n','import hashlib\n','import hmac\n','import base64\n','from urllib.parse import urlencode\n','from sqlalchemy import select, update\n']:
 s=s.replace(line,'')
write('main.py',s)
write('common/llm.py','''"""共享 LLM 端口；装配层注册实例，业务层不导入应用入口。"""
from typing import Any

_llm: Any = None


def configure(llm: Any) -> None:
    global _llm
    _llm = llm


def get_llm() -> Any:
    if _llm is None:
        raise RuntimeError("LLM facade has not been configured")
    return _llm
''')
for rel in ['services/interview_agent.py','services/interview_planner.py']:
 t=(root/rel).read_text(encoding='utf-8')
 t=t.replace('延迟获取全局 Spark 实例，避免 services → main 的模块级循环导入。','获取装配层注册的 LLM 门面，不导入应用入口。')
 t=t.replace('from main import spark_api  # noqa: PLC0415 - 刻意延迟导入','from common.llm import get_llm').replace('        return spark_api\n','        return get_llm()\n')
 write(rel,t)
# Tests now inject through the explicit port instead of reassigning an entrypoint global.
for rel in ['tests/test_rag_production_route.py','tests/rag_production_chain_spark_run.py']:
 t=(root/rel).read_text(encoding='utf-8');t=t.replace('original_spark = main.spark_api','from common.llm import get_llm, configure\n    original_spark = get_llm()').replace('main.spark_api = spark  # type: ignore[assignment]','configure(spark)').replace('main.spark_api = original_spark  # type: ignore[assignment]','configure(original_spark)');write(rel,t)
# Refresh only the explicitly changed default dependency wiring fingerprints.
rel='tests/test_agent_full_input.py';t=(root/rel).read_text(encoding='utf-8')
for source in ['services/interview_agent.py','services/interview_planner.py']:
 old=(backup/source).read_text(encoding='utf-8');new=(root/source).read_text(encoding='utf-8')
 t=t.replace(hashlib.sha256(old.encode()).hexdigest(),hashlib.sha256(new.encode()).hexdigest())
t=t.replace('FROZEN_SOURCES: Tuple','#: 本轮仅更新 Agent/Planner 的默认 LLM 端口接线指纹，业务函数与 Prompt 未改。\nFROZEN_SOURCES: Tuple');write(rel,t)
rel='tests/test_module_boundaries.py';t=(root/rel).read_text(encoding='utf-8')
t=re.sub(r'R7_KNOWN_ALLOWED: Dict\[str, str\] = \{.*?\n\}', 'R7_KNOWN_ALLOWED: Dict[str, str] = {}',t,flags=re.S)
t=re.sub(r'R1_KNOWN_ALLOWED: Dict\[Tuple\[str, str\], str\] = \{.*?\n\}', 'R1_KNOWN_ALLOWED: Dict[Tuple[str, str], str] = {}',t,flags=re.S)
t=t.replace('    "auth": (','    "admin": (\n        ("__init__.py", "api.py", "service.py", "schemas.py"),\n        ("def get_user_logs(", "def add_job(", "def kick_user_sessions(", "class SparkAPI"),\n        "admin_router",\n    ),\n    "auth": (',1);write(rel,t)
rel='tests/test_ai_layer.py';t=(root/rel).read_text(encoding='utf-8')
t=t.replace('"旧协议实现仍留在 main.py（本层不重写 websocket 协议）",\n                 "class SparkAPI" in (_BACKEND / "main.py").read_text(encoding="utf-8"))','"Spark 协议实现独立归属传输模块，入口不再定义",\n                 "class SparkAPI" in (_BACKEND / "modules/ai/spark_transport.py").read_text(encoding="utf-8")\n                 and "class SparkAPI" not in (_BACKEND / "main.py").read_text(encoding="utf-8"))');write(rel,t)
rel='tests/test_ws_revocation.py';t=(root/rel).read_text(encoding='utf-8')
t=t.replace('    text_main = (_BACKEND / "main.py").read_text(encoding="utf-8")','    text_main = (_BACKEND / "main.py").read_text(encoding="utf-8")\n    text_admin = (_BACKEND / "modules/admin/api.py").read_text(encoding="utf-8")')
t=t.replace("'\"/api/admin/users/{user_id}/kick\"' in text_main", "'\"/api/admin/users/{user_id}/kick\"' in text_admin").replace('and "Depends(get_current_admin)" in text_main','and "Depends(get_current_admin)" in text_admin');write(rel,t)
print('extraction complete; existing files backed up')
