from pathlib import Path
import hashlib
root=Path('backend');backup=Path('.rag-trace-audit/before')
def save(rel,s):
 p=root/rel;b=backup/rel
 if not b.exists():b.parent.mkdir(parents=True,exist_ok=True);b.write_bytes(p.read_bytes())
 p.write_text(s,encoding='utf-8')
rel='modules/ai/policy.py';s=(root/rel).read_text(encoding='utf-8').replace('                    message=error.message,','                    # 不记录上游原文：可能回显 Prompt、凭据或敏感文档。');save(rel,s)
rel='tests/test_rag_retriever_config.py';s=(root/rel).read_text(encoding='utf-8').replace('ALLOWED_TOP_LEVEL_IMPORTS = {"__future__", "math", "os", "collections.abc", "typing"}','ALLOWED_TOP_LEVEL_IMPORTS = {"__future__", "math", "os", "collections.abc", "typing", "common.rag_trace"}');save(rel,s)
rel='tests/test_vector_knowledge_retriever.py';s=(root/rel).read_text(encoding='utf-8').replace('top_level == {"__future__", "collections", "math", "typing", "services"}','top_level == {"__future__", "collections", "math", "typing", "services", "common"}\n           and all(m == "common.rag_trace" or m.startswith("common.rag_trace.") for m in all_imports if m.startswith("common"))').replace('顶层只有 __future__ / collections / math / typing / services','允许标准库、services 与纯标准库 common.rag_trace');save(rel,s)
# Re-baseline only the authorized telemetry edit, accounting for hashes already updated in this turn.
for source in ['services/vector_store_sql.py']:
 old=(backup/source).read_text(encoding='utf-8');new=(root/source).read_text(encoding='utf-8')
 intermediate=new.replace('@observed("vector_search")','@observed("search")').replace('with span("candidate_filter")','with span("filter")')
 for p in (root/'tests').glob('test_*.py'):
  s=p.read_text(encoding='utf-8');t=s.replace(hashlib.sha256(intermediate.encode()).hexdigest(),hashlib.sha256(new.encode()).hexdigest())
  if t!=s:save(str(p.relative_to(root)),t)
