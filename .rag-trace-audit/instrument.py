from pathlib import Path
import ast,hashlib,re
root=Path('backend');backup=Path('.rag-trace-audit/before')
def write(rel,s):
 p=root/rel;b=backup/rel
 if p.exists() and not b.exists():b.parent.mkdir(parents=True,exist_ok=True);b.write_bytes(p.read_bytes())
 p.write_text(s,encoding='utf-8')
def instrument(rel,entries):
 s=(root/rel).read_text(encoding='utf-8');tree=ast.parse(s)
 # insert after future import/docstring, preserving signatures through wraps
 idx=tree.body[0].end_lineno if isinstance(tree.body[0],ast.Expr) else 0
 for n in tree.body:
  if isinstance(n,ast.ImportFrom) and n.module=='__future__':idx=n.end_lineno
 lines=s.splitlines(keepends=True)
 additions={idx:'from common.rag_trace import observed, span, context_ready\n'}
 for node in ast.walk(tree):
  if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in entries:
   additions[node.lineno-1]=' '*node.col_offset+f'@observed("{entries[node.name]}")\n'
 for line,txt in sorted(additions.items(),reverse=True):lines.insert(line,txt)
 write(rel,''.join(lines))
instrument('services/knowledge_import_pipeline.py',{'import_document':'import','_embed_planned':'embedding','_encode_batch':'embedding_batch'})
p=root/'services/knowledge_import_pipeline.py';s=p.read_text(encoding='utf-8');s=s.replace('            chunks = self.chunker.split(\n                content, document_id=document_id, category=category, source=source\n            )','            with span("chunk") as diagnostic:\n                chunks = self.chunker.split(\n                    content, document_id=document_id, category=category, source=source\n                )\n                diagnostic["count"] = len(chunks)');write(str(p.relative_to(root)),s)
instrument('services/vector_knowledge_retriever.py',{'retrieve':'retrieval','_embed':'query_embedding','_search':'search','_apply_ratio_filter':'filter'})
instrument('services/knowledge_rag.py',{'build_vector_retriever':'retriever_setup'})
instrument('services/interview_core.py',{'generate_next_question':'outcome'})
instrument('services/interview_followup.py',{'generate_followup':'outcome'})
for rel,fn in [('services/interview_agent.py','render_question_prompt'),('services/interview_next_agent.py','render_next_prompt'),('services/interview_followup_agent.py','render_followup_prompt')]:
 instrument(rel,{fn:'context'})
 s=(root/rel).read_text(encoding='utf-8').replace('    lines = format_knowledge_context(knowledge_context)','    lines = format_knowledge_context(knowledge_context)\n    context_ready(lines)');write(rel,s)
instrument('modules/ai/compat.py',{'chat_async':'generation'})
rel='modules/ai/llm_spark.py';s=(root/rel).read_text(encoding='utf-8');pos=s.index('import time');s=s[:pos]+'from common.rag_trace import context_submitted\n'+s[pos:];s=s.replace('            raw = await self._transport(prompt)','            context_submitted()\n            raw = await self._transport(prompt)');write(rel,s)
# SQL candidate count, not pretend an ANN candidate count is available.
instrument('services/vector_store_sql.py',{'search':'search'})
rel='services/vector_store_sql.py';s=(root/rel).read_text(encoding='utf-8');s=s.replace('        return select_matches(\n            candidates, query, top_k=top_k, min_score=min_score, store_name=self.name,\n        )','        with span("filter") as diagnostic:\n            diagnostic["candidates"] = len(candidates)\n            matches = select_matches(\n                candidates, query, top_k=top_k, min_score=min_score, store_name=self.name,\n            )\n            diagnostic["count"] = len(matches)\n            return matches');write(rel,s)
# Actual HTTP request root: binds parsing/import and retrieval/generation without changing signatures.
instrument('api/knowledge.py',{'upload_document':'request'})
rel='api/knowledge.py';s=(root/rel).read_text(encoding='utf-8');s=s.replace('        parsed = await run_in_threadpool(document_parser.parse_document, filename, data)','        with span("parse") as diagnostic:\n            parsed = await run_in_threadpool(document_parser.parse_document, filename, data)\n            diagnostic["chars"] = parsed.chars');write(rel,s)
# HTTP names confirmed from code separately; outcome decorators cover WS/direct service callers as well.
# Update frozen full-file fingerprints only for observed files; preserve all business assertions.
changed={}
for b in backup.rglob('*.py'):
 rel=b.relative_to(backup);old=b.read_text(encoding='utf-8');new=(root/rel).read_text(encoding='utf-8')
 changed[hashlib.sha256(old.encode()).hexdigest()]=hashlib.sha256(new.encode()).hexdigest()
for p in (root/'tests').glob('test_*.py'):
 s=p.read_text(encoding='utf-8');t=s
 for old,new in changed.items():t=t.replace(old,new)
 if t!=s:write(str(p.relative_to(root)),t)
print('instrumented and updated exact changed fingerprints')
