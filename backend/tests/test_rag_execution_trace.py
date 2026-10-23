"""RAG diagnostics from actual parser/chunker/hash embeddings/SQL retrieval/HTTP generation.
Only external LLM transport is replaced; hash-local is explicitly not semantic quality validation.
"""
import asyncio
import ast
import json
import logging
import os
import pathlib
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL']='sqlite+aiosqlite:///:memory:'
import regression_env
import main
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy import select,func
from database import Base,get_db
from deps import get_current_user
from models import User,Job,KnowledgeChunk
from modules.ai.factory import register_spark_transport,get_spark_transport,reset_llm_provider
from services.vector_store_sql import SqlAlchemyVectorStore
from services.embedding_service import HashEmbeddingService
from common.rag_trace import reference,trace_scope,span

SECRET='SECRET_SENTINEL_abc123'
PRIVATE='private.person@example.test'
TEXT=('Python MySQL 索引与缓存设计，事务隔离与性能优化。'+SECRET+' '+PRIVATE+'\n')*28
REPLY=json.dumps({'question':'请结合项目经历说明 MySQL 索引和缓存设计的取舍。','question_type':'technical','topic':'综合','difficulty':'mid','expected_points':['取舍','量化'],'reason':'工程判断'},ensure_ascii=False)
ENV=dict(AI_RUN_MODE='production',AI_LLM_PROVIDER='spark',SPARK_APP_ID=SECRET,SPARK_API_KEY=SECRET,SPARK_API_SECRET=SECRET,AI_LLM_TIMEOUT='1',AI_LLM_MAX_ATTEMPTS='1',RAG_VECTOR_STORE='sql',RAG_MIN_SCORE='0',RAG_MIN_SCORE_RATIO='',RAG_TOP_K='3',EMBEDDING_PROVIDER='hash')

class Sink(logging.Handler):
    def __init__(self):super().__init__();self.events=[];self.raw=[]
    def emit(self,record):
        self.raw.append(record.getMessage());self.events.append(json.loads(record.getMessage()))

class RagTraceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env=patch.dict(os.environ,ENV);self.env.start()
        self.original=get_spark_transport();self.prompts=[];self.reply=REPLY
        async def wire(prompt):self.prompts.append(prompt);return self.reply
        register_spark_transport(wire)
        self.engine=create_async_engine('sqlite+aiosqlite:///:memory:',poolclass=StaticPool)
        self.sessions=async_sessionmaker(self.engine,expire_on_commit=False)
        async with self.engine.begin() as conn:await conn.run_sync(Base.metadata.create_all)
        async with self.sessions() as db:
            user=User(username='trace',email=PRIVATE,password_hash='x',role='admin')
            job=Job(job_name='Python MySQL 开发',skills='Python,MySQL',city='北京',salary='面议')
            db.add_all([user,job]);await db.commit();self.uid,self.jid=user.id,job.id
        async def db_override():
            async with self.sessions() as db:yield db
        async def actor():return {'user_id':self.uid,'role':'admin','email':PRIVATE}
        main.app.dependency_overrides[get_db]=db_override
        main.app.dependency_overrides[get_current_user]=actor
        self.client=AsyncClient(transport=ASGITransport(app=main.app),base_url='http://test')
        self.log=logging.getLogger('career_ai.rag.trace');self.level=self.log.level
        self.sink=Sink();self.log.addHandler(self.sink);self.log.setLevel(logging.INFO)

    async def asyncTearDown(self):
        self.log.removeHandler(self.sink);self.log.setLevel(self.level)
        await self.client.aclose();main.app.dependency_overrides.clear();await self.engine.dispose()
        register_spark_transport(self.original);self.env.stop();reset_llm_provider()

    async def upload(self,text=TEXT):
        return await self.client.post('/api/knowledge/documents/upload',files={'file':(PRIVATE+'.txt',text.encode(),'text/plain')},data={'category':'technical','source':'https://'+PRIVATE+'/?key='+SECRET})

    async def session(self):
        r=await self.client.post('/api/interview/create',json={'job_id':self.jid,'difficulty':'mid','total_questions':3})
        self.assertEqual(r.status_code,200,r.text);sid=r.json()['session']['id']
        r=await self.client.post(f'/api/interview/{sid}/start');self.assertEqual(r.status_code,200,r.text)
        return sid

    async def question(self,sid,**kwargs):
        return await self.client.post(f'/api/interview/{sid}/next-question',json={'use_rag':True,'min_score':0,**kwargs})

    def events(self,stage):return [e for e in self.sink.events if e['stage']==stage]
    def clear(self):self.sink.events.clear();self.sink.raw.clear();self.prompts.clear()
    def assert_private(self):
        log='\n'.join(self.sink.raw)
        for value in (SECRET,PRIVATE,TEXT,'https://','password','Bearer '):self.assertNotIn(value,log)
        for e in self.sink.events:self.assertGreaterEqual(e['elapsed_ms'],0)

    async def test_upload_real_retrieval_and_generation_correlate(self):
        r=await self.upload();self.assertEqual(r.status_code,200,r.text);self.assertTrue(r.json()['ok'],r.text)
        did=r.json()['document_id'];count=r.json()['chunk_count']
        self.assertEqual(self.events('chunk')[0]['count'],count)
        self.assertEqual(self.events('embedding')[0]['embedded'],count)
        self.assertEqual(len({e['trace_id'] for e in self.sink.events}),1)
        self.assertTrue(self.events('parse')[0]['chars']>0)
        async with self.sessions() as db:
            actual=await db.scalar(select(func.count()).select_from(KnowledgeChunk))
        self.assertEqual(actual,count)
        self.assert_private()
        sid=await self.session();self.clear()
        r=await self.question(sid);self.assertEqual(r.status_code,200,r.text);self.assertTrue(r.json()['ok'],r.text)
        recalled=self.events('retrieval')[-1]
        self.assertGreater(recalled['count'],0)
        self.assertEqual(recalled['count'],r.json()['knowledge_chunks'])
        self.assertEqual(recalled['rerank'],'not_supported')
        self.assertTrue(all(x['document_ref']==reference('document',did) for x in recalled['results']))
        self.assertTrue(all('score' in x for x in recalled['results']))
        self.assertGreater(len(self.prompts),0);self.assertIn(SECRET,self.prompts[-1])
        self.assertTrue(self.events('outcome')[-1]['context_submitted'])
        self.assertTrue(self.events('outcome')[-1]['generation_ok'])
        self.assertEqual(len({e['trace_id'] for e in self.sink.events}),1)
        self.assertTrue(self.events('query_embedding'));self.assertTrue(self.events('search'));self.assertTrue(self.events('filter'))
        self.assert_private()

    async def test_empty_and_disabled_do_not_claim_context(self):
        sid=await self.session();self.clear()
        r=await self.question(sid);self.assertTrue(r.json()['ok'],r.text)
        self.assertEqual(self.events('retrieval')[-1]['count'],0)
        self.assertFalse(self.events('outcome')[-1]['context_submitted'])
        self.clear();r=await self.question(sid,use_rag=False)
        self.assertTrue(r.json()['ok'],r.text);self.assertEqual(self.events('retrieval'),[])
        self.assertFalse(self.events('outcome')[-1]['rag_requested'])
        self.assertFalse(self.events('outcome')[-1]['context_submitted'])

    async def test_storage_failure_degrades_without_leaking_exception(self):
        sid=await self.session();self.clear()
        with patch.object(SqlAlchemyVectorStore,'search',side_effect=RuntimeError(SECRET+' '+PRIVATE)):
            r=await self.question(sid)
        self.assertTrue(r.json()['ok'],r.text)
        self.assertIn('knowledge_retrieval_failed',r.json()['warnings'])
        self.assertEqual(self.events('search')[-1]['status'],'failed')
        self.assertIn(self.events('search')[-1]['failure_code'],('unavailable','execution_failed'))
        self.assertFalse(self.events('outcome')[-1]['context_submitted']);self.assert_private()

    async def test_parse_and_embedding_failure_report_actual_stage(self):
        r=await self.upload('');self.assertEqual(r.status_code,400)
        self.assertEqual(self.events('parse')[-1]['status'],'failed')
        self.assertEqual(self.events('embedding'),[]);self.assert_private();self.clear()
        with patch.object(HashEmbeddingService,'_embed_one',side_effect=RuntimeError(SECRET+' '+PRIVATE)):
            r=await self.upload()
        self.assertEqual(r.status_code,200,r.text);self.assertFalse(r.json()['ok'])
        self.assertEqual(self.events('embedding')[-1]['status'],'failed')
        self.assertEqual(self.events('embedding')[-1]['embedded'],0)
        self.assert_private()

    async def test_generation_failure_does_not_hide_context_submission(self):
        await self.upload();sid=await self.session();self.clear()
        self.reply='API调用失败: 401 auth '+SECRET
        with self.assertLogs('career_ai.ai',level='INFO') as ai_logs:
            r=await self.question(sid)
        self.assertNotIn(SECRET,'\n'.join(ai_logs.output))
        self.assertFalse(r.json()['ok'],r.text)
        self.assertTrue(self.events('outcome')[-1]['context_submitted'])
        self.assertFalse(self.events('outcome')[-1]['generation_ok'])
        self.assertEqual(self.events('generation')[-1]['failure_code'],'auth')
        self.assert_private()

    async def test_followup_uses_real_retrieval_context(self):
        await self.upload();sid=await self.session();self.clear()
        r=await self.client.post(f'/api/interview/{sid}/follow-up',json={
            'answer_text':'我使用 MySQL 索引和缓存优化查询，降低接口延迟。',
            # Hash vectors can have negative similarity; this wiring test must
            # retain real candidates rather than apply a semantic-quality threshold.
            'question_no':1,'use_rag':True,'min_score':-1})
        self.assertEqual(r.status_code,200,r.text)
        self.assertTrue(self.events('retrieval'),r.text)
        self.assertGreater(self.events('retrieval')[-1]['count'],0)
        self.assertIn(SECRET,self.prompts[-1])
        self.assertTrue(self.events('outcome')[-1]['context_submitted'])
        self.assert_private()

    async def test_context_prepared_but_boundary_rejected_not_submitted(self):
        from common.rag_trace import observed,context_ready
        @observed('context')
        def render():
            context_ready(['evidence'])
            return 'knowledge','x'*21000
        with trace_scope():
            _,prompt=render()
            result=await main.spark_api.chat_async(prompt)
        self.assertTrue(result.startswith('API调用失败'))
        self.assertEqual(self.prompts,[])
        self.assertTrue(self.events('generation')[-1]['context_rendered'])
        self.assertFalse(self.events('generation')[-1]['context_submitted'])
        self.assertEqual(self.events('generation')[-1]['failure_code'],'boundary')

    async def test_trace_has_only_standard_library_dependencies(self):
        source=pathlib.Path(__file__).resolve().parents[1]/'common/rag_trace.py'
        for node in ast.walk(ast.parse(source.read_text(encoding='utf-8-sig'))):
            names=[a.name for a in node.names] if isinstance(node,ast.Import) else [node.module] if isinstance(node,ast.ImportFrom) else []
            for name in names:self.assertIn(name.split('.')[0],sys.stdlib_module_names)

    async def test_trace_isolation_and_logging_failure_noninterference(self):
        async def work(delay,count):
            with trace_scope():
                with span('chunk') as d:
                    await asyncio.sleep(delay);d['count']=count
        await asyncio.gather(work(.01,1),work(0,2))
        events=self.events('chunk');self.assertEqual(len({e['trace_id'] for e in events}),2)
        self.assertEqual(sorted(e['count'] for e in events),[1,2])
        sid=await self.session()
        with patch.object(self.log,'info',side_effect=RuntimeError('broken logging')):
            r=await self.question(sid)
        self.assertEqual(r.status_code,200);self.assertTrue(r.json()['ok'],r.text)

if __name__=='__main__':unittest.main()
