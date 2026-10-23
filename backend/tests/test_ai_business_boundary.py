"""Real HTTP/service -> facade -> factory -> policy -> Spark provider -> fake wire."""
import asyncio
import json
import os
import pathlib
import sys
import unittest
from unittest.mock import patch, Mock
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
os.environ['DATABASE_URL'] = 'sqlite+aiosqlite:///:memory:'
import regression_env
import main
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool
from database import Base, get_db
from deps import get_current_user
from models import User, Job
from modules.ai.factory import register_spark_transport, get_spark_transport, reset_llm_provider
from modules.ai.telemetry import ai_call_scope, METRICS
from modules.ai.errors import AiConfigError
from modules.ai.policy import CallPolicy

REPLY = json.dumps({'question':'请结合你的项目经历，说明索引与缓存设计上的取舍。','question_type':'technical','topic':'综合','difficulty':'mid','expected_points':['取舍','量化'],'reason':'考察工程判断'}, ensure_ascii=False)
ENV = dict(AI_RUN_MODE='production', AI_LLM_PROVIDER='spark', SPARK_APP_ID='test', SPARK_API_KEY='test', SPARK_API_SECRET='test', AI_LLM_TIMEOUT='1', AI_LLM_MAX_ATTEMPTS='1', AI_LLM_BACKOFF_BASE='0')

class BusinessAiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = patch.dict(os.environ, ENV); self.env.start()
        self.original = get_spark_transport()
        self.calls = []
        self.reply = REPLY
        self.delay = False
        async def wire(prompt):
            self.calls.append(prompt)
            if self.delay: await asyncio.sleep(5)
            return self.reply
        register_spark_transport(wire)
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:', poolclass=StaticPool)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn: await conn.run_sync(Base.metadata.create_all)
        async with self.sessions() as db:
            user = User(username='boundary', email='boundary@example.test', password_hash='x', role='user')
            job = Job(job_name='后端开发', skills='Python,MySQL', city='北京', salary='面议')
            db.add_all([user, job]); await db.commit()
            self.uid, self.jid = user.id, job.id
        async def db_override():
            async with self.sessions() as db: yield db
        async def actor(): return {'user_id':self.uid, 'role':'user','email':'boundary@example.test'}
        main.app.dependency_overrides[get_db] = db_override
        main.app.dependency_overrides[get_current_user] = actor
        self.client = AsyncClient(transport=ASGITransport(app=main.app), base_url='http://test')

    async def asyncTearDown(self):
        await self.client.aclose()
        main.app.dependency_overrides.clear()
        await self.engine.dispose()
        register_spark_transport(self.original)
        self.env.stop(); reset_llm_provider()

    async def chat(self, message='Python 后端开发职业规划'):
        return await self.client.post('/api/chat',json={'message':message,'jobs':[{'job_name':'开发','city':'北京','skills':'Python'}]})

    async def question(self):
        r=await self.client.post('/api/interview/create',json={'job_id':self.jid,'difficulty':'mid','total_questions':3})
        self.assertEqual(r.status_code,200,r.text)
        sid=r.json()['session']['id']
        r=await self.client.post(f'/api/interview/{sid}/start');self.assertEqual(r.status_code,200,r.text)
        return await self.client.post(f'/api/interview/{sid}/next-question',json={'use_rag':False})

    async def test_success_interview_and_chat_use_registered_provider(self):
        before=METRICS.snapshot()['ai_calls_total']
        r=await self.question();self.assertEqual(r.status_code,200,r.text)
        self.assertTrue(r.json()['ok'],r.text)
        self.assertIn('索引',r.json()['question'])
        self.assertGreater(len(self.calls),0)
        n=len(self.calls);self.reply='职业建议：结合后端项目展示你的工程能力。'
        r=await self.chat();self.assertEqual(r.json()['chat_answer'],self.reply)
        self.assertEqual(len(self.calls),n+1)
        self.assertEqual(METRICS.snapshot()['ai_calls_total']-before,len(self.calls))

    async def test_interview_auth_error_is_classified_no_retry(self):
        self.reply='API调用失败: 401 authentication failed'
        os.environ['AI_LLM_MAX_ATTEMPTS']='3'
        with self.assertLogs('career_ai.ai',level='INFO') as logs:
            r=await self.question()
        self.assertEqual(r.status_code,200,r.text)
        self.assertFalse(r.json()['ok'],r.text)
        self.assertIn('agent_failed',r.json()['errors'])
        joined='\n'.join(logs.output)
        self.assertIn('"error_kind": "auth"',joined)
        self.assertNotIn('ai_call_retry',joined)

    async def test_chat_timeout_retries_and_falls_back(self):
        self.delay=True;os.environ['AI_LLM_MAX_ATTEMPTS']='2'
        with self.assertLogs('career_ai.ai',level='INFO') as logs:
            r=await self.chat()
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.json()['chat_answer'],'抱歉，暂时无法获取分析结果。')
        self.assertEqual(len(self.calls),2)
        self.assertIn('"error_kind": "timeout"','\n'.join(logs.output))
        self.assertIn('ai_call_retry','\n'.join(logs.output))

    async def test_boundary_blocks_before_transport_on_http_path(self):
        before=METRICS.snapshot()['ai_boundary_blocks_total']
        r=await self.chat('x'*21000)
        self.assertEqual(r.json()['chat_answer'],'抱歉，暂时无法获取分析结果。')
        self.assertEqual(self.calls,[])
        self.assertEqual(METRICS.snapshot()['ai_boundary_blocks_total'],before+1)

    async def test_missing_credentials_after_cache_warmup(self):
        await self.chat();n=len(self.calls)
        os.environ['SPARK_API_KEY']=''
        with self.assertLogs('career_ai.ai',level='INFO') as logs: r=await self.chat()
        self.assertEqual(r.json()['chat_answer'],'抱歉，暂时无法获取分析结果。')
        self.assertEqual(len(self.calls),n)
        self.assertIn('"error_kind": "mode"','\n'.join(logs.output))

    async def test_startup_rejects_missing_config_or_transport_before_database(self):
        for changes, transport in [({'SPARK_API_KEY':''},self.original), ({},None), ({'AI_LLM_TIMEOUT':'nan'},self.original)]:
            with patch.dict(os.environ,changes):
                register_spark_transport(transport)
                engine = Mock()
                engine.begin.side_effect = AssertionError('must not touch DB')
                with patch.object(main,'engine',engine):
                    with self.assertRaises(AiConfigError):
                        async with main.lifespan(main.app): pass
                    engine.begin.assert_not_called()

    async def test_cancellation_is_not_retried_or_converted_to_answer(self):
        self.delay=True
        task=asyncio.create_task(main.spark_api.chat_async('cancel'))
        while not self.calls: await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(len(self.calls),1)

    async def test_offline_explicit_fallback_no_transport(self):
        os.environ['AI_RUN_MODE']='offline'
        os.environ['AI_LLM_MAX_ATTEMPTS']='3'
        before=METRICS.snapshot()['ai_calls_total']
        r=await self.chat()
        self.assertEqual(r.json()['chat_answer'],'抱歉，暂时无法获取分析结果。')
        self.assertEqual(self.calls,[])
        self.assertEqual(METRICS.snapshot()['ai_calls_total']-before,1)

    async def test_chat_rate_limit_then_success_uses_real_retry_policy(self):
        os.environ['AI_LLM_MAX_ATTEMPTS']='2'
        async def wire(prompt):
            self.calls.append(prompt)
            return 'API调用失败: 429 quota' if len(self.calls)==1 else '恢复后的职业建议'
        register_spark_transport(wire)
        with self.assertLogs('career_ai.ai',level='INFO') as logs:
            r=await self.chat()
        self.assertEqual(r.json()['chat_answer'],'恢复后的职业建议')
        self.assertEqual(len(self.calls),2)
        self.assertIn('"error_kind": "rate_limit"','\n'.join(logs.output))

    async def test_resume_analysis_upstream_failure_exposes_typed_fallback(self):
        self.reply='API调用失败: 503 unavailable'
        r=await self.client.post('/api/resume/analyze', files={'file':('resume.txt','Python 后端开发，熟悉 MySQL，完成项目接口开发。'.encode(),'text/plain')})
        self.assertEqual(r.status_code,200,r.text)
        self.assertFalse(r.json()['ai_available'])
        self.assertEqual(r.json()['ai_error']['kind'],'unavailable')
        self.assertIn('规则引擎',r.json()['chat_answer'])
        self.assertEqual(len(self.calls),1)

    async def test_nonfinite_or_fractional_policy_rejected(self):
        for key,value in [('AI_LLM_TIMEOUT','nan'),('AI_LLM_TIMEOUT','inf'),('AI_LLM_MAX_ATTEMPTS','1.5')]:
            with self.assertRaises(AiConfigError): CallPolicy.from_env({key:value})

if __name__=='__main__': unittest.main()
