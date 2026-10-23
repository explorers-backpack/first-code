import os,sys,pathlib,asyncio
sys.path.insert(0,str(pathlib.Path('backend').resolve()));sys.path.insert(0,str(pathlib.Path('backend/tests').resolve()))
os.environ['DATABASE_URL']='sqlite+aiosqlite:///:memory:'
import regression_env
for p in pathlib.Path('backend').rglob('*.py'): compile(p.read_bytes(),str(p),'exec')
import main,models
from fastapi.testclient import TestClient
with TestClient(main.app) as c:
 assert c.get('/api/health').status_code==200
 assert c.get('/api/auth/me').status_code==401
 paths=[r.path for r in main.app.routes]
 for p in ['/api/interview/create','/api/interview/{session_id}/ws','/api/interview/{session_id}/ws-ticket','/api/knowledge/documents','/api/chat']: assert p in paths,p
print('PASS compile, imports, lifespan SQLite schema initialization, health, auth, route registration')
