import os,sys,json,pathlib
sys.path[:0]=['backend','backend/tests']
os.environ['DATABASE_URL']='sqlite+aiosqlite:///:memory:'
import regression_env
import main
before=json.loads(pathlib.Path('.extraction-audit/openapi-after.json').read_text(encoding='utf-8'))
assert main.app.openapi()==before
for p in pathlib.Path('backend').rglob('*.py'):compile(p.read_bytes(),str(p),'exec')
from fastapi.testclient import TestClient
with TestClient(main.app) as c:
 assert c.get('/api/health').status_code==200
 assert c.get('/api/auth/me').status_code==401
print('PASS OpenAPI equal, compile, imports, SQLite startup, health/auth')
