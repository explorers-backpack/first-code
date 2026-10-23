import ast,json,os,pathlib,sys
sys.path[:0]=['backend','backend/tests']
os.environ['DATABASE_URL']='sqlite+aiosqlite:///:memory:'
import regression_env
import main
r=pathlib.Path('.extraction-audit')
before=json.loads((r/'openapi-before.json').read_text(encoding='utf-8'))
after=main.app.openapi()
(r/'openapi-after.json').write_text(json.dumps(after,ensure_ascii=False,sort_keys=True),encoding='utf-8')
assert before==after,'OpenAPI changed'
old=ast.parse((r/'before/main.py').read_text(encoding='utf-8'))
new=ast.parse(pathlib.Path('backend/modules/ai/spark_transport.py').read_text(encoding='utf-8'))
for name in ['SparkAPI','_make_url']:
 a=next(n for n in old.body if getattr(n,'name',None)==name)
 b=next(n for n in new.body if getattr(n,'name',None)==name)
 assert ast.dump(a)==ast.dump(b),name
new=ast.parse(pathlib.Path('backend/modules/admin/service.py').read_text(encoding='utf-8'))
class Normalize(ast.NodeTransformer):
 def visit_Raise(self,node):
  if isinstance(node.exc,ast.Call) and isinstance(node.exc.func,ast.Name) and node.exc.func.id=='HTTPException':
   return ast.parse('raise UserNotFound("用户不存在")').body[0]
  return node
for name in ['get_user_logs','add_job','kick_user_sessions']:
 a=next(n for n in old.body if getattr(n,'name',None)==name)
 b=next(n for n in new.body if getattr(n,'name',None)==name)
 a=Normalize().visit(a)
 assert [ast.dump(x) for x in a.body]==[ast.dump(x) for x in b.body],name
for p in pathlib.Path('backend').rglob('*.py'):
 compile(p.read_bytes(),str(p),'exec')
from fastapi.testclient import TestClient
with TestClient(main.app) as c:
 assert c.get('/api/health').status_code==200
 assert c.get('/api/admin/user-logs').status_code==401
print('PASS: complete OpenAPI equal; transport AST equal; admin business bodies equal except HTTP error mapping; compile, imports, SQLite lifespan and health/auth smoke')
