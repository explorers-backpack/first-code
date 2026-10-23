import os,sys,json,pathlib
sys.path[:0]=['backend','backend/tests']
os.environ['DATABASE_URL']='sqlite+aiosqlite:///:memory:'
import regression_env
import main
pathlib.Path('.extraction-audit/openapi-before.json').write_text(json.dumps(main.app.openapi(),ensure_ascii=False,sort_keys=True),encoding='utf-8')
print('OpenAPI baseline saved')
