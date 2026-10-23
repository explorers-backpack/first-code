import os,sys,pathlib,subprocess,json,ast
sys.stdout.reconfigure(encoding='utf-8')
r=pathlib.Path.cwd();out=r/'.extraction-audit';env=dict(os.environ,PYTHONIOENCODING='utf-8',AI_RUN_MODE='offline');env.pop('DATABASE_URL',None)
for name in ['test_module_boundaries.py','test_ai_layer.py','test_admin_role.py','test_ws_revocation.py','test_rag_production_route.py','test_agent_full_input.py']:
 p=subprocess.run([sys.executable,'tests/'+name],cwd=r/'backend',env=env,capture_output=True,timeout=120)
 (out/('focused-'+name+'.log')).write_bytes(p.stdout+p.stderr)
 print(name,p.returncode,flush=True)
 if p.returncode:print((p.stdout+p.stderr).decode('utf-8','replace')[-5000:],flush=True)
