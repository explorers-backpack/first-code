import os,sys,subprocess,json,pathlib
sys.stdout.reconfigure(encoding='utf-8')
r=pathlib.Path.cwd(); out=r/'.rag-trace-audit'; env=dict(os.environ,AI_RUN_MODE='offline',PYTHONIOENCODING='utf-8')
env.pop('DATABASE_URL',None)
small=['test_rag_execution_trace.py','test_rag_retriever_config.py','test_vector_knowledge_retriever.py','test_rag_diagnostics.py','test_module_boundaries.py']
results=[]
for phase,names in [('focused',small),('full',[p.name for p in sorted((r/'backend/tests').glob('test_*.py'))])]:
 for name in names:
  try:
   p=subprocess.run([sys.executable,'tests/'+name],cwd=r/'backend',env=env,capture_output=True,timeout=120)
   log=(p.stdout+p.stderr).decode('utf-8','replace'); code=p.returncode
  except subprocess.TimeoutExpired: log='TIMEOUT 120s'; code=124
  (out/(phase+'-'+name+'.log')).write_text(log,encoding='utf-8')
  results.append(dict(phase=phase,test=name,code=code))
  (out/(phase+'-'+name+'.json')).write_text(json.dumps(results[-1]),encoding='utf-8')
  print(phase,name,code,flush=True)

sys.exit(1 if any(x['code'] for x in results) else 0)
