from pathlib import Path
import hashlib
p=Path('backend/tests/test_interview_eval_benchmark.py')
b=Path('.extraction-audit/before/tests/test_interview_eval_benchmark.py');b.write_bytes(p.read_bytes())
s=p.read_text(encoding='utf-8')
for rel in ['services/interview_agent.py','services/interview_planner.py']:
 old=Path('.extraction-audit/before',rel).read_text(encoding='utf-8')
 new=Path('backend',rel).read_text(encoding='utf-8')
 s=s.replace(hashlib.sha256(old.encode()).hexdigest(),hashlib.sha256(new.encode()).hexdigest())
s=s.replace('FROZEN_SOURCES: Tuple','#: 仅更新默认 LLM 端口接线后的 Agent/Planner 指纹；保留全部评测断言。\nFROZEN_SOURCES: Tuple')
p.write_text(s,encoding='utf-8')
