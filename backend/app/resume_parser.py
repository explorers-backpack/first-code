import json
import re
from .spark import SparkAPI

RESUME_PARSE_PROMPT = """从以下简历文本提取信息，直接返回JSON格式，不要其他内容。

示例格式: {"name":"张三","major":"计算机科学","skills":["Python","Flask","MySQL"],"education":"本科","summary":"...","score":85}

【重要】技能点必须保留原始技术名词，不得翻译或本地化。例如：
- Python 不能写成"派森"或"python"
- Flask 不能写成"烧瓶"或"flask"
- MySQL 不能写成"mysql"或"数据库"
- 必须使用标准的英文技术名称

简历内容:
"""

def parse_resume(resume_text):
    if not resume_text or not resume_text.strip():
        return {
            "姓名": None,
            "专业": None,
            "技能": [],
            "学历": None,
            "摘要": None,
            "核心竞争力评分": None,
            "error": "简历文本为空"
        }
    
    spark = SparkAPI()
    prompt = RESUME_PARSE_PROMPT + resume_text
    
    try:
        result = spark.chat(prompt)
        
        match = re.search(r'\{[\s\S]+\}', result)
        if match:
            text = match.group()
            text = text.replace('```json', '').replace('```', '')
            parsed = json.loads(text)
            
            return {
                "姓名": parsed.get("name"),
                "专业": parsed.get("major"),
                "技能": parsed.get("skills", []),
                "学历": parsed.get("education"),
                "摘要": parsed.get("summary"),
                "核心竞争力评分": parsed.get("score")
            }
        
        return {"raw_response": result, "error": "解析失败"}
        
    except Exception as e:
        return {"error": str(e)}

def parse_resume_stream(resume_text, callback):
    if not resume_text or not resume_text.strip():
        callback({"error": "简历文本为空"})
        return
    
    spark = SparkAPI()
    prompt = RESUME_PARSE_PROMPT + resume_text
    spark.chat_stream(prompt, callback)