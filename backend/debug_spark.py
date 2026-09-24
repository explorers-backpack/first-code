"""讯飞 X1 WebSocket 独立调试脚本"""
import os, json, hashlib, hmac, base64, datetime
from urllib.parse import urlencode
import websocket
from dotenv import load_dotenv

# 凭据只从环境变量读取（backend/.env 或系统环境），源码中不保留任何默认值
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
APP_ID = os.getenv('SPARK_APP_ID', '')
API_KEY = os.getenv('SPARK_API_KEY', '')
API_SECRET = os.getenv('SPARK_API_SECRET', '')
_missing = [k for k, v in (('SPARK_APP_ID', APP_ID), ('SPARK_API_KEY', API_KEY),
                           ('SPARK_API_SECRET', API_SECRET)) if not v]
if _missing:
    raise SystemExit(f"缺少环境变量：{'、'.join(_missing)}；请在 backend/.env 中配置后重试。")

host = 'spark-api.xf-yun.com'
path = '/v1/x1'
spark_url = f'wss://{host}{path}'

# 认证 URL
now = datetime.datetime.utcnow()
date_str = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
print(f"Date: {date_str}")

signature_str = f"host: {host}\ndate: {date_str}\nGET {path} HTTP/1.1"
print(f"Signature string:\n{signature_str}")

signature = hmac.new(API_SECRET.encode(), signature_str.encode(), hashlib.sha256).digest()
signature_b64 = base64.b64encode(signature).decode()
print(f"Signature: {signature_b64}")

auth_origin = (
    f'api_key="{API_KEY}", '
    f'algorithm="hmac-sha256", '
    f'headers="host date request-line", '
    f'signature="{signature_b64}"'
)
authorization = base64.b64encode(auth_origin.encode()).decode()
print(f"Authorization: {authorization[:60]}...")

params = {"authorization": authorization, "date": date_str, "host": host}
url = f"{spark_url}?{urlencode(params)}"
print(f"\nWebSocket URL (first 100 chars):\n{url[:100]}...")

# 建立连接
print("\nConnecting...")
try:
    ws = websocket.create_connection(url, timeout=60)
    print("Connected!")

    req_data = {
        "header": {"app_id": APP_ID, "uid": "user_001"},
        "parameter": {"chat": {"domain": "x1", "temperature": 0.5, "max_tokens": 100}},
        "payload": {"message": {"text": [{"role": "user", "content": "hello"}]}}
    }

    ws.send(json.dumps(req_data))
    print("Sent request")

    response_text = ""
    while True:
        result = ws.recv()
        result_dict = json.loads(result)
        print(f"\nReceived frame: {json.dumps(result_dict, indent=2, ensure_ascii=False)[:500]}")

        header = result_dict.get("header", {})
        code = header.get("code", 0)
        print(f"Code: {code}, Status: {header.get('status')}")

        choices = result_dict.get("payload", {}).get("choices", {}).get("text", [])
        print(f"Choices: {choices}")

        if choices:
            response_text += choices[0].get("content", "")

        if header.get("status") == 2:
            break

    ws.close()
    print(f"\nFinal response: {response_text}")

except Exception as e:
    print(f"Error: {e}")
