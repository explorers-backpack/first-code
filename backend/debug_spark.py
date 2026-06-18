"""讯飞 X1 WebSocket 独立调试脚本"""
import os, json, hashlib, hmac, base64, datetime
from urllib.parse import urlencode
import websocket

APP_ID = os.getenv('SPARK_APP_ID', '0c00a4b1')
API_KEY = os.getenv('SPARK_API_KEY', '874db996e73544fe2d8637eef9572ea8')
API_SECRET = os.getenv('SPARK_API_SECRET', 'ZDQ0Y2QzMzQxM2EyMmYxZDg2NzViNjIx')

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
