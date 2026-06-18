"""
讯飞星火 v2.1 REST API 认证
参考讯飞官方文档: https://www.xfyun.cn/doc/spark/Web.html
"""
import os, hashlib, hmac, base64, datetime, requests

APP_ID = os.getenv('SPARK_APP_ID', '0c00a4b1')
API_KEY = os.getenv('SPARK_API_KEY', '874db996e73544fe2d8637eef9572ea8')
API_SECRET = os.getenv('SPARK_API_SECRET', 'ZDQ0Y2QzMzQxM2EyMmYxZDg2NzViNjIx')

host = 'spark-api.xf-yun.com'
path = '/v2.1/chat'
url = f'https://{host}{path}'

# RFC 2616 Date 格式
date_str = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

# 签名原文: "host: {host}\ndate: {date}\nGET {path} HTTP/1.1"
signature_str = f"host: {host}\ndate: {date_str}\nGET {path} HTTP/1.1"

# HMAC-SHA256 签名
signature = hmac.new(
    API_SECRET.encode('utf-8'),
    signature_str.encode('utf-8'),
    hashlib.sha256
).digest()
signature_b64 = base64.b64encode(signature).decode('utf-8')

# 拼接 authorization_origin: api_key="{api_key}", algorithm="hmac-sha256", headers="host date request-line", signature="{signature_b64}"
authorization_origin = (
    f'api_key="{API_KEY}", '
    f'algorithm="hmac-sha256", '
    f'headers="host date request-line", '
    f'signature="{signature_b64}"'
)
authorization = base64.b64encode(authorization_origin.encode('utf-8')).decode('utf-8')

body = {
    "header": {"app_id": APP_ID, "uid": "user_001"},
    "parameter": {
        "chat": {
            "domain": "generalv3.5",
            "temperature": 0.5,
            "max_tokens": 100
        }
    },
    "payload": {
        "message": {"text": [{"role": "user", "content": "hello"}]}
    }
}

headers = {
    "Content-Type": "application/json",
    "Authorization": authorization,
    "Host": host,
    "Date": date_str,
}

print(f"URL: {url}")
print(f"Date: {date_str}")
print(f"Signature str:\n{signature_str}")
print(f"Signature (first 30 chars): {signature_b64[:30]}...")
print(f"Authorization (first 50 chars): {authorization[:50]}...")
print()

resp = requests.post(url, json=body, headers=headers, timeout=30)
print(f"Status: {resp.status_code}")
print(f"Response: {resp.text[:500]}")
