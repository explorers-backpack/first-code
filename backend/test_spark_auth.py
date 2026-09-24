"""讯飞星火 v2.1 REST API 认证测试"""
import os, hashlib, hmac, base64, time, requests
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

def test_basic_auth():
    """方式1: Basic Auth (最简单)"""
    url = 'https://spark-api.xf-yun.com/v2.1/chat'
    body = {
        "header": {"app_id": APP_ID, "uid": "user_001"},
        "parameter": {"chat": {"domain": "generalv3.5", "temperature": 0.5, "max_tokens": 100}},
        "payload": {"message": {"text": [{"role": "user", "content": "hello"}]}}
    }
    auth = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth}",
    }
    resp = requests.post(url, json=body, headers=headers, timeout=30)
    return resp.status_code, resp.json()

def test_hmac_auth():
    """方式2: HMAC 签名 (讯飞 v2.1 官方 RFC 2616 方式)"""
    import datetime

    host = 'spark-api.xf-yun.com'
    path = '/v2.1/chat'
    url = f'https://{host}{path}'

    # RFC 2616 格式时间戳
    date_str = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')

    # 签名字符串：host + date (无 \\n)
    signature_str = host + '\n' + date_str
    signature = hmac.new(
        API_SECRET.encode(),
        signature_str.encode(),
        hashlib.sha1
    ).digest()
    signature_b64 = base64.b64encode(signature).decode()

    # Authorization = base64(sn app_id:signature)
    auth_str = f"SN {APP_ID}:{signature_b64}"
    auth_b64 = base64.b64encode(auth_str.encode()).decode()

    body = {
        "header": {"app_id": APP_ID, "uid": "user_001"},
        "parameter": {"chat": {"domain": "generalv3.5", "temperature": 0.5, "max_tokens": 100}},
        "payload": {"message": {"text": [{"role": "user", "content": "hello"}]}}
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_b64}",
        "Host": host,
        "Date": date_str,
    }

    resp = requests.post(url, json=body, headers=headers, timeout=30)
    return resp.status_code, resp.json()

if __name__ == '__main__':
    print("=== Test 1: Basic Auth ===")
    code, data = test_basic_auth()
    print(f"Status: {code}")
    print(data)

    print("\n=== Test 2: HMAC Auth ===")
    code, data = test_hmac_auth()
    print(f"Status: {code}")
    print(data)
