import base64
import hashlib
import hmac
import json
import os
import struct
import sys
from datetime import datetime
from time import mktime
from urllib.parse import urlencode
from wsgiref.handlers import format_date_time

import httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

APPID = os.getenv("SPARK_APP_ID", "")
APIKEY = os.getenv("SPARK_API_KEY", "")
APISECRET = os.getenv("SPARK_API_SECRET", "")
HOST = "emb-cn-huabei-1.xf-yun.com"
PATH = "/"

print("creds present:", bool(APPID), bool(APIKEY), bool(APISECRET))


def signed_url() -> str:
    date = format_date_time(mktime(datetime.now().timetuple()))
    tmp = f"host: {HOST}\ndate: {date}\nPOST {PATH} HTTP/1.1"
    sig = base64.b64encode(
        hmac.new(APISECRET.encode(), tmp.encode(), digestmod=hashlib.sha256).digest()
    ).decode()
    origin = (
        f'api_key="{APIKEY}", algorithm="hmac-sha256", '
        f'headers="host date request-line", signature="{sig}"'
    )
    auth = base64.b64encode(origin.encode()).decode()
    return "https://" + HOST + PATH + "?" + urlencode(
        {"authorization": auth, "date": date, "host": HOST}
    )


def build_body(text: str, domain: str) -> dict:
    payload_text = base64.b64encode(
        json.dumps({"messages": [{"content": text, "role": "user"}]}, ensure_ascii=False).encode()
    ).decode()
    return {
        "header": {"app_id": APPID, "uid": "career-ai-probe", "status": 3},
        "parameter": {
            "emb": {
                "domain": domain,
                "feature": {"encoding": "utf8", "compress": "raw", "format": "plain"},
            }
        },
        "payload": {
            "messages": {
                "encoding": "utf8",
                "compress": "raw",
                "format": "json",
                "status": 3,
                "text": payload_text,
            }
        },
    }


def probe(text: str, domain: str) -> None:
    print("=" * 60)
    print(f"domain={domain} text={text!r}")
    try:
        r = httpx.post(
            signed_url(),
            json=build_body(text, domain),
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )
    except Exception as exc:
        print("  request failed:", type(exc).__name__, exc)
        return
    print("  status:", r.status_code)
    try:
        data = r.json()
    except Exception:
        print("  non-json body:", r.text[:400])
        return
    print("  header:", json.dumps(data.get("header"), ensure_ascii=False))
    feat = (data.get("payload") or {}).get("feature") or {}
    raw = feat.get("text")
    if not raw:
        print("  no feature.text; full:", json.dumps(data, ensure_ascii=False)[:600])
        return
    print("  feature keys:", sorted(feat.keys()), "| b64 len:", len(raw))
    dec = base64.b64decode(raw)
    print("  decoded bytes:", len(dec), "| first 48:", dec[:48])
    # try json
    try:
        arr = json.loads(dec.decode("utf-8"))
        if isinstance(arr, list) and arr and isinstance(arr[0], (int, float)):
            print("  -> JSON float array, dim =", len(arr), "| head:", arr[:5])
        else:
            print("  -> JSON but not float array:", str(arr)[:200])
    except Exception as exc:
        print("  json decode failed:", type(exc).__name__)
    # try raw float32
    if len(dec) % 4 == 0:
        vals = struct.unpack(f"<{len(dec)//4}f", dec)
        print("  -> float32 array, dim =", len(vals), "| head:", [round(v, 6) for v in vals[:5]])
    # try base64 inside json
    try:
        obj = json.loads(dec.decode("utf-8"))
        if isinstance(obj, dict):
            print("  dict keys:", sorted(obj.keys()))
    except Exception:
        pass


if __name__ == "__main__":
    probe("MySQL 索引的选择性与最左前缀原则", "para")
    probe("MySQL 索引的选择性与最左前缀原则", "query")
    probe("Redis 持久化 RDB 与 AOF 的区别", "para")
