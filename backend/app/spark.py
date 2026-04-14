import os
import json
import websocket
from sparkai.spark_proxy.spark_auth import create_url

class SparkAPI:
    def __init__(self):
        self.app_id = os.getenv('SPARK_APP_ID', '')
        self.api_key = os.getenv('SPARK_API_KEY', '')
        self.api_secret = os.getenv('SPARK_API_SECRET', '')
        self.domain = 'x1'
        self.host = 'spark-api.xf-yun.com'
        self.path = '/v1/x1'

    def _create_url(self):
        return create_url(
            host=self.host,
            path=self.path,
            api_key=self.api_key,
            api_secret=self.api_secret,
            spark_url=f'wss://{self.host}{self.path}'
        )

    def chat(self, message):
        try:
            url = self._create_url()
            ws = websocket.create_connection(url, timeout=60)
            
            req_data = {
                "header": {
                    "app_id": self.app_id,
                    "uid": "user_001"
                },
                "parameter": {
                    "chat": {
                        "domain": self.domain,
                        "temperature": 0.5,
                        "max_tokens": 2048
                    }
                },
                "payload": {
                    "message": {
                        "text": [
                            {"role": "user", "content": message}
                        ]
                    }
                }
            }
            
            ws.send(json.dumps(req_data))
            
            response_text = ""
            while True:
                result = ws.recv()
                result_dict = json.loads(result)
                
                header = result_dict.get('header', {})
                if header.get('code', 0) != 0:
                    ws.close()
                    return json.dumps(header, ensure_ascii=False)
                
                choices = result_dict.get('payload', {}).get('choices', {}).get('text', [])
                if choices:
                    response_text += choices[0].get('content', '')
                
                status = header.get('status', 0)
                if status == 2:
                    break
            
            ws.close()
            return response_text
            
        except Exception as e:
            return f"API调用失败: {str(e)}"

    def chat_stream(self, message, callback):
        result = self.chat(message)
        if result:
            callback(result)