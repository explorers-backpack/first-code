# Career.ai 数字人 Avatar Provider 接入说明

> 版本：v1.0 ｜ 对应代码：`backend/services/avatar_provider.py` /
> `backend/services/avatar_provider_iflytek.py` / `backend/services/avatar_interview.py`
> ｜ 回归基线：**60 套**（含新增 `test_avatar_provider`）

本文说明「`interview_mode="avatar"` 这条通道怎么把交付内容交给数字人」，
以及**把 Mock 换成真实讯飞虚拟人需要改哪几个常量**。

---

## 0. 一句话结论

- **不配任何 `AVATAR_*` ⇒ 通道走 Mock**（离线、确定性、不产生真实音视频），
  avatar 模式的面试**照常跑完**，交付内容与 text 模式**逐字段一致**。
- 数字人故障（配置错 / 网络错 / 上游业务错）**一律降级为纯文本交付**，
  **绝不阻断面试**——这是本层最硬的一条约定。
- 讯飞实现按「**HTTP 协议**」写，**不引入官方 SDK**；鉴权与既有
  `main.SparkAPI` / `embedding_provider_spark` **同族**（HMAC-SHA256）。
- **端点路径与响应字段名是占位**，必须与官方文档核对后改
  `avatar_provider_iflytek.py` 顶部的常量（见 §5）。

---

## 1. 分层位置（严格单向）

```
api/interview.py                                   ← HTTP 路由（8 个，未改）
    └── services/interview_service.py              ← 唯一接触 DB 处；按 interview_mode 分流
            └── services/avatar_interview.py       ← 【通道层】阶段判别 + 驱动 + 降级
                    └── services/avatar_provider.py        ← 【接口层】抽象 + 值对象 + Mock
                            └── services/avatar_provider_iflytek.py  ← 【实现层】讯飞 HTTP
```

分层硬约束（`test_avatar_channel.py` [7] 节用 AST 锁住）：

| 约束 | 含义 |
|---|---|
| `interview_service` 只 import `services.avatar_interview` | **不得**直接 import provider |
| `avatar_interview` 顶层只 import 标准库 + `sqlalchemy` + `avatar_provider` | 具体讯飞实现**延迟到 `_build_provider()` 函数体内** import ⇒ 换厂商不用改顶层 |
| `interview_core` 源码**不含** `avatar_interview` | 业务层不反向依赖通道层 |
| `avatar_provider.py` **零第三方依赖** | 顶层只有 `__future__` / `abc` / `dataclasses` / `typing` |
| `httpx` 只在 `HttpxAvatarTransport.post_json` **函数体内** import | 未装 httpx 也能 import 整个后端 |

**为什么 provider 接口要「零第三方依赖」**：与 `embedding_service` / `vector_store`
同源——真实实现必然要发 HTTP，塞进接口模块会当场破坏「接口层可离线单测」。

---

## 2. 接口契约（六个能力）

`services/avatar_provider.AvatarProvider`（`ABC`），**六个方法全部 `async`**：

| 方法 | 签名 | 返回 | 说明 |
|---|---|---|---|
| `create_session` | `(*, session_ref, avatar_id="", vcn="")` | `AvatarSession` | 创建数字人 Session。**必须对同一 `session_ref` 幂等** |
| `initialize` | `(avatar_session_id, *, stream=True)` | `AvatarSession` | 初始化数字人；`stream=True` 时返回 `stream_url` |
| `speak` | `(avatar_session_id, text, *, interrupt=False)` | `AvatarSpeech` | 文本驱动（TTS 播放） |
| `fetch_answer` | `(avatar_session_id, *, timeout_ms=0)` | `AvatarAnswer` | 取用户语音回答（ASR 结果） |
| `status` | `(avatar_session_id)` | `AvatarStatus` | 状态查询 |
| `stop` | `(avatar_session_id)` | `AvatarStatus` | 停止（关闭）会话 |

**为什么 `create_session` 必须幂等**：上层在 `start` / `answer` / `end`
**每个交互入口都会调用它**（不额外持久化会话 id）。若每次新建，一场面试会散出
多场数字人会话。Mock 用 `self._refs[str(session_ref)]` 做幂等。

**失败一律抛 `AvatarProviderError` 子类，绝不返回 `None`**。

### 2.1 值对象（`@dataclass(frozen=True)`，键集恒定）

| 值对象 | 键（顺序即 `to_dict` 顺序） |
|---|---|
| `AvatarSession` | `avatar_session_id` `provider` `status` `stream_url` `avatar_id` `vcn` `raw` |
| `AvatarSpeech` | `avatar_session_id` `text` `status` `stream_url` `duration_ms` `raw` |
| `AvatarAnswer` | `avatar_session_id` `status` `text` `audio_url` `raw` |
| `AvatarStatus` | `avatar_session_id` `status` `initialized` `speaking` `raw` |

- `from_dict`：**未知键忽略 / 缺失取默认 / 类型不符抛 `AvatarValueError`**。
- `to_dict()` 的 `raw` 是**浅拷贝**（改返回值不污染内部）。
- `avatar_session_id` 必填非空；`duration_ms` 必须 `>= 0` 的 `int`。

### 2.2 状态常量

```python
# 会话（AvatarSession.status / AvatarStatus.status）
SESSION_CREATED     = "created"
SESSION_INITIALIZED = "initialized"
SESSION_SPEAKING    = "speaking"
SESSION_STOPPED     = "stopped"
SESSION_FAILED      = "failed"

# 语音回答（AvatarAnswer.status）
ANSWER_PENDING = "pending"   # 用户还没说完
ANSWER_READY   = "ready"     # 拿到文本
ANSWER_EMPTY   = "empty"     # 这一轮没有语音
```

### 2.3 错误分类（三档，处理方式完全不同）

| 异常 | 继承 | 语义 | 处置 |
|---|---|---|---|
| `AvatarProviderConfigError` | `+ValueError` | **配置**错（缺凭据 / 非法 timeout / 非法 provider 取值） | **构造期就报**，不重试；修配置 |
| `AvatarProviderUnavailableError` | `+RuntimeError` | **上游不可用**（网络 / 超时 / 5xx） | 可重试，也可降级 |
| `AvatarProviderResponseError` | — | 上游**业务错误**（`header.code != 0`）或响应不可解析 | 暴露日志；降级 |
| `AvatarValueError` | `+ValueError` | 值对象构造 / 解析错 | 编程错误，早失败 |

`ConfigError` 同时是 `ValueError` 是刻意的：**「部署配错了」不该被伪装成「数字人连不上」**。

---

## 3. 通道层：`avatar_interview.py`

### 3.1 阶段判别（靠 payload 形状，不改签名）

`enter(db, session, payload)` 的签名**刻意固定**（没有 `stage` 参数）。四种交互入口的
交付内容**键集互不相同**，因此用「标志键」判别阶段：

| 阶段 | 标志键 | 驱动动作 |
|---|---|---|
| `start` | `job_name` | `create_session` → `initialize` → `speak(第一题)` |
| `current_question` | `session_id` + `status` | `create_session` → `initialize` → `speak(当前题)` |
| `answer` | `answer` | `create_session` → `speak(下一题)` → `fetch_answer(ASR)` |
| `end` | `report` | `create_session` → `stop()` |
| `unknown` | 其它（如 `/create` 的交付内容） | **不做任何事** |

纯函数（可单独单测）：`classify_payload(payload)` / `spoken_text(stage, payload)` /
`drive(provider, session, payload)`。

### 3.2 元信息

```python
CHANNEL       = "avatar"
STATUS        = STATUS_READY          # "ready"（provider 层已接入）
INTEGRATIONS  = {"iflytek": True, "asr": True, "tts": True}
```

> `INTEGRATIONS` 为 `True` 表示「**通道已具备该能力**」，**不等于**「真实可用」——
> 真实可用性取决于 provider 配置（未配置时是 Mock，不产生真实音视频）。
> 这一点与 `embedding_status.semantic_enabled` 的口径一致。

### 3.3 降级（核心原则）

```python
async def enter(db, session, payload):
    try:
        provider = get_provider()          # 解析失败 → 降级
    except Exception:
        return payload
    try:
        await drive(provider, session, payload)   # 上游失败 → 降级
    except Exception:
        return payload
    return payload                          # ← 始终是**同一个对象**
```

⇒ **`enter` 的返回值与入参 `payload` 永远是同一个对象**。
因此 text 与 avatar 两种模式在 provider 不可用时**交付内容逐字节一致**。

**`enter` 不往 payload 里塞任何新字段**。原因：payload 最终要经
`schemas/interview.py` 的响应模型序列化，给共享模型加可选字段会让 **text 响应也多出
`null` 键**，等于改了默认路径的契约。把数字人信息暴露给前端应当是**独立的、显式的**
一步（如新增 `POST /api/interview/{id}/avatar/start` 端点），不在本次范围内。

### 3.4 进程内 provider 缓存

```python
get_provider()                    # 首次按环境变量解析并缓存
reset_provider(provider=None)     # 清空 / 替换缓存（测试用）
query_status(avatar_session_id)   # 只读；失败返回 None，不抛
```

**为什么要缓存**：Mock 需要**跨调用保持会话状态**（幂等靠内存 `_refs`），
每次新建会让 `create_session` 失去幂等性。

---

## 4. 环境变量表

| 变量 | 默认 | 取值 | 说明 |
|---|---|---|---|
| `AVATAR_PROVIDER` | *（空）* | `""` / `mock` / `iflytek` / 其它 | **空 / `mock` → Mock**；`iflytek` → 讯飞（**显式 opt-in**）；**其它 → 启动即报错**（报错不静默） |
| `AVATAR_IFLYTEK_APP_ID` | *（空）* | 字符串 | 讯飞控制台「AI 虚拟人」AppId |
| `AVATAR_IFLYTEK_API_KEY` | *（空）* | 字符串 | APIKey（**不进日志 / 不进 `repr`**） |
| `AVATAR_IFLYTEK_API_SECRET` | *（空）* | 字符串 | APISecret（同上） |
| `AVATAR_IFLYTEK_HOST` | `avatar.cn-huabei-1.xf-yun.com` | 域名 | **★ 占位**，以官方为准 |
| `AVATAR_IFLYTEK_BASE_PATH` | `/v1/avatar` | 以 `/` 开头 | **★ 占位**，以官方为准 |
| `AVATAR_IFLYTEK_AVATAR_ID` | *（空）* | 字符串 | 形象 ID（可在 `create_session` 按会话覆盖） |
| `AVATAR_IFLYTEK_VCN` | *（空）* | 字符串 | 发音人 |
| `AVATAR_TIMEOUT` | `15.0` | 正数 | 单次请求超时（秒） |

**统一口径（与既有 `EMBEDDING_*` / `RAG_*` 一致）**：

- **一个都不配 = 未配置 = 行为不变**（⇒ Mock，离线）；
- **空串等价于未配置**（不是「配了个空值」）；
- **非法值报错不静默**（`AVATAR_TIMEOUT=abc` / `base_path` 不以 `/` 开头 /
  `AVATAR_PROVIDER=bogus` ⇒ `AvatarProviderConfigError`）；
- **新 provider 显式 opt-in**（不写 `AVATAR_PROVIDER=iflytek` 就永远不会去打讯飞）。

`backend/.env.example` 已追加整段注释块（含占位符），照抄即可。

---

## 5. 讯飞鉴权与端点（★ 全部需要核对）

### 5.1 鉴权：HMAC-SHA256 + 三段请求行

```
sig_str       = "host: {host}\ndate: {date}\nPOST {path} HTTP/1.1"
signature     = base64(hmac_sha256(api_secret, sig_str))
authorization = base64('api_key="{api_key}", algorithm="hmac-sha256", '
                       'headers="host date request-line", signature="{signature}"')
url           = f"https://{host}{path}?{urlencode({authorization, date, host})}"
```

**与 `main._make_url` 的唯一差别**：请求行是 **`POST`** 而不是 `GET`。
因此**不能复用** `main._make_url`（它把方法硬编码成 GET）——本模块自带一份
`build_auth_query`，并支持显式传 `date` ⇒ **签名可复现**（单测用固定 date 手算比对）。

> `build_auth_query(host, path, api_key, api_secret, *, method="POST", date=None)`
> `build_signed_url(host, path, api_key, api_secret, *, date=None)`

### 5.2 三段式报文

```json
{
  "header":    {"app_id": "<AppId>", "uid": "<上层会话 id>"},
  "parameter": {"<action>": { ... }},
  "payload":   { ... }
}
```

`header.code != 0`（`0` / `"0"` / 缺省视为成功）⇒ `AvatarProviderResponseError`，
消息里带上 `header.message`。

### 5.3 六个能力的占位映射（★ 逐条核对）

| 能力 | `action` | 路径模板 | `parameter` | `payload` |
|---|---|---|---|---|
| `create_session` | `session_create` | `{base}/session` | `avatar_id` `vcn` `session_ref` | `{}` |
| `initialize` | `session_init` | `{base}/session/{sid}/init` | `stream` | `session_id` |
| `speak` | `text_drive` | `{base}/session/{sid}/speak` | `interrupt` | `session_id` `text` |
| `fetch_answer` | `answer_fetch` | `{base}/session/{sid}/answer` | `timeout_ms` | `session_id` |
| `status` | `status_query` | `{base}/session/{sid}/status` | `{}` | `session_id` |
| `stop` | `session_stop` | `{base}/session/{sid}/stop` | `{}` | `session_id` |

### 5.4 响应字段路径（★ 逐条核对）

实现用 `_dig(data, "payload.session_id")` 这类**路径逐个尝试**，容忍字段名差异：

```python
SESSION_ID_PATHS = ("payload.session_id", "payload.session.id",
                    "payload.avatar_session_id", "header.sid")
STREAM_URL_PATHS = ("payload.stream_url", "payload.stream.url", "payload.video_url")
```

- `fetch_answer` 的文本：`payload.text` → `payload.answer.text` → `payload.asr.text`
- `fetch_answer` 的音频：`payload.audio_url` → `payload.audio.url`
- `speak` 的时长：`payload.duration_ms`
- `status`：`payload.status` → `payload.session.status`；
  `payload.initialized` / `payload.speaking`

**所有待核对项都收敛在 `avatar_provider_iflytek.py` 顶部的常量与 `*_PATHS` 表**，
改一处即可，**不用动逻辑、不用动通道层、不用动测试之外的任何文件**。

### 5.5 讯飞虚拟人的接入约束（官方口径，来自前期分析）

- **不支持浏览器跨域** ⇒ 必须**后端代理**（这正是本 Provider 层存在的理由）；
- **60 秒无心跳断会话** ⇒ 生产需一个 30s 心跳任务（**本次未实现**）；
- **需单独开通**「AI 虚拟人」，**凭据与 Spark 大模型不是同一套**；
- `stream_url` 是**流地址**（非文件），前端需播放器（前端目前无播放器依赖）。

---

## 6. 状态流转

```
                     create_session
                          │
                          ▼
                    SESSION_CREATED
                          │ initialize(stream=True) → 拿 stream_url
                          ▼
                  SESSION_INITIALIZED ──speak()──▶ SESSION_SPEAKING
                          ▲                              │
                          └──────── speak() 完成 ─────────┘
                          │
                          │ stop()
                          ▼
                    SESSION_STOPPED
                          │
                          └── 任意阶段出错 ──▶ SESSION_FAILED
```

`AvatarStatus.initialized` = `status in (initialized, speaking)`；
`speaking` = `status == speaking`（Mock 口径；讯飞实现优先读上游字段，缺省回落到同口径）。

**Mock 的确定性约定**：`duration_ms = len(text) * 60`（60ms/字），
`stream_url = "mock://stream"`，`audio_url = "mock://audio/{sid}"` ⇒ 便于断言、无随机。

---

## 7. 怎么把 Mock 换成真实实现

**不需要改任何代码**，只做配置：

```bash
# backend/.env
AVATAR_PROVIDER=iflytek
AVATAR_IFLYTEK_APP_ID=<你的 AppId>
AVATAR_IFLYTEK_API_KEY=<你的 APIKey>
AVATAR_IFLYTEK_API_SECRET=<你的 APISecret>
# 端点若与占位不同，改这两行（改配置，不改代码）
AVATAR_IFLYTEK_HOST=<官方域名>
AVATAR_IFLYTEK_BASE_PATH=<官方路径前缀>
# 形象 / 发音人
AVATAR_IFLYTEK_AVATAR_ID=<形象 ID>
AVATAR_IFLYTEK_VCN=<发音人>
```

重启后端，启动日志会打印：

```
[avatar] provider=iflytek host=... base_path=... api_key=****XXXX configured=True
```

**核对入口（唯一在线入口）**：`GET /api/health` 的 `avatar` 字段（7 键）：

```json
{"provider": "iflytek", "known": ["mock", "iflytek"], "configured": true,
 "host": "...", "base_path": "/v1/avatar", "masked_api_key": "****XXXX", "error": null}
```

- `describe_avatar_provider()` **纯读配置、不构造 provider、不联网、绝不抛异常**
  （配置非法时把原因塞进 `error`）⇒ 健康检查**永不 500**；
- `api_key` **只露尾 4 位**；`api_key` / `api_secret` 声明 `repr=False`，**不进日志**。

### 换成「第三个厂商」

新写一个 `AvatarProvider` 子类（六个 `async` 方法 + `name`），
在 `build_avatar_provider` 里加一个分支即可。
**`avatar_interview.py` 一行不用改**（它只认接口，不认厂商）。

---

## 8. 测试

| 套件 | 断言数 | 覆盖 |
|---|---|---|
| `tests/test_avatar_provider.py` | **81** | [1] 值对象契约 [2] 错误分类 [3] Mock 六能力全流程 [4] 失败注入 [5] 讯飞配置 [6] **签名可复现**（固定 date 手算 HMAC 比对） [7] 假 transport 走通六能力 + `code!=0` + 网络异常 + 缺 id [8] 工厂与 describe [9] **零依赖 AST 守卫** [10] **Mock 流程测试**（经 `interview_service` 跑完整面试） |
| `tests/test_avatar_channel.py` | **58** | 通道契约 / text 不受影响 / avatar 入口 / 两模式互不影响 / HTTP 全链路 / **`/api/health` avatar 键** / 分层 AST 守卫 |
| `tests/test_interview_mode_channels.py` | **23** | `_deliver` 覆盖 4 个交互入口；text 零分流；逐字段等价 |

**全部离线、零联网**：

- `_FakeTransport(AvatarTransport)` 预置响应 ⇒ 讯飞实现单测**不碰网络**；
- `tests/regression_env.py` 把 **9 个 `AVATAR_*`** 钉成空串 ⇒ 防「有人往 `.env` 设
  `AVATAR_PROVIDER=iflytek`，导致回归去构造真实 provider / 真打网络耗配额」。

> ★ 钉扎必须**设空串**、不能 `pop`——`pop` 后 `load_dotenv(override=False)` 会把
> `.env` 的值**重新灌回来**。这是本项目唯一正确的钉扎手法。

跑法（**通配符**，别用手写清单）：

```bash
cd backend
for t in tests/test_*.py; do python "$t" || echo "FAIL: $t"; done
```

---

## 9. 已知未做（不在本次范围）

| 项 | 说明 |
|---|---|
| **真实端点 / 字段名核对** | §5.3 / §5.4 全部是**占位**，必须与官方文档逐条核对 |
| `avatar_session_id` / `stream_url` 落库 | `models/interview.py` 未加列（`schema_sync` 也未加）；目前会话 id 只活在进程内 provider 缓存里 |
| `POST /api/interview/{id}/avatar/start\|speak\|stop` | 把数字人信息**显式**暴露给前端的独立端点 |
| 30s 心跳 | 讯飞 60s 无心跳断会话 |
| 前端 `mode` 选择器 | 后端 `/create` 已支持 `mode`；`InterviewRoom.vue` 仍固定 text |
| 前端播放器 | 前端无播放器依赖，`stream_url` 暂无消费方 |
| 读侧 / HTTP 写侧的离线防护 | 与 RAG 侧同款缺口（只有 CLI 拒绝非语义配置） |

---

## 10. 文件清单

| 文件 | 状态 | 说明 |
|---|---|---|
| `backend/services/avatar_provider.py` | **新增**（504 行） | 接口 + 值对象 + 错误分类 + `MockAvatarProvider`；**零第三方依赖** |
| `backend/services/avatar_provider_iflytek.py` | **新增**（608 行） | 配置 + HMAC 鉴权 + `AvatarTransport` + `IflytekAvatarProvider` + 工厂 + `describe_*` |
| `backend/services/avatar_interview.py` | **重写**（278 行） | 通道层：阶段判别 + `drive` + `enter`（降级） |
| `backend/main.py` | 修改 | 新增 `avatar_status()` / `_report_avatar_status()`；`/api/health` 加 `avatar` 键；lifespan 加一行日志 |
| `backend/tests/test_avatar_provider.py` | **新增**（576 行） | Provider 单测 + Mock 流程测试 |
| `backend/tests/test_avatar_channel.py` | 修改（492 行） | 从「空实现」契约改为「provider 已接入」契约；加 health 断言 |
| `backend/tests/regression_env.py` | 修改 | `PINNED` 新增 9 个 `AVATAR_*`（设空串） |
| `backend/.env.example` | 修改 | 追加数字人配置段（占位符） |

**未修改**（受 `FROZEN_SOURCES` 源码指纹保护，且本次一行未动）：
`interview_core.py` / `interview_agent.py` / `interview_planner.py` /
`interview_context.py` / `question_validator.py` / `question_generator.py` /
scoring 逻辑 / RAG 逻辑（`knowledge_rag` / `knowledge_retriever` / `vector_*` / `embedding_*`）。

**测试替身签名未变**（`test_agent_*` / `test_interview_eval_benchmark` 的
`interview_core.py` sha256 指纹仍匹配）⇒ 60/60 套全绿。
