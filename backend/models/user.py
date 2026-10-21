# -*- coding: utf-8 -*-
"""用户与会话相关 ORM 模型。

原定义于 ``main.py``，为支持 ``models/`` / ``services/`` / ``api/`` 分层
而迁移至此，字段与表名逐字未改。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from database import Base


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(80), unique=True, nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    password_hash = Column(String(200), nullable=False)
    role = Column(String(20), default="user")
    created_at = Column(DateTime, default=datetime.utcnow)


class UserSession(Base):
    __tablename__ = "user_session"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    token = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(120), nullable=False)
    role = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
    revoked_at = Column(DateTime, nullable=True)


class UserLoginLog(Base):
    __tablename__ = "user_login_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(120), nullable=False)
    role = Column(String(20), nullable=False)
    last_login = Column(String(32), nullable=False)
    search_count = Column(Integer, default=0)


class WsTicket(Base):
    """WebSocket 一次性连接凭证（ticket）—— **跨进程 / 跨实例共享的存储**。

    为什么要有这张表
    ----------------
    浏览器 ``WebSocket`` 不能自定义请求头，长期 Bearer Token 只能拼进 URL
    query，会被网关/代理的访问日志记录。改用「HTTP 签发短时 ticket → WS 用
    ticket 握手」后，ticket 的**签发方与校验方可能不是同一个进程**：多 worker
    或多副本部署时，A 实例签发、B 实例校验。进程内字典在这个场景下必然失效
    （表现为随机 ``4401``，而且"消费"语义完全不存在——每个实例各有一份副本）。

    因此 ticket 落在**与业务同一个 MySQL** 上，靠 ``ticket_hash`` 的唯一索引 +
    ``UPDATE … WHERE used_at IS NULL`` 的条件更新实现**原子消费**：
    并发校验同一个 ticket 时，InnoDB 的行锁会把两条 UPDATE 串行化，
    只有一条能匹配到 ``used_at IS NULL``，另一条 ``rowcount == 0`` ⇒ 失败。

    字段说明
    --------
    - ``ticket_hash``：**只存 SHA-256 摘要，不存明文**。ticket 本身是
      256-bit 的随机串（``secrets.token_urlsafe(32)``），不存在字典攻击面，
      因此不加盐（加盐会破坏等值查询）；库被读走也拿不到可用凭证。
    - ``user_id`` / ``session_id``：**绑定**关系。``session_id`` 参与消费条件，
      拿 A 会话的 ticket 去连 B 会话时 ``rowcount == 0`` 且**不会消耗**该 ticket。
    - ``auth_session_id``：绑定的**登录会话**（``user_session.id``）。★ 这一列是
      「撤销能生效」的前提：ticket 路径没有长期 token 可复核，若只记 ``user_id``，
      连接存续期间就只能验「用户还在不在」——管理员踢下线**永远踢不掉**已建立的
      连接（实测 >20s 完全不生效，见 ``CONCURRENCY_CONSISTENCY_REPORT.md`` §10）。
      有了它，复核可以走 ``user_session.revoked_at``，与 HTTP 接口同一口径。
      可空：历史 ticket 与「无 Authorization 头」的测试路径没有这个值，
      此时退化为「用户仍存在」并记 WARNING。
    - ``expires_at``：短时有效（默认 30 秒）。过期即不可用，与"已消费"分开判断。
    - ``used_at``：该 ticket **已不可再用**（被消费**或**被撤销）。刻意保留行
      而不是消费即删：便于区分"用过"与"从不存在"，也便于排查重放。

    生命周期
    --------
    行不长期保留：``services.ws_ticket_store.sweep_expired`` 会删除
    ``expires_at < now`` 的行（机会式、节流，无后台任务）。因此本表常态下只有
    「最近 30 秒签发、尚未清理」的少量行。
    """

    __tablename__ = "ws_ticket"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 唯一索引：既是"消费"的定位键，也是"并发只成功一次"的最终兜底
    ticket_hash = Column(String(64), unique=True, nullable=False, index=True)

    user_id = Column(Integer, nullable=False)
    # 不建外键：ticket 是**凭证**不是业务数据，不该因会话删除而被级联约束
    session_id = Column(Integer, nullable=False)
    #: 绑定的登录会话 id（``user_session.id``）。可空，见类文档。
    auth_session_id = Column(Integer, nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False, index=True)
    used_at = Column(DateTime, nullable=True)


class WsRevocation(Base):
    """**跨实例**的「会话已撤销」广播（实时通道主动断开用）。

    为什么需要它
    ------------
    撤销动作发生在**某一个**实例上（管理员踢下线 / 登出），而受影响的 WS 连接
    可能长在**另一个**实例上。进程内的事件、字典、asyncio 队列都到不了那边。
    所以撤销必须落在一个**共享**的地方，让每个实例自己去读。

    为什么是一张**只追加**的表而不是消息队列
    ----------------------------------------
    1. 项目里没有 Redis / MQ（见 ``ws_ticket_store`` 的选型说明），为「踢下线」
       引入一个有状态中间件不成比例；
    2. 只追加 + 单调自增主键 ⇒ **消息不会丢**：某个实例错过一次轮询，
       下次用 ``id > watermark`` 就能补齐（这正是 pub/sub 做不到的）；
    3. 实例重启后从 ``max(id)`` 起步，不会重放历史撤销（历史连接本来就不存在）；
    4. 单实例一次轮询是**一条**索引查询，与连接数无关 —— 不是「每条连接每秒查一次」。

    字段
    ----
    - ``user_id``：被撤销的用户。
    - ``auth_session_id``：非空 = **只**影响这一条登录会话（登出）；
      为空 = 影响该用户**所有**会话（管理员踢下线）。
    - ``reason``：稳定短标签（``admin_kick`` / ``logout`` / ``expire``），供排障聚合。
    """

    __tablename__ = "ws_revocation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False, index=True)
    auth_session_id = Column(Integer, nullable=True, index=True)
    reason = Column(String(32), nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)



class LoginFailure(Base):
    """登录失败记录（**共享存储**的滑动窗口限流）。

    为什么需要它
    ------------
    登录爆破限流原先在 ``utils/login_throttle`` 里用**进程内**字典实现。
    单 worker 够用，多 worker 时每个进程独立计数 ⇒ 实际允许次数 = 上限 × 进程数
    （安全项被放大，这是 ``worker_guard`` 至今仍需保留的主因）。

    本表把「谁在什么时候失败过」放到**共享**的数据库里，任何实例都能看到同一份计数。

    为什么是「一条失败一行」而不是「一个计数器列」
    ----------------------------------------------
    - **滑窗语义逐字保留**：``check`` = ``COUNT(*) WHERE failed_at >= now()-window``，
      与原实现的 ``deque`` + 修剪窗口完全等价（固定窗口会在边界处产生双倍突发）；
    - 写入量极小：**只在登录失败时**写，正常登录一次都不写；
    - 过期行按 ``failed_at`` 索引删除，成本与窗口内的失败数成正比。

    隐私
    ----
    只存 ``sha256(小写邮箱|客户端IP)`` 的十六进制摘要，**不存**明文邮箱 / IP。
    排障时用同一算法重算即可比对，但库被读走也拿不到账号清单。

    字段
    ----
    - ``key_hash``：``sha256("<email>|<ip>")``，64 位十六进制。
    - ``failed_at``：失败发生的**墙钟**时间（UTC）。★ 不能用 ``monotonic``——
      它只在单个进程内有意义，跨实例/重启不可比。
    """

    __tablename__ = "login_failure"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_hash = Column(String(64), nullable=False, index=True)
    failed_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
