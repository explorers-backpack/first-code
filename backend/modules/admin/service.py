"""管理员用例：查询、岗位写入和会话撤销；事务顺序保持不变。"""
import logging
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from models import User, UserLoginLog, UserSession, Job
from services import ws_ticket_store, ws_revocation
from modules.admin.schemas import JobCreate

_ticket_log = logging.getLogger("careerai.ws_ticket")

class UserNotFound(Exception):
    pass


async def get_user_logs(db: AsyncSession):
    """获取普通用户登录历史日志（仅 admin 可访问）。"""
    result = await db.execute(
        select(UserLoginLog).order_by(UserLoginLog.id.desc()).limit(100)
    )
    logs = result.scalars().all()

    return {
        "logs": [
            {
                "email": log.email,
                "lastLogin": log.last_login,
                "role": log.role,
                "searchCount": log.search_count,
            }
            for log in logs
        ]
    }



async def add_job(data: JobCreate, db: AsyncSession):
    """添加新岗位（仅 admin 可访问）。"""
    new_job = Job(
        job_name=data.job_name,
        salary=data.salary,
        edu_require="本科",
        major_require="不限",
        skills=",".join(data.skills),
        duty=data.description,
        city=data.city,
        industry="互联网",
    )
    db.add(new_job)
    await db.commit()
    await db.refresh(new_job)

    return {
        "success": True,
        "message": "岗位添加成功",
        "job": {
            "job_id": new_job.id,
            "job_name": new_job.job_name,
            "city": new_job.city,
            "salary": new_job.salary,
            "skills": new_job.skills,
            "description": new_job.duty,
        },
    }



async def kick_user_sessions(user_id: int, db: AsyncSession):
    """**管理员踢下线**：撤销该用户全部登录会话，并让已建立的实时通道断开。

    三步（顺序有意）：
    1. ``user_session.revoked_at = now``（**主效应**：HTTP 侧下一次请求即 401）；
    2. 作废该用户所有**未使用**的 WS ticket（防止踢完再用旧票建连）；
    3. 写一条**跨实例**撤销广播（``user_id`` 级）⇒ 各实例的轮询器读到后
       主动断开自己身上的连接（目标延迟见 ``services/ws_revocation_watcher``）。

    ★ 为什么第 3 步不能省：``revoked_at`` 只是"以后不认"，而 WS 连接是**已经建立**
    的长连接，它不会自己回头查库。改造前实测：ticket 路径的连接在撤销后
    **>20s 内完全不失效**（上界是空闲超时 300s，而前端有心跳 ⇒ 实际永不失效）。
    ★ 第 1 步失败会直接 500（主效应必须成功）；第 2/3 步失败只记 WARNING
    ——主效应已落库，兜底是周期性复核。
    """
    target = (
        await db.execute(select(User).where(User.id == int(user_id)))
    ).scalar_one_or_none()
    if target is None:
        raise UserNotFound("用户不存在")

    now = datetime.utcnow()
    result = await db.execute(
        update(UserSession)
        .where(UserSession.user_id == int(user_id), UserSession.revoked_at.is_(None))
        .values(revoked_at=now)
        .execution_options(synchronize_session=False)
    )
    await db.commit()
    revoked_sessions = int(result.rowcount or 0)

    revoked_tickets = 0
    try:
        revoked_tickets = await ws_ticket_store.revoke_for_user(db, user_id=int(user_id))
    except ws_ticket_store.TicketStoreUnavailable as exc:
        _ticket_log.warning(
            "ws_ticket_revoke_on_kick_failed user_id=%s error=%s",
            user_id,
            type(exc).__name__,
        )

    broadcast = True
    try:
        await ws_revocation.record(
            db,
            user_id=int(user_id),
            auth_session_id=None,  # 空 = 该用户所有会话
            reason=ws_revocation.REASON_ADMIN_KICK,
        )
    except ws_revocation.RevocationStoreUnavailable as exc:
        broadcast = False
        _ticket_log.warning(
            "ws_revocation_record_on_kick_failed user_id=%s error=%s",
            user_id,
            type(exc).__name__,
        )

    return {
        "success": True,
        "message": "已踢下线",
        "user_id": int(user_id),
        "revoked_sessions": revoked_sessions,
        "revoked_tickets": revoked_tickets,
        # 广播失败时明确告知：调用方要知道"主动断开"这次可能不生效，
        # 而不是以为已经踢干净了。
        "broadcast": broadcast,
    }


