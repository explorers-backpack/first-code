"""管理员 HTTP 路由：原路径、鉴权依赖、参数和响应不变。"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from deps import get_current_admin
from modules.admin.schemas import JobCreate
import modules.admin.service as service

router = APIRouter()

@router.get("/api/admin/user-logs", tags=["管理员"])
async def get_user_logs(
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """获取普通用户登录历史日志（仅 admin 可访问）。"""
    return await service.get_user_logs(db)


@router.post("/api/admin/add-job", tags=["管理员"])
async def add_job(
    data: JobCreate,
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """添加新岗位（仅 admin 可访问）。"""
    return await service.add_job(data, db)


@router.post("/api/admin/users/{user_id}/kick", tags=["管理员"])
async def kick_user_sessions(
    user_id: int,
    current_admin: dict = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
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
    try:
        return await service.kick_user_sessions(user_id, db)
    except service.UserNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


