"""历史温度归档 API 路由。

端点
----
* ``POST   /archive/batches/{recorder_id}/{export_date}``  上传/整体替换批次
* ``GET    /archive/batches``                              已发布批次清单
* ``GET    /archive/batches/{recorder_id}/{export_date}``  批次元数据（固定版本）
* ``GET    /archive/batches/{recorder_id}/{export_date}/readings``  时段闭区间回查
* ``GET    /archive/batches/{recorder_id}/{export_date}/download``  下载原 CSV
"""

from __future__ import annotations

import contextlib
import functools
import hashlib
import os
import re
import asyncio
from datetime import datetime

import anyio
from fastapi import APIRouter, HTTPException, Path as PathParam, Query, Request, UploadFile
from fastapi.responses import StreamingResponse

from .ingest import ArchiveError, build_index
from .store import ArchiveStore, BatchBusy, BatchNotFound, VersionInfo

_RECORDER_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")
_EXPORT_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CHUNK = 1024 * 1024  # 1 MiB 流式落盘块
_MAX_LIMIT = 100_000
_DEFAULT_LIMIT = 10_000

# 模块级引用，便于测试注入慢文件/挂起文件，模拟在途下载与客户端断连。
_open_file = open

router = APIRouter(prefix="/archive", tags=["历史温度归档"])


def make_batch_key(recorder_id: str, export_date: str) -> str:
    return f"{recorder_id}/{export_date}"


def _validate_batch_params(recorder_id: str, export_date: str) -> str:
    if not _RECORDER_ID_RE.match(recorder_id):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_recorder_id",
                "message": "记录仪 ID 须为 1~64 位字母、数字或 ._-，且以字母数字开头",
            },
        )
    if not _EXPORT_DATE_RE.match(export_date):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_export_date",
                "message": "导出日期必须为 YYYY-MM-DD 格式",
            },
        )
    try:
        datetime.strptime(export_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_export_date", "message": f"日历日期不存在：{export_date}"},
        ) from None
    return make_batch_key(recorder_id, export_date)


def _parse_query_timestamp(value: str, field: str) -> int:
    """查询参数时间：复用 ingest 的严格 UTC 校验，返回微秒时间戳。"""
    from .ingest import parse_utc_timestamp

    try:
        micros, _ = parse_utc_timestamp(value, 0)
    except ArchiveError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_timestamp", "message": f"{field}：{exc.message}"},
        ) from None
    return micros


def _get_store(request: Request) -> ArchiveStore:
    return request.app.state.archive_store


def _current_version_or_404(store: ArchiveStore, batch_key: str) -> VersionInfo:
    try:
        return store.current_version(batch_key)
    except BatchNotFound:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "batch_not_found",
                "message": f"批次 {batch_key} 尚未发布任何版本",
            },
        ) from None


# ----------------------------------------------------------------------
# 上传 / 替换
# ----------------------------------------------------------------------
@router.post(
    "/batches/{recorder_id}/{export_date}",
    summary="上传记录仪导出 CSV（同批重传整体替换，处理期间旧批仍可查）",
)
async def upload_batch(
    request: Request,
    recorder_id: str = PathParam(..., description="记录仪 ID"),
    export_date: str = PathParam(..., description="导出日期 YYYY-MM-DD"),
    file: UploadFile | None = None,
):
    store = _get_store(request)
    max_bytes = request.app.state.settings.archive_max_upload_bytes
    batch_key = _validate_batch_params(recorder_id, export_date)

    if file is None or not file.filename:
        raise HTTPException(
            status_code=400,
            detail={"error": "missing_file", "message": "必须以 multipart 字段 file 上传 CSV"},
        )
    if file.content_type not in ("text/csv", "application/vnd.ms-excel", "application/octet-stream"):
        raise HTTPException(
            status_code=415,
            detail={"error": "unsupported_media_type", "message": "仅接受 CSV 文件（text/csv）"},
        )

    # 同批并发替换只接纳一份；不同批次互不影响。
    try:
        lock_cm = store.batch_lock(batch_key)
        await lock_cm.__aenter__()
    except BatchBusy:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "batch_busy",
                "message": f"批次 {batch_key} 已有替换任务在途，请稍后重试",
            },
        ) from None

    staging_id, staging_dir, csv_path = store.create_staging()
    try:
        # ---- 流式落盘 + 16 MiB 上限（内存只保留 1 MiB 块）----
        digest = hashlib.sha256()
        total = 0
        try:
            with open(csv_path, "wb") as sink:
                while True:
                    chunk = await file.read(_CHUNK)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail={
                                "error": "file_too_large",
                                "message": f"上传超过 {max_bytes // (1024 * 1024)} MiB 上限",
                            },
                        )
                    sink.write(chunk)
                    digest.update(chunk)
        finally:
            await file.close()

        if total == 0:
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_csv", "message": "上传文件为空"},
            )

        # ---- 线程池内流式校验 + 建索引，不阻塞事件循环 ----
        index_path = os.path.join(staging_dir, "index.db")
        try:
            row_count = await anyio.to_thread.run_sync(
                build_index, csv_path, index_path, abandon_on_cancel=True
            )
        except ArchiveError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail()) from None
        except OSError as exc:
            # 磁盘 I/O 故障等：旧批次不受影响，暂存在 finally 中清理。
            raise HTTPException(
                status_code=500,
                detail={
                    "error": "write_failed",
                    "message": f"索引/原件写入失败：{exc.__class__.__name__}",
                },
            ) from None

        # ---- 原子发布（rename + catalog 短事务）----
        try:
            info, stale = await anyio.to_thread.run_sync(
                functools.partial(
                    store.publish,
                    batch_key=batch_key,
                    recorder_id=recorder_id,
                    export_date=export_date,
                    staging_dir=staging_dir,
                    row_count=row_count,
                    byte_size=total,
                    sha256=digest.hexdigest(),
                )
            )
        except Exception as exc:  # 发布失败：旧批仍在，暂存稍后清理
            raise HTTPException(
                status_code=500,
                detail={"error": "publish_failed", "message": f"批次发布失败：{exc.__class__.__name__}"},
            ) from None
        # 回到事件循环线程后再安排旧版本回收（等待在途租约排空）。
        if stale is not None:
            store.schedule_reclaim(stale[0], stale[1])
        # 发布成功：目录已移走，finally 中的清理对不存在目录无副作用。
        return {
            "status": "published",
            "replaced": stale is not None,
            "batch": info.as_dict(),
        }
    finally:
        # 客户端断连/任何异常：丢弃暂存（已 rename 走的目录不在此处）。
        await anyio.to_thread.run_sync(store.discard_staging, staging_dir)
        await lock_cm.__aexit__(None, None, None)


# ----------------------------------------------------------------------
# 批次清单与元数据
# ----------------------------------------------------------------------
@router.get("/batches", summary="列出全部已发布批次（当前版本）")
async def list_batches(request: Request):
    store = _get_store(request)
    infos = await anyio.to_thread.run_sync(store.list_batches)
    return {"batches": [info.as_dict() for info in infos], "count": len(infos)}


@router.get(
    "/batches/{recorder_id}/{export_date}",
    summary="查询批次元数据（返回当前固定版本号）",
)
async def get_batch(
    request: Request,
    recorder_id: str = PathParam(...),
    export_date: str = PathParam(...),
):
    store = _get_store(request)
    batch_key = _validate_batch_params(recorder_id, export_date)
    info = await anyio.to_thread.run_sync(_current_version_or_404, store, batch_key)
    return info.as_dict()


# ----------------------------------------------------------------------
# 时段回查
# ----------------------------------------------------------------------
@router.get(
    "/batches/{recorder_id}/{export_date}/readings",
    summary="按回路与 UTC 时间闭区间回查读数",
)
async def query_readings(
    request: Request,
    recorder_id: str = PathParam(...),
    export_date: str = PathParam(...),
    circuit: int | None = Query(None, ge=0, le=7, description="可选：限定回路 0~7"),
    start: str | None = Query(None, description="起始 UTC 时间（含，ISO 8601）"),
    end: str | None = Query(None, description="结束 UTC 时间（含，ISO 8601）"),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    store = _get_store(request)
    batch_key = _validate_batch_params(recorder_id, export_date)
    start_micros = _parse_query_timestamp(start, "start") if start is not None else None
    end_micros = _parse_query_timestamp(end, "end") if end is not None else None
    if (
        start_micros is not None
        and end_micros is not None
        and end_micros < start_micros
    ):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_range", "message": "end 不得早于 start"},
        )

    # 版本在请求开始时固定：期间即使发生替换，本次查询仍读同一完整版本。
    info = await anyio.to_thread.run_sync(_current_version_or_404, store, batch_key)
    with store.leases.lease(info.version_id):
        readings, total = await anyio.to_thread.run_sync(
            functools.partial(
                store.query_readings,
                info,
                circuit=circuit,
                start_micros=start_micros,
                end_micros=end_micros,
                limit=limit,
                offset=offset,
            )
        )
    return {
        "batch": info.as_dict(),
        "query": {
            "circuit": circuit,
            "start": start,
            "end": end,
            "limit": limit,
            "offset": offset,
            "closed_interval": True,
        },
        "total": total,
        "returned": len(readings),
        "truncated": offset + len(readings) < total,
        "readings": readings,
    }


# ----------------------------------------------------------------------
# 原件下载（引用计数保护，断连也释放）
# ----------------------------------------------------------------------
@router.get(
    "/batches/{recorder_id}/{export_date}/download",
    summary="下载批次原始 CSV（固定版本，旧原件等在途下载结束后回收）",
)
async def download_batch(
    request: Request,
    recorder_id: str = PathParam(...),
    export_date: str = PathParam(...),
):
    store = _get_store(request)
    batch_key = _validate_batch_params(recorder_id, export_date)
    info = await anyio.to_thread.run_sync(_current_version_or_404, store, batch_key)

    # 先取租约再打开文件；租约释放放在流生成器 finally，断连同样执行。
    store.leases.acquire(info.version_id)
    file_handle = None
    try:
        file_handle = await anyio.to_thread.run_sync(
            _open_file, info.csv_path, "rb"
        )
    except OSError as exc:
        store.leases.release(info.version_id)
        raise HTTPException(
            status_code=500,
            detail={"error": "read_failed", "message": f"原件打开失败：{exc.__class__.__name__}"},
        ) from None

    async def stream():
        # 每次块读取封装成独立任务：客户端断连导致生成器被取消时，
        # 即使底层工作线程仍阻塞在 read 上，也能立即进入 finally 释放租约；
        # 句柄等该读取线程结束后再关闭，避免在另一个线程读取时关闭文件。
        pending_read: asyncio.Task | None = None

        async def _close_when_done() -> None:
            if pending_read is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await pending_read
            with contextlib.suppress(OSError):
                await anyio.to_thread.run_sync(file_handle.close)

        try:
            while True:
                pending_read = asyncio.create_task(
                    anyio.to_thread.run_sync(file_handle.read, _CHUNK)
                )
                chunk = await pending_read
                pending_read = None
                if not chunk:
                    break
                yield chunk
        finally:
            # 客户端断连、正常结束或异常都会走到这里，且不依赖阻塞线程返回。
            store.leases.release(info.version_id)
            # 句柄清理异步进行：等待可能在途的读取线程自然结束后关闭。
            asyncio.create_task(_close_when_done())

    filename = f"{recorder_id}_{export_date}_{info.version_id[:8]}.csv"
    return StreamingResponse(
        stream(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(info.byte_size),
            "X-Archive-Version-Id": info.version_id,
            "X-Archive-Sha256": info.sha256,
        },
    )
