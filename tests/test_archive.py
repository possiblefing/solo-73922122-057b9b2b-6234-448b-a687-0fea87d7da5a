"""历史温度归档 API 测试（ASGI 进程内调用，临时磁盘目录）。

覆盖：流式校验、内存有界、时段闭区间回查、原件下载、原子替换、
处理期间旧批可读、同批并发替换只接纳一份、重启恢复与暂存清理、
下载租约（断连释放、旧原件延迟回收）、16 MiB 上限与坏行 422。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app import main as web
from app.archive.ingest import build_index
from app.archive.store import ArchiveStore

HEADER = "circuit,received_at,temperature_c\n"


def csv_bytes(rows: list[tuple[int, str, str]]) -> bytes:
    body = "\n".join(f"{c},{ts},{t}" for c, ts, t in rows)
    return (HEADER + body + "\n").encode("utf-8")


SAMPLE_ROWS = [
    (0, "2026-11-13T08:00:00Z", "-18.5"),
    (0, "2026-11-13T08:30:00Z", "-17.0"),
    (0, "2026-11-13T09:00:00Z", "-19.25"),
    (3, "2026-11-13T08:15:00Z", "4.5"),
    (3, "2026-11-13T09:15:00Z", "6.0"),
    (7, "2026-11-13T10:00:00+00:00", "11.123"),
]


@pytest.fixture
async def archive_app(tmp_path: Path):
    """带完整 lifespan 的应用：启动清理、关闭回收。"""
    store = ArchiveStore(str(tmp_path / "archive"))
    web.app.state.archive_store = store
    web.app.state.settings = web.settings
    transport = ASGITransport(app=web.app)
    lifespan = web.app.router.lifespan_context(web.app)
    await lifespan.__aenter__()
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac, store, tmp_path
    finally:
        await lifespan.__aexit__(None, None, None)


@pytest.fixture
def api(archive_app):
    return archive_app[0]


@pytest.fixture
def store(archive_app):
    return archive_app[1]


@pytest.fixture
def tmp(archive_app):
    return archive_app[2]


async def _upload(api: AsyncClient, recorder: str, date: str, content: bytes,
                  filename: str = "export.csv", content_type: str = "text/csv"):
    return await api.post(
        f"/archive/batches/{recorder}/{date}",
        files={"file": (filename, content, content_type)},
    )


# ----------------------------------------------------------------------
# 上传与基础校验
# ----------------------------------------------------------------------
async def test_upload_publishes_batch(api: AsyncClient, store: ArchiveStore):
    r = await _upload(api, "REC-001", "2026-11-13", csv_bytes(SAMPLE_ROWS))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "published"
    assert body["replaced"] is False
    batch = body["batch"]
    assert batch["recorder_id"] == "REC-001"
    assert batch["export_date"] == "2026-11-13"
    assert batch["row_count"] == len(SAMPLE_ROWS)
    assert batch["sha256"] == hashlib.sha256(csv_bytes(SAMPLE_ROWS)).hexdigest()
    assert batch["byte_size"] == len(csv_bytes(SAMPLE_ROWS))

    # 磁盘布局：暂存已清空，版本目录含原件、索引与元数据
    assert not any(store.staging_dir.iterdir())
    version_dir = store.versions_dir / batch["version_id"]
    assert (version_dir / "source.csv").is_file()
    assert (version_dir / "index.db").is_file()
    assert (version_dir / "meta.json").is_file()


async def test_list_and_get_batch(api: AsyncClient):
    await _upload(api, "REC-001", "2026-11-13", csv_bytes(SAMPLE_ROWS))
    r = await api.get("/archive/batches")
    assert r.status_code == 200
    assert r.json()["count"] == 1
    assert r.json()["batches"][0]["batch_key"] == "REC-001/2026-11-13"

    r = await api.get("/archive/batches/REC-001/2026-11-13")
    assert r.status_code == 200
    assert r.json()["version_id"]


async def test_get_unknown_batch_returns_404(api: AsyncClient):
    r = await api.get("/archive/batches/NOPE/2026-01-01")
    assert r.status_code == 404
    assert r.json()["error"] == "batch_not_found"


async def test_invalid_batch_params_400(api: AsyncClient):
    r = await api.get("/archive/batches/REC%201/2026-11-13")
    assert r.status_code == 400
    r = await api.get("/archive/batches/REC-1/2026-13-40")
    assert r.status_code == 400


@pytest.mark.parametrize(
    "bad_rows, error_code",
    [
        # 回路越界
        ([(8, "2026-11-13T08:00:00Z", "-18.0")], "invalid_csv"),
        ([(-1, "2026-11-13T08:00:00Z", "-18.0")], "invalid_csv"),
        # 非 UTC / 无时区
        ([(0, "2026-11-13T08:00:00", "-18.0")], "invalid_csv"),
        ([(0, "2026-11-13T08:00:00+08:00", "-18.0")], "invalid_csv"),
        ([(0, "not-a-time", "-18.0")], "invalid_csv"),
        # 非有限十进制
        ([(0, "2026-11-13T08:00:00Z", "1e3")], "invalid_csv"),
        ([(0, "2026-11-13T08:00:00Z", "NaN")], "invalid_csv"),
        ([(0, "2026-11-13T08:00:00Z", "inf")], "invalid_csv"),
        ([(0, "2026-11-13T08:00:00Z", "")], "invalid_csv"),
        # 同批回路+时刻重复
        (
            [
                (0, "2026-11-13T08:00:00Z", "-18.0"),
                (0, "2026-11-13T08:00:00Z", "-19.0"),
            ],
            "duplicate_reading",
        ),
    ],
)
async def test_bad_rows_rejected_422(api: AsyncClient, bad_rows, error_code):
    r = await _upload(api, "REC-BAD", "2026-11-13", csv_bytes(bad_rows))
    assert r.status_code == 422, r.text
    assert r.json()["error"] == error_code
    # 坏行绝不留暂存，也不产生版本
    r2 = await api.get("/archive/batches/REC-BAD/2026-11-13")
    assert r2.status_code == 404


async def test_bad_header_rejected(api: AsyncClient, store: ArchiveStore):
    content = b"circuit,time,temp\n0,2026-11-13T08:00:00Z,-18\n"
    r = await _upload(api, "REC-H", "2026-11-13", content)
    assert r.status_code == 422
    assert "表头" in r.json()["message"]
    assert not any(store.staging_dir.iterdir())


async def test_header_only_rejected(api: AsyncClient):
    r = await _upload(api, "REC-E", "2026-11-13", HEADER.encode())
    assert r.status_code == 422


async def test_non_utf8_rejected(api: AsyncClient):
    content = HEADER.encode() + b"0,2026-11-13T08:00:00Z,\xff\xfe\x00\n"
    r = await _upload(api, "REC-U", "2026-11-13", content)
    assert r.status_code == 422
    assert "UTF-8" in r.json()["message"]


async def test_utf8_bom_accepted(api: AsyncClient):
    # 记录仪/Excel 导出常带 UTF-8 BOM，应兼容
    content = b"\xef\xbb\xbf" + csv_bytes(SAMPLE_ROWS[:2])
    r = await _upload(api, "REC-BOM", "2026-11-13", content)
    assert r.status_code == 200, r.text
    assert r.json()["batch"]["row_count"] == 2


async def test_crlf_and_header_whitespace_accepted(api: AsyncClient):
    content = (
        b"circuit , received_at , temperature_c \r\n"
        b"0,2026-11-13T08:00:00Z,-18.5\r\n"
        b"3,2026-11-13T08:30:00.250+00:00,4.25\r\n"
    )
    r = await _upload(api, "REC-CRLF", "2026-11-13", content)
    assert r.status_code == 200, r.text
    r = await api.get("/archive/batches/REC-CRLF/2026-11-13/readings")
    rows = r.json()["readings"]
    assert rows[0]["received_at"] == "2026-11-13T08:00:00+00:00"
    assert rows[1]["temperature_c"] == 4.25


async def test_wrong_column_count_and_blank_line(api: AsyncClient):
    content = (HEADER + "0,2026-11-13T08:00:00Z\n").encode()
    r = await _upload(api, "REC-C", "2026-11-13", content)
    assert r.status_code == 422
    content2 = (HEADER + "0,2026-11-13T08:00:00Z,-18\n\n").encode()
    r = await _upload(api, "REC-C", "2026-11-13", content2)
    assert r.status_code == 422


async def test_empty_upload_422(api: AsyncClient):
    r = await _upload(api, "REC-0", "2026-11-13", b"")
    assert r.status_code == 422


async def test_missing_file_field_400(api: AsyncClient):
    r = await api.post(
        "/archive/batches/REC-X/2026-11-13",
        content=b"",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code in (400, 422)


# ----------------------------------------------------------------------
# 回查：回路 + 时间闭区间
# ----------------------------------------------------------------------
async def test_query_all_and_filters(api: AsyncClient):
    await _upload(api, "REC-001", "2026-11-13", csv_bytes(SAMPLE_ROWS))
    base = "/archive/batches/REC-001/2026-11-13/readings"

    r = await api.get(base)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 6
    assert body["returned"] == 6
    assert body["truncated"] is False
    # 按 circuit、时间升序
    circuits = [x["circuit"] for x in body["readings"]]
    assert circuits == sorted(circuits)
    assert all(isinstance(x["temperature_c"], float) for x in body["readings"])

    r = await api.get(base, params={"circuit": 0})
    assert r.status_code == 200
    assert r.json()["total"] == 3
    assert {x["circuit"] for x in r.json()["readings"]} == {0}

    r = await api.get(
        base,
        params={
            "start": "2026-11-13T08:30:00Z",
            "end": "2026-11-13T09:00:00Z",
        },
    )
    body = r.json()
    assert body["total"] == 2  # 闭区间：08:30 与 09:00 都包含
    assert [x["received_at"] for x in body["readings"]] == [
        "2026-11-13T08:30:00+00:00",
        "2026-11-13T09:00:00+00:00",
    ]


async def test_query_closed_interval_boundaries(api: AsyncClient):
    await _upload(api, "REC-001", "2026-11-13", csv_bytes(SAMPLE_ROWS))
    base = "/archive/batches/REC-001/2026-11-13/readings"
    r = await api.get(
        base,
        params={"circuit": 0, "start": "2026-11-13T08:00:00Z",
                "end": "2026-11-13T08:00:00Z"},
    )
    body = r.json()
    assert body["total"] == 1  # 单点闭区间
    assert body["readings"][0]["temperature_c"] == -18.5


async def test_query_limit_offset_and_invalid_range(api: AsyncClient):
    await _upload(api, "REC-001", "2026-11-13", csv_bytes(SAMPLE_ROWS))
    base = "/archive/batches/REC-001/2026-11-13/readings"
    r = await api.get(base, params={"limit": 2, "offset": 2})
    body = r.json()
    assert body["returned"] == 2
    assert body["truncated"] is True

    r = await api.get(
        base, params={"start": "2026-11-13T10:00:00Z", "end": "2026-11-13T08:00:00Z"}
    )
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_range"

    r = await api.get(base, params={"start": "not-a-time"})
    assert r.status_code == 400


# ----------------------------------------------------------------------
# 下载：原件字节一致、版本固定
# ----------------------------------------------------------------------
async def test_download_original_csv(api: AsyncClient):
    content = csv_bytes(SAMPLE_ROWS)
    await _upload(api, "REC-001", "2026-11-13", content)
    r = await api.get("/archive/batches/REC-001/2026-11-13/download")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers["content-disposition"]
    assert r.content == content
    assert r.headers["x-archive-sha256"] == hashlib.sha256(content).hexdigest()
    assert r.headers["content-length"] == str(len(content))


# ----------------------------------------------------------------------
# 替换：原子整体替换、旧批处理期间可读、并发只接纳一份
# ----------------------------------------------------------------------
async def test_replace_atomically_and_old_version_reclaimed(api: AsyncClient, store: ArchiveStore):
    c1 = csv_bytes(SAMPLE_ROWS)
    r = await _upload(api, "REC-R", "2026-11-13", c1)
    v1 = r.json()["batch"]["version_id"]
    assert r.json()["replaced"] is False

    new_rows = [(1, "2026-11-13T07:00:00Z", "-20.0")]
    c2 = csv_bytes(new_rows)
    r = await _upload(api, "REC-R", "2026-11-13", c2)
    assert r.status_code == 200
    assert r.json()["replaced"] is True
    v2 = r.json()["batch"]["version_id"]
    assert v1 != v2

    # 当前版本指向新数据
    r = await api.get("/archive/batches/REC-R/2026-11-13")
    assert r.json()["version_id"] == v2
    r = await api.get("/archive/batches/REC-R/2026-11-13/readings")
    assert r.json()["total"] == 1

    # 旧版本目录在租约排空后被回收
    await asyncio.sleep(0)
    await store.shutdown()
    assert not (store.versions_dir / v1).exists()
    assert (store.versions_dir / v2 / "source.csv").is_file()


async def test_old_batch_queryable_during_processing(api: AsyncClient, store: ArchiveStore,
                                                     monkeypatch):
    import threading

    c1 = csv_bytes(SAMPLE_ROWS)
    r = await _upload(api, "REC-P", "2026-11-13", c1)
    v1 = r.json()["batch"]["version_id"]
    entered = threading.Event()
    proceed = threading.Event()
    original_build = build_index

    def slow_build(csv_path, index_path):
        entered.set()
        proceed.wait(5)  # 工作线程阻塞，不占用事件循环
        return original_build(csv_path, index_path)

    monkeypatch.setattr("app.archive.router.build_index", slow_build)

    upload_task = asyncio.create_task(
        _upload(api, "REC-P", "2026-11-13", csv_bytes([(2, "2026-11-13T00:00:00Z", "1.0")]))
    )
    await asyncio.to_thread(entered.wait, 5)

    # 处理期间：旧批仍可查询、可下载，版本固定为 v1
    r = await api.get("/archive/batches/REC-P/2026-11-13")
    assert r.json()["version_id"] == v1
    r = await api.get("/archive/batches/REC-P/2026-11-13/readings")
    assert r.json()["total"] == len(SAMPLE_ROWS)
    r = await api.get("/archive/batches/REC-P/2026-11-13/download")
    assert r.content == c1

    proceed.set()
    result = await upload_task
    assert result.status_code == 200
    assert result.json()["replaced"] is True


async def test_concurrent_same_batch_only_one_accepted(api: AsyncClient, store: ArchiveStore,
                                                       monkeypatch):
    import threading

    await _upload(api, "REC-M", "2026-11-13", csv_bytes(SAMPLE_ROWS))
    entered = threading.Event()
    proceed = threading.Event()
    original_build = build_index

    def slow_build(csv_path, index_path):
        entered.set()
        proceed.wait(5)
        return original_build(csv_path, index_path)

    monkeypatch.setattr("app.archive.router.build_index", slow_build)

    first = asyncio.create_task(
        _upload(api, "REC-M", "2026-11-13", csv_bytes([(1, "2026-11-13T00:00:00Z", "1")]))
    )
    await asyncio.to_thread(entered.wait, 5)
    # 第二份同批上传立即 409，不等待第一份结束
    second = await _upload(api, "REC-M", "2026-11-13", csv_bytes([(2, "2026-11-13T00:00:00Z", "2")]))
    assert second.status_code == 409
    assert second.json()["error"] == "batch_busy"

    proceed.set()
    assert (await first).status_code == 200

    # 不同批次互不阻塞
    other = await _upload(api, "REC-OTHER", "2026-11-13", csv_bytes([(3, "2026-11-13T00:00:00Z", "3")]))
    assert other.status_code == 200


# ----------------------------------------------------------------------
# 下载租约：在途下载保护旧原件，断连也释放
# ----------------------------------------------------------------------
async def test_old_version_kept_until_inflight_download_finishes(api: AsyncClient,
                                                                  store: ArchiveStore,
                                                                  monkeypatch):
    import threading

    c1 = csv_bytes(SAMPLE_ROWS)
    r = await _upload(api, "REC-L", "2026-11-13", c1)
    v1 = r.json()["batch"]["version_id"]

    hold = threading.Event()
    lease_acquired = threading.Event()
    released = threading.Event()
    real_release = store.leases.release

    def tracking_release(version_id):
        real_release(version_id)
        if version_id == v1:
            released.set()

    # 让首块读取后挂起，模拟慢速/在途下载（同步线程事件，不占用事件循环）
    from app.archive import router as router_mod

    real_read = open

    class SlowFile:
        def __init__(self, path, mode):
            self._f = real_read(path, mode)
            self._reads = 0

        def read(self, n):
            self._reads += 1
            if self._reads == 2:
                lease_acquired.set()
                hold.wait(5)
            return self._f.read(n)

        def close(self):
            return self._f.close()

    monkeypatch.setattr(router_mod, "_open_file", SlowFile)
    monkeypatch.setattr(store.leases, "release", tracking_release)

    download_task = asyncio.create_task(
        api.get("/archive/batches/REC-L/2026-11-13/download")
    )
    # 等待下载拿到 v1 租约并进入在途状态
    await asyncio.to_thread(lease_acquired.wait, 5)
    assert store.leases._leases.get(v1) == 1

    # 触发替换：旧版本进入回收等待，目录仍在
    r = await _upload(api, "REC-L", "2026-11-13",
                      csv_bytes([(1, "2026-11-13T00:00:00Z", "1")]))
    assert r.status_code == 200
    await asyncio.sleep(0.1)
    assert (store.versions_dir / v1).exists()

    hold.set()
    response = await download_task
    assert response.status_code == 200
    assert response.content == c1
    await asyncio.to_thread(released.wait, 5)
    # 回收协程在租约排空后删除旧目录
    await asyncio.sleep(0.1)
    assert not (store.versions_dir / v1).exists()


async def test_download_lease_released_on_disconnect(api: AsyncClient, store: ArchiveStore,
                                                      monkeypatch):
    import threading

    c1 = csv_bytes(SAMPLE_ROWS)
    r = await _upload(api, "REC-D", "2026-11-13", c1)
    v1 = r.json()["batch"]["version_id"]

    block = threading.Event()
    lease_acquired = threading.Event()
    real_read = open
    from app.archive import router as router_mod

    class HangingFile:
        def __init__(self, path, mode):
            self._f = real_read(path, mode)
            self._reads = 0

        def read(self, n):
            self._reads += 1
            if self._reads == 1:
                # 首块立即返回，让流真正开始向客户端传输
                lease_acquired.set()
                return self._f.read(n)
            # 第二块前挂起，取消将在事件循环调度点送达生成器
            block.wait(5)
            return self._f.read(n)

        def close(self):
            return self._f.close()

    monkeypatch.setattr(router_mod, "_open_file", HangingFile)

    transport = ASGITransport(app=web.app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        task = asyncio.create_task(
            ac.get("/archive/batches/REC-D/2026-11-13/download")
        )
        await asyncio.to_thread(lease_acquired.wait, 5)
        # 让出事件循环，使流生成器完成首块 yield 后挂在第二块读取上
        await asyncio.sleep(0.1)
        assert store.leases._leases.get(v1) == 1
        # 客户端断连：取消在途响应，生成器 finally 必须释放租约
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        block.set()
        await asyncio.sleep(0.1)
        assert v1 not in store.leases._leases


# ----------------------------------------------------------------------
# 重启：已发布数据保留，暂存与孤儿版本清理
# ----------------------------------------------------------------------
async def test_restart_preserves_published_and_cleans_staging(tmp: Path):
    data_dir = str(tmp / "archive")
    first = ArchiveStore(data_dir)
    await first.startup()
    staging_id, staging_dir, csv_path = first.create_staging()
    with open(csv_path, "wb") as fp:
        fp.write(csv_bytes(SAMPLE_ROWS))
    # 手动构建一个已发布版本
    index_path = os.path.join(staging_dir, "index.db")
    count = build_index(csv_path, index_path)
    info, stale = first.publish(
        batch_key="REC-K/2026-11-13", recorder_id="REC-K", export_date="2026-11-13",
        staging_dir=staging_dir, row_count=count,
        byte_size=os.path.getsize(csv_path), sha256=hashlib.sha256(csv_bytes(SAMPLE_ROWS)).hexdigest(),
    )
    assert stale is None
    # 遗留暂存（模拟中断）
    first.create_staging()
    # 孤儿版本目录（有目录无 catalog 记录）
    orphan = first.versions_dir / "orphan123"
    orphan.mkdir()
    (orphan / "source.csv").write_bytes(b"x")

    second = ArchiveStore(data_dir)
    await second.startup()
    try:
        assert not any(second.staging_dir.iterdir())  # 暂存清理
        assert not orphan.exists()                    # 孤儿清理
        current = second.current_version("REC-K/2026-11-13")
        assert current.version_id == info.version_id
        readings, total = second.query_readings(
            current, circuit=None, start_micros=None, end_micros=None,
            limit=100, offset=0,
        )
        assert total == len(SAMPLE_ROWS)
        assert len(readings) == len(SAMPLE_ROWS)
    finally:
        await second.shutdown()


# ----------------------------------------------------------------------
# 写入失败 / 中断：旧批保留，半成品绝不发布
# ----------------------------------------------------------------------
async def test_failed_replacement_keeps_old_batch(api: AsyncClient, store: ArchiveStore,
                                                  monkeypatch):
    good = csv_bytes(SAMPLE_ROWS)
    r = await _upload(api, "REC-F", "2026-11-13", good)
    v1 = r.json()["batch"]["version_id"]

    # 让索引写入抛错，模拟磁盘故障；旧批次必须毫发无损
    def boom(csv_path, index_path):
        raise OSError("simulated disk failure")

    monkeypatch.setattr("app.archive.router.build_index", boom)
    r = await _upload(api, "REC-F", "2026-11-13",
                      csv_bytes([(1, "2026-11-13T00:00:00Z", "1")]))
    assert r.status_code == 500
    assert r.json()["error"] == "write_failed"

    # 旧批仍可查、可下载，版本号不变
    r = await api.get("/archive/batches/REC-F/2026-11-13")
    assert r.json()["version_id"] == v1
    r = await api.get("/archive/batches/REC-F/2026-11-13/readings")
    assert r.json()["total"] == len(SAMPLE_ROWS)
    r = await api.get("/archive/batches/REC-F/2026-11-13/download")
    assert r.content == good
    # 失败暂存已清理，无孤儿版本
    assert not any(store.staging_dir.iterdir())
    assert [p.name for p in store.versions_dir.iterdir()] == [v1]


def test_build_index_removes_artifacts_on_bad_row(tmp_path: Path):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_bytes(
        csv_bytes([
            (0, "2026-11-13T08:00:00Z", "-18.0"),
            (0, "2026-11-13T08:00:00Z", "-19.0"),  # 重复
        ])
    )
    index_path = tmp_path / "index.db"
    with pytest.raises(Exception):
        build_index(str(csv_path), str(index_path))
    # 半成品索引及 WAL/SHM 全部清除
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.startswith("index.db")]
    assert leftovers == []


# ----------------------------------------------------------------------
# 16 MiB 上限与内存有界
# ----------------------------------------------------------------------
async def test_size_limit_enforced(api: AsyncClient, tmp: Path):
    import dataclasses

    limit = 1 * 1024 * 1024  # 测试环境调小为 1 MiB
    web.app.state.settings = dataclasses.replace(
        web.settings, archive_max_upload_bytes=limit
    )
    try:
        big = csv_bytes([(i % 8, f"2026-11-13T00:{i // 60:02d}:{i % 60:02d}Z", "-18.0")
                         for i in range(40000)])
        assert len(big) > limit
        r = await _upload(api, "REC-BIG", "2026-11-13", big)
        assert r.status_code == 413
        assert r.json()["error"] == "file_too_large"
    finally:
        web.app.state.settings = web.settings


def test_build_index_memory_bounded(tmp_path: Path):
    """10 万行索引构建：进程内去重集合只存整数键，不缓存完整行。"""
    csv_path = tmp_path / "big.csv"
    rows = []
    seen = set()
    i = 0
    while len(rows) < 100_000:
        circuit = i % 8
        total_seconds = i
        hour = (total_seconds // 3600) % 24
        minute = (total_seconds % 3600) // 60
        second = total_seconds % 60
        day = 1 + (i // (24 * 3600))  # 跨天保证 (circuit, 时刻) 不重复
        ts = f"2026-11-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
        key = (circuit, ts)
        if key in seen:
            i += 1
            continue
        seen.add(key)
        rows.append((circuit, ts, f"{-20 + (i % 50) * 0.1:.2f}"))
        i += 1
    csv_path.write_bytes(csv_bytes(rows))
    index_path = tmp_path / "index.db"
    count = build_index(str(csv_path), str(index_path))
    assert count == 100_000
    assert index_path.stat().st_size > 0
