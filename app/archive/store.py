"""归档批次的磁盘存储、原子发布与版本生命周期管理。

磁盘布局（``data_dir`` 根目录）::

    catalog.db                 全局清单（仅短事务写，绝不全程持写锁）
    staging/<uuid>/            处理中的暂存批次（原件 + 半成品索引）
        source.csv
        index.db(-wal/-shm)
    versions/<version_id>/     已发布、不可变的版本目录
        source.csv
        index.db
        meta.json

发布协议（崩溃安全）
====================
1. 暂存目录内流式校验并构建索引，任何失败整体丢弃，旧批次毫发无损；
2. ``staging/<uuid>`` 原子 rename 为 ``versions/<version_id>``（同文件系统）；
3. 在 **一个短 SQLite 事务** 内写入 batches 行并切换 current_versions 指针，
   提交即发布。提交前崩溃 → 重启清理「无 catalog 记录的孤儿版本目录」；
   提交后崩溃 → 版本已发布，重启后照常可查。

旧版本回收
==========
被替换的旧版本目录不会立即删除：下载/查询先通过 :class:`LeaseRegistry`
取得租约（引用计数），回收协程等待在途租约全部释放后才真正删除；
客户端断连时流生成器的 ``finally`` 同样会释放租约。

并发替换
========
每把批次一把 :class:`asyncio.Lock`，同批并发上传只接纳一份，其余 409；
不同批次互不阻塞。锁只在单进程事件循环内生效，跨进程的最终互斥由
catalog 的短事务 + 版本指针保证。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    version_id       TEXT PRIMARY KEY,
    batch_key        TEXT NOT NULL,
    recorder_id      TEXT NOT NULL,
    export_date      TEXT NOT NULL,
    version_dir      TEXT NOT NULL,
    row_count        INTEGER NOT NULL,
    byte_size        INTEGER NOT NULL,
    sha256           TEXT NOT NULL,
    published_at     TEXT NOT NULL,
    replaced_version TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_batch_key ON batches(batch_key);
CREATE TABLE IF NOT EXISTS current_versions (
    batch_key  TEXT PRIMARY KEY,
    version_id TEXT NOT NULL
);
"""


class BatchNotFound(LookupError):
    """批次尚未发布。"""


class BatchBusy(RuntimeError):
    """同批次已有替换在途。"""


@dataclass(frozen=True, slots=True)
class VersionInfo:
    """一个已发布版本的不可变元数据。"""

    batch_key: str
    recorder_id: str
    export_date: str
    version_id: str
    version_dir: str
    row_count: int
    byte_size: int
    sha256: str
    published_at: str
    replaced_version: str | None

    @property
    def csv_path(self) -> str:
        return os.path.join(self.version_dir, "source.csv")

    @property
    def index_path(self) -> str:
        return os.path.join(self.version_dir, "index.db")

    def as_dict(self) -> dict:
        return {
            "batch_key": self.batch_key,
            "recorder_id": self.recorder_id,
            "export_date": self.export_date,
            "version_id": self.version_id,
            "row_count": self.row_count,
            "byte_size": self.byte_size,
            "sha256": self.sha256,
            "published_at": self.published_at,
        }


class LeaseRegistry:
    """版本目录引用计数：租约未释放前，回收协程不得删除目录。

    所有方法仅在事件循环线程调用，无需线程锁。
    """

    def __init__(self) -> None:
        self._leases: dict[str, int] = {}
        self._waiters: dict[str, list[asyncio.Event]] = {}

    def acquire(self, version_id: str) -> None:
        self._leases[version_id] = self._leases.get(version_id, 0) + 1

    def release(self, version_id: str) -> None:
        count = self._leases.get(version_id, 0)
        if count <= 1:
            self._leases.pop(version_id, None)
            for event in self._waiters.pop(version_id, []):
                event.set()
        else:
            self._leases[version_id] = count - 1

    async def wait_drained(self, version_id: str) -> None:
        """等待某版本的全部在途租约释放（已无租约则立即返回）。"""
        while version_id in self._leases:
            event = asyncio.Event()
            self._waiters.setdefault(version_id, []).append(event)
            # 并发 release 可能已把它清空并通知过，重新检查后再等。
            if version_id not in self._leases:
                self._waiters.get(version_id, []).remove(event)
                return
            try:
                await event.wait()
            finally:
                with contextlib.suppress(ValueError):
                    self._waiters.get(version_id, []).remove(event)

    @contextlib.contextmanager
    def lease(self, version_id: str):
        self.acquire(version_id)
        try:
            yield
        finally:
            self.release(version_id)


class ArchiveStore:
    """归档存储门面：暂存、发布、查询、下载与回收。"""

    def __init__(self, data_dir: str) -> None:
        self.root = Path(data_dir)
        self.staging_dir = self.root / "staging"
        self.versions_dir = self.root / "versions"
        self.catalog_path = str(self.root / "catalog.db")
        self._batch_locks: dict[str, asyncio.Lock] = {}
        self._reaper_tasks: set[asyncio.Task] = set()
        self.leases = LeaseRegistry()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def startup(self) -> None:
        """建目录、建清单，并清理上次进程遗留的暂存/孤儿数据。"""
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._init_catalog)
        await asyncio.to_thread(self._cleanup_legacies)

    async def shutdown(self) -> None:
        """停机：等待已派发的回收任务结束（测试与优雅退出使用）。"""
        tasks = list(self._reaper_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _connect_catalog(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.catalog_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _init_catalog(self) -> None:
        with self._connect_catalog() as connection:
            connection.executescript(_SCHEMA)

    def _cleanup_legacies(self) -> None:
        """重启清理：暂存目录全清；无 catalog 记录的版本目录回收。"""
        if self.staging_dir.exists():
            for child in self.staging_dir.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
        recorded: set[str] = set()
        with self._connect_catalog() as connection:
            for row in connection.execute("SELECT version_dir FROM batches"):
                recorded.add(os.path.abspath(row["version_dir"]))
        if not self.versions_dir.exists():
            return
        for child in self.versions_dir.iterdir():
            if not child.is_dir():
                continue
            if os.path.abspath(str(child)) not in recorded:
                shutil.rmtree(child, ignore_errors=True)
                continue
            # 已发布版本若原件/索引缺失（极端磁盘损坏），摘牌并回收目录，
            # 保证查询永远不会指向不完整版本。
            if not (child / "source.csv").is_file() or not (child / "index.db").is_file():
                version_id = child.name
                with self._connect_catalog() as connection:
                    connection.execute(
                        "DELETE FROM current_versions WHERE version_id=?",
                        (version_id,),
                    )
                    connection.execute("DELETE FROM batches WHERE version_id=?", (version_id,))
                shutil.rmtree(child, ignore_errors=True)

    # ------------------------------------------------------------------
    # 批次锁：同批并发替换只接纳一份
    # ------------------------------------------------------------------
    @contextlib.asynccontextmanager
    async def batch_lock(self, batch_key: str):
        lock = self._batch_locks.setdefault(batch_key, asyncio.Lock())
        if lock.locked():
            raise BatchBusy(batch_key)
        await lock.acquire()
        try:
            yield
        finally:
            lock.release()

    # ------------------------------------------------------------------
    # 暂存与发布
    # ------------------------------------------------------------------
    def create_staging(self) -> tuple[str, str, str]:
        """创建一个全新暂存目录，返回 ``(staging_id, 目录, 原件路径)``。"""
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        staging_id = uuid.uuid4().hex
        directory = self.staging_dir / staging_id
        directory.mkdir(parents=True, exist_ok=False)
        return staging_id, str(directory), str(directory / "source.csv")

    def discard_staging(self, staging_dir: str) -> None:
        # 只删本次暂存子目录，保留 staging 根目录本身。
        shutil.rmtree(staging_dir, ignore_errors=True)

    def publish(
        self,
        *,
        batch_key: str,
        recorder_id: str,
        export_date: str,
        staging_dir: str,
        row_count: int,
        byte_size: int,
        sha256: str,
    ) -> tuple[VersionInfo, tuple[str, str] | None]:
        """原子发布暂存批次。

        返回 ``(新版本信息, 待回收旧版本 (version_id, version_dir) | None)``。
        旧版本在同一事务内摘牌，回收动作由事件循环侧调用
        :meth:`schedule_reclaim` 安排（本方法在线程中执行，不能建任务）。
        """
        version_id = uuid.uuid4().hex
        target = self.versions_dir / version_id
        # 同文件系统原子 rename：rename 后目录内容即不可变版本。
        os.rename(staging_dir, target)
        version_dir = str(target)
        published_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        info = VersionInfo(
            batch_key=batch_key,
            recorder_id=recorder_id,
            export_date=export_date,
            version_id=version_id,
            version_dir=version_dir,
            row_count=row_count,
            byte_size=byte_size,
            sha256=sha256,
            published_at=published_at,
            replaced_version=None,
        )
        # 元数据落盘（catalog 的冗余快照，便于离线核对原件）。
        with open(os.path.join(version_dir, "meta.json"), "w", encoding="utf-8") as fp:
            json.dump(info.as_dict(), fp, ensure_ascii=False, indent=2)

        old_version: str | None = None
        old_dir: str | None = None
        try:
            with self._connect_catalog() as connection:
                row = connection.execute(
                    "SELECT c.version_id AS version_id, b.version_dir AS version_dir"
                    " FROM current_versions c"
                    " JOIN batches b ON b.version_id = c.version_id"
                    " WHERE c.batch_key=?",
                    (batch_key,),
                ).fetchone()
                if row is not None:
                    old_version = row["version_id"]
                    old_dir = row["version_dir"]
                # 整个切换在单个短事务内完成，锁仅持有毫秒级。
                connection.execute(
                    "INSERT INTO batches"
                    "(version_id, batch_key, recorder_id, export_date, version_dir,"
                    " row_count, byte_size, sha256, published_at, replaced_version)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        version_id, batch_key, recorder_id, export_date, version_dir,
                        row_count, byte_size, sha256, published_at, old_version,
                    ),
                )
                connection.execute(
                    "INSERT INTO current_versions(batch_key, version_id) VALUES (?,?)"
                    " ON CONFLICT(batch_key) DO UPDATE SET version_id=excluded.version_id",
                    (batch_key, version_id),
                )
        except Exception:
            # 事务失败：把目录移回暂存并交由上层丢弃，绝不让孤儿版本可见。
            with contextlib.suppress(OSError):
                os.rename(target, self.staging_dir / version_id)
            raise

        stale = (old_version, old_dir) if old_version is not None and old_dir is not None else None
        # 回填替换来源，便于响应体与 meta 核对。
        if stale is not None:
            object.__setattr__(info, "replaced_version", old_version)
        return info, stale

    def schedule_reclaim(self, version_id: str, version_dir: str) -> None:
        """在事件循环线程安排旧版本回收（等租约排空后删除目录）。"""
        task = asyncio.create_task(
            self._reclaim_when_drained(version_id, version_dir),
            name=f"archive-reclaim-{version_id[:8]}",
        )
        self._reaper_tasks.add(task)
        task.add_done_callback(self._reaper_tasks.discard)

    async def _reclaim_when_drained(self, version_id: str, version_dir: str) -> None:
        """等在途下载/查询全部结束后删除旧原件；断连也会释放租约。"""
        await self.leases.wait_drained(version_id)
        await asyncio.to_thread(shutil.rmtree, version_dir, True)

    # ------------------------------------------------------------------
    # 读取路径
    # ------------------------------------------------------------------
    def current_version(self, batch_key: str) -> VersionInfo:
        with self._connect_catalog() as connection:
            row = connection.execute(
                "SELECT b.* FROM batches b"
                " JOIN current_versions c ON c.version_id = b.version_id"
                " WHERE c.batch_key=?",
                (batch_key,),
            ).fetchone()
        if row is None:
            raise BatchNotFound(batch_key)
        return _row_to_version(row)

    def list_batches(self) -> list[VersionInfo]:
        with self._connect_catalog() as connection:
            rows = connection.execute(
                "SELECT b.* FROM batches b"
                " JOIN current_versions c ON c.version_id = b.version_id"
                " ORDER BY b.recorder_id, b.export_date"
            ).fetchall()
        return [_row_to_version(row) for row in rows]

    def query_readings(
        self,
        info: VersionInfo,
        *,
        circuit: int | None,
        start_micros: int | None,
        end_micros: int | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict], int]:
        """在版本索引上执行只读查询，返回 ``(读数列表, 命中总数)``。

        以 immutable URI 打开，杜绝任何写锁；调用方须持有该版本租约，
        保证查询期间目录不会被回收。
        """
        clauses: list[str] = []
        params: list[int] = []
        if circuit is not None:
            clauses.append("circuit = ?")
            params.append(circuit)
        if start_micros is not None:
            clauses.append("received_at_micros >= ?")
            params.append(start_micros)
        if end_micros is not None:
            clauses.append("received_at_micros <= ?")
            params.append(end_micros)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        uri = f"file:{info.index_path}?immutable=1"
        connection = sqlite3.connect(uri, uri=True, timeout=30.0)
        try:
            total = connection.execute(
                f"SELECT COUNT(*) FROM readings{where}", params
            ).fetchone()[0]
            rows = connection.execute(
                f"SELECT circuit, received_at, temperature_c FROM readings{where}"
                " ORDER BY circuit, received_at_micros"
                " LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        finally:
            connection.close()
        readings = [
            {
                "circuit": row[0],
                "received_at": row[1],
                "temperature_c": float(row[2]),
            }
            for row in rows
        ]
        return readings, total


def _row_to_version(row: sqlite3.Row) -> VersionInfo:
    return VersionInfo(
        batch_key=row["batch_key"],
        recorder_id=row["recorder_id"],
        export_date=row["export_date"],
        version_id=row["version_id"],
        version_dir=row["version_dir"],
        row_count=row["row_count"],
        byte_size=row["byte_size"],
        sha256=row["sha256"],
        published_at=row["published_at"],
        replaced_version=row["replaced_version"],
    )
