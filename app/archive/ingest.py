"""历史温度 CSV 的流式校验与磁盘索引构建。

设计要点
========
* **内存有界**：逐行读取、逐行校验；同批去重只在 ``set`` 中保存紧凑整数键
  ``(circuit, micros)``（2 个 int/行），不缓存任何已通过的完整行；SQLite
  按块批量插入，内存占用不随行数线性增长。
* **先校验、后发布**：校验失败、写入失败都抛出 :class:`ArchiveError`，
  半成品索引立即删除，不会触碰已发布批次（发布由
  :mod:`app.archive.store` 在原子事务里完成）。
* **唯一约束双重保证**：应用层 ``seen`` 集合与 SQLite 唯一索引共同强制
  同批 ``(circuit, received_at)`` 组合唯一。
* 温度以「有限十进制」校验（拒绝科学计数法、NaN/Infinity 及溢出值），
  规范后的十进制文本原样入库，查询时再转 float 输出。

模块全部为同步代码，由路由层放入线程池执行，避免阻塞事件循环。
"""

from __future__ import annotations

import contextlib
import csv
import io
import os
import re
import sqlite3
from datetime import datetime, timezone

EXPECTED_HEADER = ["circuit", "received_at", "temperature_c"]
CIRCUIT_MIN = 0
CIRCUIT_MAX = 7
_INSERT_CHUNK = 2000

# 有限十进制：可选符号 + 至少一位数字（可含小数点），不允许指数/特殊值。
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$")
# ISO 8601 日期时间基本形态（细节由 fromisoformat 严格判定）。
_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:?\d{2})?$"
)


class ArchiveError(Exception):
    """归档处理失败基类（路由层统一映射 HTTP 状态码）。"""

    status_code = 400
    error_code = "invalid_csv"

    def __init__(self, message: str, *, line: int | None = None):
        super().__init__(message)
        self.message = message
        self.line = line

    def detail(self) -> dict:
        message = self.message if self.line is None else f"第 {self.line} 行：{self.message}"
        return {"error": self.error_code, "message": message}


class InvalidCsvError(ArchiveError):
    """CSV 结构或字段值不合法。"""

    status_code = 422
    error_code = "invalid_csv"


class DuplicateRowError(ArchiveError):
    """同批内 (circuit, received_at) 组合重复。"""

    status_code = 422
    error_code = "duplicate_reading"


class IndexWriteError(ArchiveError):
    """索引/原件写入失败（磁盘故障等），旧批次不受影响。"""

    status_code = 500
    error_code = "write_failed"


def parse_utc_timestamp(raw: str, line: int) -> tuple[int, str]:
    """校验 UTC 时间字符串，返回 ``(微秒级整数时间戳, 规范 ISO 文本)``。

    仅接受显式 UTC（``Z``/``z``/``+00:00``）的 ISO 8601 日期时间；
    秒以下最多保留微秒（Python 3.11 的 fromisoformat 对超过 6 位小数报错）。
    规范化文本（``...+00:00``）本身即合法标准 ISO 表示，直接复用，避免重复格式化。
    """
    text = raw.strip()
    if not _ISO_RE.match(text):
        raise InvalidCsvError(
            f"received_at 不是合法 ISO 8601 时间：{raw!r}", line=line
        )
    normalized = text.replace(" ", "T")
    if normalized.endswith(("Z", "z")):
        normalized = normalized[:-1] + "+00:00"
    # 归一化 +0000 → +00:00
    if len(normalized) >= 5 and normalized[-5] in "+-" and normalized[-3] != ":":
        normalized = normalized[:-2] + ":" + normalized[-2:]
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        raise InvalidCsvError(f"received_at 无法解析：{raw!r}", line=line) from None
    if dt.tzinfo is None:
        raise InvalidCsvError(
            f"received_at 必须带 UTC 时区标记（Z 或 +00:00）：{raw!r}", line=line
        )
    if dt.utcoffset().total_seconds() != 0:
        raise InvalidCsvError(f"received_at 必须为 UTC 时间：{raw!r}", line=line)
    dt = dt.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt - epoch
    micros = (
        delta.days * 86_400 * 1_000_000
        + delta.seconds * 1_000_000
        + dt.microsecond
    )
    return micros, normalized


def parse_circuit(raw: str, line: int) -> int:
    text = raw.strip()
    if not text:
        raise InvalidCsvError("circuit 不能为空", line=line)
    try:
        value = int(text)
    except ValueError:
        raise InvalidCsvError(f"circuit 必须是 0~7 的整数：{raw!r}", line=line) from None
    if not CIRCUIT_MIN <= value <= CIRCUIT_MAX:
        raise InvalidCsvError(f"circuit 必须在 0~7 之间，收到 {value}", line=line)
    return value


def parse_temperature(raw: str, line: int) -> tuple[str, float]:
    """校验有限十进制温度，返回 ``(原始十进制文本, float 值)``。

    正则已限定为可选符号的十进制数字（拒绝指数、NaN、Infinity 及空值），
    因此 ``float()`` 对通过正则的输入必然得到有限数；再做一次有限性兜底，
    拒绝极端溢出。原件文本原样保留，查询时以 float 输出数值。
    """
    text = raw.strip()
    if not text or not _DECIMAL_RE.match(text):
        raise InvalidCsvError(
            "temperature_c 必须是有限十进制数字（不支持指数/NaN/Infinity）："
            f"{raw!r}",
            line=line,
        )
    float_value = float(text)
    # 兜底：拒绝极端溢出（正则通过的常规输入不会触发），保证 JSON 始终是有限数。
    if float_value != float_value or float_value in (float("inf"), float("-inf")):
        raise InvalidCsvError(f"temperature_c 超出可表示范围：{raw!r}", line=line)
    return text, float_value


def build_index(csv_path: str, index_path: str) -> int:
    """流式校验 ``csv_path`` 并在 ``index_path`` 构建 SQLite 索引。

    返回写入的读数行数。任何失败都抛出 :class:`ArchiveError` 并删除半成品
    索引文件，保证「原件与索引完整就绪才一起发布」。

    内存有界
    ========
    唯一性完全由 SQLite 的 **前置唯一索引** 强制，进程内只保留当前分块
    （:data:`_INSERT_CHUNK` 行，默认 2000）的待插入数据与块内去重集合，
    峰值内存与文件总行数无关；整个导入在单个事务内提交，最后强制落盘。
    """
    try:
        raw_file = open(csv_path, "rb")
    except OSError as exc:
        raise IndexWriteError(f"无法打开上传原件：{exc.__class__.__name__}") from exc

    row_count = 0
    # 仅当前分块的 (circuit, micros) 块内去重集合，大小恒为 _INSERT_CHUNK。
    chunk_seen: set[tuple[int, int]] = set()
    pending: list[tuple[int, int, str, str]] = []
    chunk_first_line = 2  # 首个数据行位于文件第 2 行（第 1 行为表头）
    connection: sqlite3.Connection | None = None
    text_stream: io.TextIOWrapper | None = None
    line_no = 1
    try:
        # errors="strict"：非 UTF-8 字节抛 UnicodeDecodeError → InvalidCsvError。
        # utf-8-sig 兼容带 BOM 的记录仪/Excel 导出（无 BOM 时行为与 utf-8 一致）。
        text_stream = io.TextIOWrapper(
            raw_file, encoding="utf-8-sig", errors="strict", newline=""
        )
        reader = csv.reader(text_stream)
        try:
            header = next(reader)
        except StopIteration:
            raise InvalidCsvError("CSV 文件为空，缺少表头") from None
        if [h.strip() for h in header] != EXPECTED_HEADER:
            raise InvalidCsvError(
                f"表头必须严格为 {','.join(EXPECTED_HEADER)}，实际为 {','.join(header)!r}"
            )

        connection = _open_index_db(index_path)
        # 整个导入一个事务：任一行失败整体回滚，半成品绝不发布。
        connection.execute("BEGIN")
        for row in reader:
            line_no += 1
            if not row or all(cell == "" for cell in row):
                raise InvalidCsvError("存在空行", line=line_no)
            if len(row) != 3:
                raise InvalidCsvError(
                    "每行必须恰好 3 列（circuit,received_at,temperature_c），"
                    f"实际 {len(row)} 列",
                    line=line_no,
                )
            circuit = parse_circuit(row[0], line_no)
            micros, iso_text = parse_utc_timestamp(row[1], line_no)
            temp_text, _ = parse_temperature(row[2], line_no)

            key = (circuit, micros)
            # 块内重复：直接定位到当前行号，无需查询数据库。
            if key in chunk_seen:
                raise DuplicateRowError(
                    f"回路 {circuit} 在 {iso_text} 的读数重复", line=line_no
                )
            chunk_seen.add(key)
            pending.append((circuit, micros, iso_text, temp_text))
            row_count += 1
            if len(pending) >= _INSERT_CHUNK:
                _insert_chunk(connection, pending, chunk_first_line)
                pending.clear()
                chunk_seen.clear()
                chunk_first_line = line_no + 1

        if row_count == 0:
            raise InvalidCsvError("CSV 只有表头，没有任何读数行")
        if pending:
            _insert_chunk(connection, pending, chunk_first_line)
            pending.clear()
            chunk_seen.clear()
        connection.execute("COMMIT")
        # 持久化校验：强制 WAL 落盘，发布后断电/重启仍可检索。
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except UnicodeDecodeError as exc:
        raise InvalidCsvError(
            f"文件不是合法 UTF-8 编码：字节 {exc.start} 附近解码失败"
        ) from None
    except csv.Error as exc:
        raise InvalidCsvError(
            f"CSV 结构解析失败：{exc}", line=line_no if line_no > 1 else None
        ) from None
    except sqlite3.Error as exc:
        raise IndexWriteError(f"索引数据库写入失败：{exc.__class__.__name__}") from exc
    finally:
        if connection is not None:
            with contextlib.suppress(sqlite3.Error, OSError):
                # 异常路径事务仍处于打开状态时回滚，避免残留写事务。
                connection.rollback()
                connection.close()
        if text_stream is not None:
            with contextlib.suppress(OSError):
                text_stream.close()
        else:
            with contextlib.suppress(OSError):
                raw_file.close()
        # 异常路径：半成品索引一律清除，绝不发布。
        if row_count == 0 or _unwinding():
            _remove_index_artifacts(index_path)
    return row_count


def _open_index_db(index_path: str) -> sqlite3.Connection:
    """建表并 **前置建立唯一索引**：唯一性由数据库强制，应用内存有界。"""
    try:
        connection = sqlite3.connect(index_path)
    except sqlite3.Error as exc:
        raise IndexWriteError(f"无法创建索引数据库：{exc.__class__.__name__}") from exc
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        # 暂存构建阶段使用 NORMAL：整体在一个事务内，失败即删库，
        # 发布前再做 TRUNCATE 检查点强制落盘，兼顾吞吐与发布后持久性。
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute(
            "CREATE TABLE readings ("
            " circuit INTEGER NOT NULL,"
            " received_at_micros INTEGER NOT NULL,"
            " received_at TEXT NOT NULL,"
            " temperature_c TEXT NOT NULL"
            ")"
        )
        # 唯一索引前置：后续 INSERT 冲突即报 IntegrityError，
        # 应用无需保存全量键集合，内存不随行数增长。
        connection.execute(
            "CREATE UNIQUE INDEX idx_readings_circuit_time "
            "ON readings(circuit, received_at_micros)"
        )
        connection.commit()
    except sqlite3.Error as exc:
        with contextlib.suppress(sqlite3.Error):
            connection.close()
        raise IndexWriteError(f"索引初始化失败：{exc.__class__.__name__}") from exc
    return connection


def _insert_chunk(
    connection: sqlite3.Connection,
    rows: list[tuple[int, int, str, str]],
    first_line_no: int,
) -> None:
    """在当前事务内逐行插入一个分块；跨块重复时精确定位到行号。

    刻意使用逐行 ``INSERT`` 而非 ``executemany``：后者在唯一约束冲突时会
    中止整条语句并回滚该语句的全部改动，使事后无法根据表内容判断究竟是
    分块内哪一行冲突。逐行插入一旦冲突即可用当前行号直接报错，且因为仍在
    单一事务内，失败时整体回滚、半成品不会发布。
    """
    for offset, candidate in enumerate(rows):
        try:
            connection.execute(
                "INSERT INTO readings"
                "(circuit, received_at_micros, received_at, temperature_c)"
                " VALUES (?, ?, ?, ?)",
                candidate,
            )
        except sqlite3.IntegrityError as exc:
            # 块内重复已由 chunk_seen 拦截；走到这里必然是跨块重复。
            raise DuplicateRowError(
                f"回路 {candidate[0]} 在 {candidate[2]} 的读数重复",
                line=first_line_no + offset,
            ) from exc
        except sqlite3.Error as exc:
            raise IndexWriteError(
                f"索引批量写入失败：{exc.__class__.__name__}"
            ) from exc


def _remove_index_artifacts(index_path: str) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        with contextlib.suppress(OSError):
            os.remove(index_path + suffix)


def _unwinding() -> bool:
    """finally 中判断当前是否处于异常展开路径。"""
    import sys

    return sys.exc_info()[0] is not None
