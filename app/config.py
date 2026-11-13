"""采集服务配置。控制器地址由环境变量固定，进程启动后不再变化。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True, slots=True)
class Settings:
    modbus_host: str
    modbus_port: int
    modbus_unit_id: int
    modbus_timeout: float
    register_count: int  # 回路数量，对应地址 0 ~ register_count-1
    # 历史温度归档：磁盘根目录与单次上传字节上限
    archive_data_dir: str
    archive_max_upload_bytes: int


def get_settings() -> Settings:
    max_mib = _env_int("ARCHIVE_MAX_UPLOAD_MIB", 16)
    return Settings(
        modbus_host=_env_str("MODBUS_HOST", "simulator"),
        modbus_port=_env_int("MODBUS_PORT", 5020),
        modbus_unit_id=_env_int("MODBUS_UNIT_ID", 1),
        modbus_timeout=_env_float("MODBUS_TIMEOUT_SECONDS", 2.0),
        register_count=_env_int("REGISTER_COUNT", 8),
        archive_data_dir=_env_str("ARCHIVE_DATA_DIR", "data/archive"),
        # 上限 16 MiB（需求固定），环境变量仅允许调小用于测试/受限环境
        archive_max_upload_bytes=max(1, max_mib) * 1024 * 1024,
    )
