# 冷库温度采集与历史归档 API（FastAPI + Modbus TCP）

单进程冷链温度服务，包含两套相互独立又共存的能力：

1. **实时采样**：通过 **Modbus TCP 功能码 03** 读取各回路当前温度（unit=1，
   保持寄存器地址 0~7，有符号 16 位整数 ÷ 10 得摄氏温度）。读数不缓存，
   每次调用都发起真实采集。
2. **历史归档**：以「记录仪 ID + 导出日期」确定批次，上传记录仪导出的历史
   温度 CSV，流式校验并建立磁盘索引，支持按回路与 UTC 时间闭区间回查、
   下载原始 CSV；同批重传原子整体替换，旧版本在途下载结束后回收。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

启动后：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集+归档 API | http://localhost:8000 | FastAPI 服务 |
| 交互式文档 | http://localhost:8000/docs | Swagger UI，可直接 Try it out |
| 演示页面 | http://localhost:8000/ | 实时采集、故障注入、归档导入/回查/下载 |
| 模拟控制器 Modbus | `simulator:5020`（宿主 `localhost:5020`） | Modbus TCP 从站 |
| 模拟器控制 API | http://localhost:8081 | 切换倒序回复 / 延迟 / 断线 / 异常 |

归档原件与索引保存在命名卷 `archive-data`（容器内 `/data/archive`），
容器重建或重启后已发布批次不丢失。停止：`docker compose down`
（加 `-v` 可同时清空归档卷）。

## 二、历史温度归档 API

批次由路径参数 `recorder_id`（记录仪 ID）与 `export_date`（导出日期
`YYYY-MM-DD`）唯一确定。

### CSV 格式约束

* 编码 **UTF-8**，单次上传 **不超过 16 MiB**；
* 表头必须严格为 `circuit,received_at,temperature_c`；
* `circuit`：整数回路编号 **0~7**；
* `received_at`：**UTC** 的 ISO 8601 时间（须带 `Z` 或 `+00:00`，拒绝无时区
  与非 UTC 偏移）；
* `temperature_c`：**有限十进制**数字（拒绝科学计数法、`NaN`、`Infinity`）；
* 同一批次内 `(circuit, received_at)` 组合必须唯一。

任一行不合法，整份上传被拒绝（HTTP 422），已发布的旧批次不受任何影响，
且不残留暂存或半成品索引。

### `POST /archive/batches/{recorder_id}/{export_date}` — 上传 / 整体替换

```bash
curl -X POST http://localhost:8000/archive/batches/REC-001/2026-11-13 \
  -F "file=@export.csv;type=text/csv"
# {"status":"published","replaced":false,
#  "batch":{"batch_key":"REC-001/2026-11-13","version_id":"…","row_count":6,
#           "byte_size":214,"sha256":"…","published_at":"…"}}
```

同批重传返回 `"replaced":true`，新版本整体替换旧版本。处理期间旧批次仍可
正常查询与下载；同批并发替换只接纳一份，其余立即返回 **409**。

### `GET /archive/batches` — 已发布批次清单

### `GET /archive/batches/{recorder_id}/{export_date}` — 批次元数据（当前固定版本）

### `GET .../readings` — 按回路与 UTC 时间闭区间回查

```bash
curl "http://localhost:8000/archive/batches/REC-001/2026-11-13/readings?circuit=0&start=2026-11-13T08:30:00Z&end=2026-11-13T09:00:00Z"
```

* `circuit` 可选（0~7）；`start`/`end` 为 ISO 8601 UTC，**两端都包含**；
* 支持 `limit`（默认 10000，上限 100000）与 `offset` 分页；
* 结果按 `circuit`、时间升序返回，响应含当前固定 `version_id`、命中总数
  `total` 与是否截断 `truncated`。

### `GET .../download` — 下载原始 CSV

返回上传时的**逐字节原件**（响应头含 `X-Archive-Version-Id`、
`X-Archive-Sha256`），客户端断连也会释放版本占用。

### 归档错误响应

| HTTP | error | 触发条件 |
| --- | --- | --- |
| 400 | `invalid_recorder_id` / `invalid_export_date` / `invalid_timestamp` / `invalid_range` | 路径或查询参数不合法 |
| 404 | `batch_not_found` | 批次尚未发布 |
| 409 | `batch_busy` | 同批次已有替换任务在途 |
| 413 | `file_too_large` | 上传超过 16 MiB |
| 415 | `unsupported_media_type` | 不是 CSV |
| 422 | `invalid_csv` | 表头/字段/编码/空行/列数等不合法 |
| 422 | `duplicate_reading` | 同批 `(回路, 时刻)` 重复（响应指出行号） |
| 500 | `write_failed` / `publish_failed` | 磁盘或发布故障，旧批次保留 |

## 三、归档关键设计（崩溃安全与内存有界）

实现见 `app/archive/`（`ingest.py` 流式校验、`store.py` 版本与租约、
`router.py` 路由）。

1. **流式校验、内存有界**：逐行读取、逐行校验；唯一性由 SQLite **前置
   唯一索引**强制，进程内只保留当前 2000 行分块及其块内去重集合，峰值
   内存与文件总行数无关（45 万行实测 Python 峰值约 0.8 MiB）。整个导入在
   单一事务内提交，失败整体回滚并删除半成品索引。
2. **原件与索引完整就绪才一起发布**：先在 `staging/<uuid>` 暂存目录内完成
   落盘、校验与建索引；通过后原子 `rename` 为不可变 `versions/<version_id>`，
   再在 catalog 的**一个短事务**内切换当前版本指针。坏行、写入失败、进程
   中断都不会发布半成品，旧批次始终可读。
3. **不全程持 SQLite 写锁**：catalog 仅在毫秒级短事务内写入；查询以
   `immutable=1` 只读 URI 打开版本索引，不产生任何写锁，读写互不阻塞。
4. **固定完整版本**：每次查询/下载在请求开始即确定 `version_id`，期间即使
   发生替换，本次请求仍读同一完整版本。
5. **旧原件延迟回收 + 引用计数**：被替换的旧版本不会立即删除。下载/查询先
   取得版本租约（`LeaseRegistry` 引用计数），回收协程等待全部在途租约释放
   后才删除目录；下载流的 `finally` 在客户端断连时同样释放租约。
6. **同批并发互斥**：每把批次一把 `asyncio.Lock`，同批并发上传只接纳一份
   （409），不同批次互不阻塞。
7. **重启恢复**：启动时清理上次遗留的全部暂存目录与无 catalog 记录的孤儿
   版本；已发布版本从 catalog 恢复，索引立即可查。

磁盘布局：

```
<ARCHIVE_DATA_DIR>/
├── catalog.db                 # 批次清单与当前版本指针（短事务写）
├── staging/<uuid>/            # 处理中的暂存（重启即清）
│   ├── source.csv
│   └── index.db
└── versions/<version_id>/     # 不可变已发布版本
    ├── source.csv             # 原始 CSV（逐字节）
    ├── index.db               # readings 表 + (circuit, time) 唯一索引
    └── meta.json
```

## 四、实时采样 API

### `GET /read/{circuit}` — 读取单个回路

`circuit` 即寄存器地址，范围 0~7。

```bash
curl http://localhost:8000/read/0
# {"circuit":0,"address":0,"temperature_c":-18.0,"raw_register":-180,
#  "received_at":"2026-11-12T04:13:03.376+00:00"}
```

### `GET /read/all` 与 `POST /read/batch`

`/read/all` 并发读取 8 个回路（单项失败不影响其他）；`/read/batch`
按给定回路列表并发采集。实时采样错误码：`illegal_address`(400)、
`timeout`(504)、`connection_lost`/`connect_failed`/`exception_response`/
`protocol_error`(502)。

实时链路通过单条 TCP 连接流水线在途、按 TID 匹配乱序响应、2 秒超时、
断线自动重连，详见 `app/modbus_client.py`。

## 五、模拟控制器与故障注入

`simulator/app.py` 在同一进程内提供 Modbus 从站（5020）与控制 API（8081）。
初始寄存器（实际温度 = 值 / 10）：

| 地址 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 原始值 | -180 | -175 | 50 | 235 | -200 | 300 | -45 | 105 |
| 温度 ℃ | -18.0 | -17.5 | 5.0 | 23.5 | -20.0 | 30.0 | -4.5 | 10.5 |

```bash
curl -X POST localhost:8081/mode   -H 'Content-Type: application/json' -d '{"reply_mode":"reverse"}'
curl -X POST localhost:8081/mode   -H 'Content-Type: application/json' -d '{"delay_seconds":2.5}'
curl -X POST localhost:8081/disconnect -H 'Content-Type: application/json' -d '{"mode":"next"}'
curl -X POST localhost:8081/exception  -H 'Content-Type: application/json' -d '{"address":2,"code":2,"count":2}'
curl -X POST localhost:8081/reset
```

## 六、本地开发（无 Docker）

```bash
pip install -r requirements-dev.txt

# 终端 1：模拟器
python -m simulator.app

# 终端 2：采集+归档 API
MODBUS_HOST=127.0.0.1 MODBUS_PORT=5020 \
  uvicorn app.main:app --reload --port 8000

# 测试（实时采样 27 个 + 历史归档 34 个，共 61 个用例）
pytest -q
```

## 七、配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MODBUS_HOST` | `simulator` | 控制器地址 |
| `MODBUS_PORT` | `5020` | 控制器 Modbus TCP 端口 |
| `MODBUS_UNIT_ID` | `1` | 单元标识符 |
| `MODBUS_TIMEOUT_SECONDS` | `2` | 单笔读取超时（秒） |
| `REGISTER_COUNT` | `8` | 回路数量（地址 0~N-1） |
| `ARCHIVE_DATA_DIR` | `data/archive` | 归档磁盘根目录（compose 挂载命名卷） |
| `ARCHIVE_MAX_UPLOAD_MIB` | `16` | 上传上限（MiB），仅允许调小 |

模拟器变量：`REGISTER_VALUES`、`REPLY_MODE`、`DELAY_SECONDS`、
`DISCONNECT_MODE`、`MODBUS_PORT`、`CONTROL_PORT`。

## 八、目录结构

```
.
├── app/
│   ├── main.py              # FastAPI 装配、实时采样路由、lifespan
│   ├── modbus_client.py     # 单连接流水线 Modbus 客户端
│   ├── config.py            # 环境变量配置
│   ├── static/index.html    # 演示页面（实时 + 归档）
│   └── archive/
│       ├── ingest.py        # CSV 流式校验 + SQLite 索引（内存有界）
│       ├── store.py         # 暂存/原子发布/版本租约/旧原件回收/重启恢复
│       └── router.py        # 归档上传、回查、下载、清单路由
├── simulator/app.py         # 本地模拟控制器（Modbus + 故障注入 API）
├── scripts/demo_concurrent.py
├── tests/                   # pytest 用例（实时 27 + 归档 34）
├── Dockerfile
└── docker-compose.yml
```
