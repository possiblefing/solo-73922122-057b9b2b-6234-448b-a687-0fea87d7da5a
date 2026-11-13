"""历史温度归档子系统。

* :mod:`app.archive.ingest` —— CSV 流式校验与 SQLite 索引构建（同步、内存有界）
* :mod:`app.archive.store`  —— 批次暂存/原子发布/版本租约与旧原件回收
* :mod:`app.archive.router` —— FastAPI 路由（上传、回查、下载、批次清单）
"""
