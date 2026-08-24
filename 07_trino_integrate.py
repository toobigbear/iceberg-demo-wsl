"""
07 Trino 对接 Iceberg 说明脚本
========================================
本套PyIceberg本地Demo使用 SqlCatalog + SQLite，仅用于Python侧学习。

⚠️【核心大坑】
PyIceberg SqlCatalog 支持 SQLite，但是 Trino 的 Iceberg Connector 的 sql‑catalog **不支持SQLite**。
Trino 使用 sql‑catalog 元存储，后端必须更换为 PostgreSQL / MySQL。

## 1、Trino Iceberg catalog配置示例 iceberg.properties
connector.name=iceberg
iceberg.catalog.type=sql
iceberg.catalog.sql.uri=jdbc:postgresql://127.0.0.1:5432/iceberg_meta
iceberg.catalog.sql.user=iceberg
iceberg.catalog.sql.password=123456
iceberg.catalog.sql.warehouse=file:///mnt/d/PythonProject/iceberg-demo-wsl/base/warehouse

## 2、容器部署额外坑
1）Trino容器必须挂载WSL的warehouse目录，文件路径两边完全一致；
2）PostgreSQL数据库、warehouse目录，Trino进程需要读写权限；
3）PyIceberg生成的Iceberg文件是标准格式，元数据库切换成PostgreSQL后，Trino可以直接读取全部表。

## 3、Trino SQL示例
-- 查询全表
SELECT * FROM nyc.taxis_basic;

-- 时间旅行，读取历史快照
SELECT * FROM nyc.taxis_basic FOR VERSION AS OF 2683604672762116215;

-- 字段过滤
SELECT vendor_id,trip_id,fare_amount FROM nyc.taxis_basic WHERE fare_amount > 20;

## 4、整套demo能力映射到湖仓一体化概念
01：建表、原始数据写入（湖仓建表落地）
03：快照、时间旅行（Iceberg核心能力）
04：Schema演进，线上表新增字段，不需要重写旧数据
05：行级delete，元数据标记删除，不修改原始parquet（Copy‑on‑Write模式）
06：快照过期清理、垃圾回收，生产元数据/文件膨胀治理
07：查询引擎Trino对接，湖仓的查询层

## 5、生产改造要点
1. sqlite → PostgreSQL/MySQL 元数据库
2. 本地file://仓库 → S3/minio对象存储
3. 配置快照过期、orphan文件清理策略
4. Trino/Presto作为查询计算引擎
"""

if __name__ == "__main__":
    print("查看源码注释获取Trino完整对接配置与SQL示例")
