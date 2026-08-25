"""
CDC 入湖演示
============
模拟 MySQL binlog → Kafka → Iceberg 的链路
ODS：原始CDC变更流水，保存全部I/U/D历史
DWD：用户最新状态快照表
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, TimestampType, NestedField
import pyarrow as pa
import pandas as pd
from datetime import datetime
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(BASE_DIR, "cdc_warehouse")
os.makedirs(WAREHOUSE, exist_ok=True)

catalog = load_catalog("cdc_catalog", **{
    "type": "sql",
    "uri": f"sqlite:///{os.path.join(BASE_DIR, 'cdc.db')}",
    "warehouse": WAREHOUSE,
})

try:
    catalog.create_namespace("ods")
except Exception:
    pass

try:
    catalog.create_namespace("dwd")
except Exception:
    pass

# ODS 层：原始数据，带 CDC 标记
# 👉修复：id 改为 required=False，适配pandas/pyarrow默认optional
schema_ods = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType()),
    NestedField(3, "age", LongType()),
    NestedField(4, "_op", StringType()),        # I=Insert, U=Update, D=Delete
    NestedField(5, "_ts", TimestampType()),     # 变更时间
)

ods_table_name = "ods.users_cdc"
if catalog.table_exists(ods_table_name):
    catalog.drop_table(ods_table_name)

ods_table = catalog.create_table(ods_table_name, schema=schema_ods)

# 模拟 CDC 数据流
cdc_events = [
    # 初始插入
    {"id": 1, "name": "Alice", "age": 25, "_op": "I", "_ts": datetime(2026, 8, 20, 10, 0, 0)},
    {"id": 2, "name": "Bob", "age": 30, "_op": "I", "_ts": datetime(2026, 8, 20, 10, 0, 0)},
    # 更新 Alice
    {"id": 1, "name": "Alice", "age": 26, "_op": "U", "_ts": datetime(2026, 8, 20, 11, 0, 0)},
    # 删除 Bob
    {"id": 2, "name": "Bob", "age": 30, "_op": "D", "_ts": datetime(2026, 8, 20, 12, 0, 0)},
    # 新增 Charlie
    {"id": 3, "name": "Charlie", "age": 35, "_op": "I", "_ts": datetime(2026, 8, 20, 12, 0, 0)},
]

print("📥 写入 CDC 原始数据：")
# 分两次写入，模拟流分批消费
for batch in [cdc_events[:2], cdc_events[2:]]:
    df_batch = pa.Table.from_pandas(pd.DataFrame(batch))
    ods_table.append(df_batch)
    print(f"   写入 {len(batch)} 条CDC事件")

print("\n📖 ODS 层原始数据（全量变更流水）：")
df_ods = ods_table.scan().to_pandas()
print(df_ods[["id", "name", "age", "_op", "_ts"]])

# ⭐ 构建 DWD 层：去重，取最新状态
print("\n📌 构建 DWD 层（取每个id最新状态，过滤删除事件）：")

# --------------------------
# ⚠️注意：这是DEMO内存实现！生产禁止，大数据会OOM
# 生产替换为Spark SQL：
# SELECT * FROM (
#    SELECT *, row_number() OVER (PARTITION BY id ORDER BY _ts DESC) rn
#    FROM ods.users_cdc
# ) t WHERE rn=1 AND _op != 'D'
# --------------------------
df = df_ods
latest_df = df.sort_values("_ts").groupby("id").last().reset_index()
latest_df = latest_df[latest_df["_op"] != "D"]
print(latest_df[["id", "name", "age", "_op", "_ts"]])

# --------------------------
# 把DWD结果落地为独立Iceberg表
# --------------------------
dwd_table_name = "dwd.users_latest"
if catalog.table_exists(dwd_table_name):
    catalog.drop_table(dwd_table_name)

dwd_table = catalog.create_table(dwd_table_name, schema=schema_ods)
dwd_table.append(pa.Table.from_pandas(latest_df))

print("\n✅ DWD层已经落地为Iceberg表 dwd.users_latest")
print("📖 读取DWD表：")
print(dwd_table.scan().to_pandas()[["id", "name", "age"]])
