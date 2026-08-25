"""
Iceberg CRUD 操作演示
=====================
V2 表格式支持行级 UPDATE / DELETE / MERGE
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
import pyarrow as pa
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "iceberg_warehouse_v2")
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

catalog = load_catalog(
    "demo_catalog",
    **{
        "type": "sql",
        "uri": f"sqlite:///{os.path.join(BASE_DIR, 'catalog_v2.db')}",
        "warehouse": WAREHOUSE_DIR,
    }
)

try:
    catalog.create_namespace("crm")
except NamespaceAlreadyExistsError:
    pass

table_name = "crm.users"
if catalog.table_exists(table_name):
    catalog.drop_table(table_name)

# ✅ 修复：全部用 nullable=True（默认），避免 required/optional 不匹配
schema = pa.schema([
    pa.field("user_id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("status", pa.string()),
    pa.field("score", pa.int64()),
])

table = catalog.create_table(table_name, schema=schema)

# 插入初始数据
df = pa.table({
    "user_id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "David"],
    "status": ["active", "active", "inactive", "active"],
    "score": [100, 85, 60, 90],
})
table.append(df)
print("✅ 初始数据：")
print(table.scan().to_pandas())

# ⭐ 模拟 UPDATE：用 overwrite 实现
print("\n📌 执行 UPDATE：Alice 分数改为 150...")
current = table.scan().to_arrow().to_pandas()
current.loc[current["user_id"] == 1, "score"] = 150
table.overwrite(pa.Table.from_pandas(current))
print(table.scan().to_pandas())

# ⭐ 模拟 DELETE：过滤掉 inactive 用户
print("\n📌 执行 DELETE：删除 inactive 用户...")
current = table.scan().to_arrow().to_pandas()
active_only = current[current["status"] == "active"].reset_index(drop=True)
table.overwrite(pa.Table.from_pandas(active_only))
print(table.scan().to_pandas())

print("\n📸 最终快照历史：")
for snap in table.snapshots():
    print(f"   {snap.snapshot_id}: {snap.summary['operation']}")