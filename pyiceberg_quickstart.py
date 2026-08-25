from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.types import LongType
import pyarrow as pa
import pandas as pd
import os

#============================================================
# 第 1 步：确定数据存放位置
# ===========================================================
# 自动取脚本当前目录，不再硬编码/mnt/d
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "iceberg_warehouse")
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

# ============================================================
# 第 2 步：创建 Catalog（表的"注册中心"）
# ============================================================
catalog = load_catalog(
    "demo_catalog",
    **{
        "type": "sql",
        "uri": f"sqlite:///{os.path.join(BASE_DIR, 'iceberg_catalog.db')}",
        "warehouse": WAREHOUSE_DIR,
    }
)

# ============================================================
# 第 3 步：创建命名空间（类似 MySQL 的 database）
# ============================================================

try:
    catalog.create_namespace("default")
    print("✅ 创建 namespace default 成功")
except NamespaceAlreadyExistsError:
    print("ℹ️ namespace default 已存在，跳过")

# ============================================================
# 第 4 步：准备数据
# ============================================================

df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

# ============================================================
# 第 5 步：创建 Iceberg 表
# ============================================================

table_name = "default.test_table"

if catalog.table_exists(table_name):
    catalog.drop_table(table_name)
    print("ℹ️ 旧表已删除")

table = catalog.create_table(table_name, schema=pa.Schema.from_pandas(df)) # 自动从 pandas 推断 Schema
# 写入数据（pandas → PyArrow → Iceberg）
table.append(pa.Table.from_pandas(df))
print("✅ 数据写入成功")

# ============================================================
# 第 6 步：查询数据
# ============================================================
print("\n📖 读取结果：")
print(table.scan().to_pandas())

# ============================================================
# 第 7 步：⭐ 时间旅行 —— 查看快照历史
# ============================================================
print("\n📸 快照列表：")
for snap in table.metadata.snapshots:
    print(f"   Snapshot id: {snap.snapshot_id}")
# 用第一个快照 ID 查询历史数据
first_snap_id = table.metadata.snapshots[0].snapshot_id
print("\n⏳ 时间旅行读取历史快照：")
print(table.scan(snapshot_id=first_snap_id).to_pandas())

# ============================================================
# 第 8 步：⭐ Schema 演进 —— 添加新列
# ============================================================

print("\n📌 Schema 演进 - 添加 age 列 ...")
with table.update_schema() as update:
    update.add_column("age", LongType())
print("✅ 新列添加成功")

df2 = pd.DataFrame({"id": [4], "name": ["d"], "age": [25]})
table.append(pa.Table.from_pandas(df2))

print("\n📌 全部数据（老数据 age 为 NULL）：")
print(table.scan().to_pandas())
