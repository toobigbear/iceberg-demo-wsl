"""
Iceberg 分区演示
================
隐藏分区：用户不感知分区字段，查询自动优化
注意：pyiceberg Python库表达式API迭代频繁，Python侧scan过滤坑较多；
分区裁剪能力主要在 Trino / Spark SQL 引擎中发挥。
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, TimestampType, StringType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform  # 按天分区
import pyarrow as pa
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(BASE_DIR, "partition_warehouse")
os.makedirs(WAREHOUSE, exist_ok=True)

catalog = load_catalog("demo", **{
    "type": "sql",
    "uri": f"sqlite:///{os.path.join(BASE_DIR, 'partition.db')}",
    "warehouse": WAREHOUSE,
})

try:
    catalog.create_namespace("logs")
except Exception:
    pass

# ⭐ 关键：分区字段不在 Schema 中！这是"隐藏分区"
schema = Schema(
    NestedField(1, "event_id", LongType()),
    NestedField(2, "event_time", TimestampType()),  # 原始字段
    NestedField(3, "user_id", StringType()),
    NestedField(4, "action", StringType()),
)

# 按 event_time 的"天"分区，但用户看不到这个分区列
partition_spec = PartitionSpec(
    PartitionField(
        source_id=2,           # 基于 event_time (field_id=2)
        field_id=1000,
        transform=DayTransform(),  # 按天转换
        name="event_day"       # 分区名（隐藏）
    )
)

table_name = "logs.events"
if catalog.table_exists(table_name):
    catalog.drop_table(table_name)

table = catalog.create_table(
    table_name,
    schema=schema,
    partition_spec=partition_spec,
)

# 插入 3 天的数据
base_time = datetime(2026, 8, 20, 10, 0, 0)

for i in range(3):
    day_time = base_time + timedelta(days=i)
    df = pa.table({
        "event_id": [100 + i],
        "event_time": [day_time],
        "user_id": [f"user_{i}"],
        "action": [["click", "view", "purchase"][i]],
    })
    table.append(df)
    print(f"✅ 写入 {day_time.date()} 的数据")

print("\n📌 查询全部数据（不感知分区）：")
df_all = table.scan().to_pandas()
print(df_all)

print("\n📁 底层目录结构（按天分区，event_day为隐藏分区）：")
data_path = os.path.join(WAREHOUSE, "logs", "events", "data")
for root, dirs, files in os.walk(data_path):
    level = root.replace(WAREHOUSE, '').count(os.sep)
    indent = '  ' * level
    print(f'{indent}{os.path.basename(root)}/')

print("""
💡说明：
1. 磁盘物理目录存在 event_day=2026‑08‑20 这类分区文件夹；
2. 但是表Schema中没有event_day字段，这就是Iceberg【隐藏分区】；
3. 在Trino/Spark中执行SQL：
   SELECT * FROM logs.events WHERE event_time >= '2026‑08‑21T00:00:00';
   👉直接写原始字段event_time，引擎内部自动转换event_day做分区裁剪，跳过不需要读取的parquet文件；
4. pyiceberg Python库的scan过滤接口还在快速迭代，版本之间API改动大，不适合作为演示分区裁剪的载体。
""")
