"""
Iceberg Schema 约束与演化
========================
required 字段、列重命名、新增列；
重要提醒：required=True 仅元数据标记，pyiceberg写入不会校验业务数据NULL；
非空业务校验由 Spark / Trino 计算引擎执行。
Iceberg 依靠 field_id 识别字段，不是列名字。
⚠️注意：Iceberg required=True，对应的pyarrow字段必须 nullable=False，否则append直接报schema不匹配。
"""

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, NestedField
import pyarrow as pa
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(BASE_DIR, "constraint_warehouse")
os.makedirs(WAREHOUSE, exist_ok=True)

catalog = load_catalog("demo", **{
    "type": "sql",
    "uri": f"sqlite:///{os.path.join(BASE_DIR, 'constraint.db')}",
    "warehouse": WAREHOUSE,
})

try:
    catalog.create_namespace("shop")
except Exception:
    pass

# ⭐ required=False 允许 NULL，required=True 元数据标记不允许NULL
schema = Schema(
    NestedField(1, "order_id", LongType(), required=True),   # 元数据标记：不能为 NULL
    NestedField(2, "customer_name", StringType(), required=False),
    NestedField(3, "amount", LongType(), required=False),
)

table_name = "shop.orders"
if catalog.table_exists(table_name):
    catalog.drop_table(table_name)

table = catalog.create_table(table_name, schema=schema)

# 关键：手动构造pyarrow schema，order_id 设置 nullable=False，和iceberg required=True对齐
arrow_schema = pa.schema([
    pa.field("order_id", pa.int64(), nullable=False),
    pa.field("customer_name", pa.string(), nullable=True),
    pa.field("amount", pa.int64(), nullable=True),
])

# 正常插入
df_ok = pa.table({
    "order_id": [1, 2],
    "customer_name": ["Alice", "Bob"],
    "amount": [100, 200],
}, schema=arrow_schema)

table.append(df_ok)
print("✅ 正常数据插入成功")

# ⭐ 注意：pyiceberg Python层只校验schema元数据，不校验业务数据内容。
# 下面我们故意构造非法数据：order_id传None，但是pyarrow这里必须nullable=False，所以无法构造。
# 👉 业务层面order_id为null的拦截，是Spark/Trino引擎的职责，pyiceberg做不到。
print("\n⚠️注意：pyiceberg不会校验业务数据NULL；required=True业务校验交给Spark/Trino。")

# ⭐ Schema 演化1：重命名列 customer_name → customer_id
print("\n📌 Schema 演化：customer_name → customer_id (只修改元数据，旧parquet文件不动，依靠field_id=2绑定)")
with table.update_schema() as update:
    update.rename_column("customer_name", "customer_id")
table = catalog.load_table(table_name)  # 重新加载元数据

print("📋 修改后 Schema (name / field_id / required):")
for f in table.schema().fields:
    print(f"  name={f.name}, id={f.field_id}, required={f.required}")

# ⭐ Schema演化2：新增列，历史旧数据自动填充null
print("\n📌 Schema 演化：新增列 pay_type")
with table.update_schema() as update:
    update.add_column("pay_type", StringType(), required=False)
table = catalog.load_table(table_name)

print("📋 新增列之后 Schema：")
for f in table.schema().fields:
    print(f"  name={f.name}, id={f.field_id}, required={f.required}")

# 查询（旧数据自动兼容，pay_type旧行全部为null）
print("\n📖 查询全部结果：")
df_result = table.scan().to_pandas()
print(df_result)

print("""
💡关键点总结：
1. Iceberg字段匹配依靠 field_id，不是列名字；重命名只改元数据显示名，历史parquet完全不用重写。
2. Iceberg required=True 对应的pyarrow字段必须 nullable=False，否则append直接schema不匹配报错；
   但是！这只是schema元数据对齐，**不是业务数据NULL校验，Spark/Trino才校验业务数据非空**。
3. add_column新增字段：旧历史数据自动补null，不需要重写历史文件，schema演化零成本。
4. 传统Hive改表结构经常要修复历史数据；Iceberg元数据解耦，历史文件不动。
""")
