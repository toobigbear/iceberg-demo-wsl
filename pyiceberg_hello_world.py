"""
PyIceberg 入门 Demo（纯 Python，无需 Spark）
===========================================
PyIceberg 是 Iceberg 官方 Python 原生库，直接操作 Iceberg 表。
"""
import os
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, DoubleType, StringType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pandas as pd
import pyarrow as pa


def main():
    print("=" * 60)
    print("🧊 PyIceberg 入门 Demo（纯 Python）")
    print("=" * 60)
    # ============================================================
    # Step 1: 创建本地 Catalog（用 SQLite 存储元数据）【适配你的环境】
    # ============================================================
    print("\n📌 Step 1: 创建本地 SQL Catalog ...")
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    warehouse_dir = os.path.join(BASE_DIR, "pyiceberg_warehouse")
    os.makedirs(warehouse_dir, exist_ok=True)

    # 和你demo.py保持统一，使用load_catalog
    catalog = load_catalog(
        "demo_catalog",
        **{
            "type": "sql",
            "uri": f"sqlite:///{os.path.join(BASE_DIR, 'catalog.db')}",
            "warehouse": warehouse_dir,
        }
    )
    print(f"✅ Catalog 创建成功，数据目录: {warehouse_dir}")

    # 创建命名空间 nyc
    try:
        catalog.create_namespace("nyc")
        print("✅ namespace nyc 创建成功")
    except NamespaceAlreadyExistsError:
        print("ℹ️ namespace nyc 已存在，跳过")

    table_identifier = "nyc.taxis"
    # 重复运行时清理旧表
    if catalog.table_exists(table_identifier):
        catalog.drop_table(table_identifier)
        print("ℹ️ 旧表 nyc.taxis 已删除")

    # ============================================================
    # Step 2: 定义表 Schema（修复：trip_distance 使用 DoubleType）
    # ============================================================
    print("\n📌 Step 2: 定义表 Schema ...")
    schema = Schema(
        NestedField(1, "vendor_id", LongType(), required=False),
        NestedField(2, "trip_id", LongType(), required=False),
        NestedField(3, "trip_distance", DoubleType(), required=False),
        NestedField(4, "fare_amount", DoubleType(), required=False),
        NestedField(5, "store_and_fwd_flag", StringType(), required=False),
    )
    print("✅ Schema 定义完成")

    # ============================================================
    # Step 3: 创建表
    # ============================================================
    print("\n📌 Step 3: 创建 Iceberg 表 ...")
    # 定义分区（这里按 vendor_id 分区，演示用）
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=IdentityTransform(),
            name="vendor_id"
        )
    )
    # 创建表
    table = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        partition_spec=partition_spec,
    )
    print("✅ 表 nyc.taxis 创建成功")

    # ============================================================
    # Step 4: 插入第一批数据（用 PyArrow）
    # ============================================================
    print("\n📌 Step 4: 插入第一批数据 ...")
    df1 = pa.table({
        "vendor_id": [1, 2, 1, 2],
        "trip_id": [1001, 1002, 1003, 1004],
        "trip_distance": [2.5, 5.0, 1.0, 10.0],
        "fare_amount": [12.5, 25.0, 8.0, 45.0],
        "store_and_fwd_flag": ["N", "Y", "N", "Y"],
    })
    table.append(df1)
    print("✅ 第一批数据插入成功（4条）")

    # ============================================================
    # Step 5: 查询数据
    # ============================================================
    print("\n📌 Step 5: 查询所有数据 ...")
    result = table.scan().to_arrow().to_pandas()
    print(result.to_string())
    print(f"📊 共 {len(result)} 条记录")

    # ============================================================
    # Step 6: 聚合查询
    # ============================================================
    print("\n📌 Step 6: 按 vendor 聚合统计 ...")
    agg = result.groupby("vendor_id").agg({
        "trip_id": "count",
        "fare_amount": ["sum", "mean"]
    }).reset_index()
    print(agg.to_string())

    # ============================================================
    # Step 7: ⭐ 查看表快照历史（时间旅行）
    # ============================================================
    print("\n📌 Step 7: ⭐ 查看表快照历史 ...")
    snapshots = table.snapshots()
    print(f"📸 当前共有 {len(snapshots)} 个快照")
    for snap in snapshots:
        print(f"   快照 ID: {snap.snapshot_id}, 操作: {snap.summary['operation']}, 时间: {snap.timestamp_ms}")
    first_snapshot_id = snapshots[0].snapshot_id
    print(f"📝 第一个快照 ID: {first_snapshot_id}")

    # ============================================================
    # Step 8: 插入第二批数据，产生新快照
    # ============================================================
    print("\n📌 Step 8: 插入第二批数据 ...")
    df2 = pa.table({
        "vendor_id": [1, 3],
        "trip_id": [1005, 1006],
        "trip_distance": [3.0, 7.5],
        "fare_amount": [15.0, 35.0],
        "store_and_fwd_flag": ["N", "Y"],
    })
    table.append(df2)
    print("✅ 第二批数据插入成功（2条）")

    # ============================================================
    # Step 9: 再次查看历史
    # ============================================================
    print("\n📌 Step 9: 再次查看表历史（现在有两个快照）...")
    snapshots = table.snapshots()
    print(f"📸 当前共有 {len(snapshots)} 个快照")
    for snap in snapshots:
        print(f"   快照 ID: {snap.snapshot_id}, 操作: {snap.summary['operation']}")

    # ============================================================
    # Step 10: ⭐ 时间旅行 - 查询历史版本
    # ============================================================
    print(f"\n📌 Step 10: ⭐ 时间旅行 - 查询第一个快照（只有4条）...")
    historical = table.scan(snapshot_id=first_snapshot_id).to_arrow().to_pandas()
    print(historical.to_string())
    print(f"📊 历史版本共 {len(historical)} 条记录")

    print("\n📌 当前最新数据（6条）...")
    current = table.scan().to_arrow().to_pandas()
    print(current.to_string())
    print(f"📊 最新版本共 {len(current)} 条记录")

    # ============================================================
    # Step 11: ⭐ Schema 演进 - 添加新列【修复bug：变更schema后重新load_table】
    # ============================================================
    print("\n📌 Step 11: ⭐ Schema 演进 - 添加 tip_amount 列 ...")
    with table.update_schema() as update:
        update.add_column("tip_amount", DoubleType())
    print("✅ 新列 tip_amount 添加成功")

    # 🔥关键修复：schema演进之后，丢弃旧table对象，重新加载，规避pyiceberg内存状态bug
    table = catalog.load_table(table_identifier)

    # 查看新 Schema
    print("\n📌 新 Schema:")
    for field in table.schema().fields:
        print(f"   {field.field_id}: {field.name} ({field.field_type})")

    # 插入带新列的数据
    print("\n📌 插入带 tip_amount 的数据 ...")
    df3 = pa.table({
        "vendor_id": [1, 2],
        "trip_id": [1007, 1008],
        "trip_distance": [4.0, 6.0],
        "fare_amount": [20.0, 30.0],
        "store_and_fwd_flag": ["N", "Y"],
        "tip_amount": [3.5, 5.0],
    })
    table.append(df3)
    print("✅ 数据插入成功")

    print("\n📌 查看全部数据（老数据 tip_amount 为 NULL）...")
    final = table.scan().to_arrow().to_pandas()
    print(final.to_string())

    # ============================================================
    # Step 12: 查看底层文件结构
    # ============================================================
    print("\n📌 Step 12: 查看底层文件 ...")
    print(f"\n📁 数据目录结构 ({warehouse_dir}):")
    for root, dirs, files in os.walk(warehouse_dir):
        level = root.replace(warehouse_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f'{indent}{os.path.basename(root)}/')
        subindent = ' ' * 2 * (level + 1)
        for file in files[:10]:  # 最多显示10个文件
            print(f'{subindent}{file}')
        if len(files) > 10:
            print(f'{subindent}... 还有 {len(files) - 10} 个文件')

    # ============================================================
    # 完成
    # ============================================================
    print("\n" + "=" * 60)
    print("🎉 PyIceberg 入门 Demo 完成！")
    print("=" * 60)
    print("""
    核心概念回顾：
    • Catalog:  表的"注册中心"，这里用 SQLite 存储
    • Namespace: 相当于数据库schema，nyc 类比数据库名
    • Schema:   表结构定义，支持演进（add column）
    • Snapshot: 每次写入产生一个快照，支持时间旅行读历史版本
    • PartitionSpec: 分区，本案例按 vendor_id 分区
    • Manifest: 元数据文件，记录数据文件信息
    底层文件在 ./pyiceberg_warehouse/ 目录下：
    - nyc/taxis/data/      → Parquet 数据文件（按分区目录分开）
    - nyc/taxis/metadata/  → Iceberg元数据：metadata.json / manifest / manifest‑list
    """)


if __name__ == "__main__":
    main()
