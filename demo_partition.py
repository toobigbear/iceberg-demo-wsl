"""⑥ 隐藏分区：按影像采集年月对表做隐藏分区，查询只扫目标分区。
分区字段不进入 schema，用户无感知，引擎自动裁剪。"""
import os
import pyarrow as pa
import pandas as pd
from datetime import datetime
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform

import config

def run():
    catalog = config.make_catalog()
    try:
        catalog.create_namespace("sepsis")
    except Exception:
        pass
    full = "sepsis.demo_partition"
    if catalog.table_exists(full):
        catalog.drop_table(full)

    schema = Schema(
        NestedField(1, "patient_id", StringType(), required=True),
        NestedField(2, "scan_time", TimestampType()),
        NestedField(3, "ef", LongType()),
    )
    # 按 scan_time 的“月”隐藏分区
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="scan_month")
    )
    t = catalog.create_table(full, schema=schema, partition_spec=spec)

    # 关键修复：手动指定 pyarrow schema，patient_id 对齐 nullable=False
    arrow_schema = pa.schema([
        pa.field("patient_id", pa.string(), nullable=False),
        pa.field("scan_time", pa.timestamp("us"), nullable=True),
        pa.field("ef", pa.int64(), nullable=True),
    ])

    for m in ["2026-01-10 08:00:00", "2026-01-20 09:00:00", "2026-02-15 10:00:00"]:
        ts = datetime.strptime(m, "%Y-%m-%d %H:%M:%S")
        df = pd.DataFrame({"patient_id": [m[:7]], "scan_time": [ts], "ef": [55]})
        t.append(pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False))
    print("[pt] 隐藏分区表写入完成，schema 中无 scan_month 列:",
          [f.name for f in t.schema().fields])
    print("[pt] 磁盘分区目录:")
    wpath = os.path.join(config.WAREHOUSE_DIR, "sepsis", "demo_partition", "data")
    for root, dirs, _ in os.walk(wpath):
        for d in dirs:
            print("   ", os.path.join(os.path.relpath(root, wpath), d))
    print("[pt] 结论: 用户写 scan_time 即可，引擎按 scan_month 自动裁剪分区")

def main():
    run()


if __name__ == "__main__":
    main()
