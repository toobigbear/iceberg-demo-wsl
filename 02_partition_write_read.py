from pyiceberg.schema import Schema
from pyiceberg.types import LongType, DoubleType, StringType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa
from common import get_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError


def main():
    catalog = get_catalog()
    ns = "nyc"
    table_name = "taxis_partitioned"
    table_ident = f"{ns}.{table_name}"

    try:
        catalog.create_namespace(ns)
    except NamespaceAlreadyExistsError:
        pass

    if catalog.table_exists(table_ident):
        catalog.drop_table(table_ident)

    schema = Schema(
        NestedField(1, "vendor_id", LongType(), required=False),
        NestedField(2, "trip_id", LongType(), required=False),
        NestedField(3, "trip_distance", DoubleType(), required=False),
        NestedField(4, "fare_amount", DoubleType(), required=False),
        NestedField(5, "store_and_fwd_flag", StringType(), required=False),
    )

    # 按vendor_id身份分区
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="vendor_id")
    )

    table = catalog.create_table(table_ident, schema=schema, partition_spec=partition_spec)

    data = pa.table({
        "vendor_id": [1, 2, 1, 2, 3],
        "trip_id": [1001, 1002, 1003, 1004, 1005],
        "trip_distance": [2.5, 5.0, 1.0, 10.0,7.2],
        "fare_amount": [12.5, 25.0, 8.0, 45.0,32],
        "store_and_fwd_flag": ["N", "Y", "N", "Y","N"],
    })
    table.append(data)

    # 分区过滤扫描：只扫描vendor_id=1分区的数据（分区裁剪）
    scan_df = table.scan(row_filter="vendor_id = 1").to_arrow().to_pandas()
    print("分区裁剪查询 vendor_id=1：")
    print(scan_df)
    print(f"\n分区表物理位置：{table.location()}")
    print("✅ 02 分区表完成；Trino执行where vendor_id=1会自动裁剪分区")


if __name__ == "__main__":
    main()
