from pyiceberg.schema import Schema
from pyiceberg.types import LongType, DoubleType, StringType, NestedField
import pyarrow as pa
from common import get_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError


def main():
    catalog = get_catalog()
    ns = "nyc"
    table_name = "taxis_basic"
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

    table = catalog.create_table(table_ident, schema=schema)

    arrow_data = pa.table({
        "vendor_id": [1, 2, 1, 2],
        "trip_id": [1001, 1002, 1003, 1004],
        "trip_distance": [2.5, 5.0, 1.0, 10.0],
        "fare_amount": [12.5, 25.0, 8.0, 45.0],
        "store_and_fwd_flag": ["N", "Y", "N", "Y"],
    })
    table.append(arrow_data)

    df = table.scan().to_arrow().to_pandas()
    print("基础表查询结果：")
    print(df)
    print(f"\n表物理路径：{table.location()}")
    print("✅ 01 基础建表完成，该表可被Trino读取")


if __name__ == "__main__":
    main()
