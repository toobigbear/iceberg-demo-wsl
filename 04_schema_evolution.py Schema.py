from pyiceberg.types import DoubleType
import pyarrow as pa
from common import get_catalog


def main():
    catalog = get_catalog()
    table_ident = "nyc.taxis_basic"
    table = catalog.load_table(table_ident)

    print("演进前schema字段列表：")
    for f in table.schema().fields:
        print(f"  {f.name}:{f.field_type}")

    # 添加新列 tip_amount
    with table.update_schema() as update:
        update.add_column("tip_amount", DoubleType(), required=False)

    # schema变更后重新load_table，规避pyiceberg内存缓存bug
    table = catalog.load_table(table_ident)

    print("\n演进后schema字段列表：")
    for f in table.schema().fields:
        print(f"  {f.name}:{f.field_type}")

    # 写入带新列的数据；老数据tip_amount自动为null
    new_row = pa.table({
        "vendor_id":[2],
        "trip_id":[3001],
        "trip_distance":[5.5],
        "fare_amount":[27.0],
        "store_and_fwd_flag":["N"],
        "tip_amount":[4.2]
    })
    table.append(new_row)

    df = table.scan().to_arrow().to_pandas()
    print("\n全部数据，老记录tip_amount为NULL：")
    print(df)
    print("✅ 04 schema演进完成；Trino可识别新增加字段")


if __name__ == "__main__":
    main()
