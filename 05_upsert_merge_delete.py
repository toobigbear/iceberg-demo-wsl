import pyarrow as pa
from pyiceberg.expressions import EqualTo
from common import get_catalog


def main():
    catalog = get_catalog()
    table_ident = "nyc.taxis_basic"
    table = catalog.load_table(table_ident)

    print("删除前数据：")
    print(table.scan().to_arrow().to_pandas())

    # 删除 trip_id=1001 的行；元数据标记删除，不修改原始parquet文件
    table.delete(EqualTo("trip_id", 1001))
    table = catalog.load_table(table_ident)

    print("\n删除 trip_id=1001 后：")
    print(table.scan().to_arrow().to_pandas())
    print("✅05 行级删除完成；Iceberg是元数据删除，不直接改写原parquet")
    print("⚠️注意：pyiceberg 无原生upsert/merge接口，upsert需要自行实现读‑合并‑overwrite")


if __name__ == "__main__":
    main()
