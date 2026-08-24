import pandas as pd
import pyarrow as pa
from common import get_catalog


def main():
    catalog = get_catalog()
    table = catalog.load_table("nyc.taxis_basic")

    print("=== 当前全部快照列表 ===")
    all_snapshots = table.snapshots()
    for snap in all_snapshots:
        op = snap.summary.get("operation", "unknown")
        print(f"snapshot_id={snap.snapshot_id}, op={op}, ts={snap.timestamp_ms}")

    new_data = pd.DataFrame({
        "vendor_id": [2],
        "trip_id": [1002],
        "trip_distance": [5.2],
        "fare_amount": [30.0],
        "store_and_fwd_flag": ["N"]
    })
    arrow_df = pa.Table.from_pandas(new_data)
    table.append(arrow_df)
    print("✅ 追加数据完成，生成新快照")

    # 取第一个快照，时间旅行查询：scan传入snapshot_id
    first_snapshot = all_snapshots[0]
    scan_old = table.scan(snapshot_id=first_snapshot.snapshot_id)
    df_old = scan_old.to_arrow().to_pandas()
    print(f"\n⏳ 时间旅行读取快照 {first_snapshot.snapshot_id}:")
    print(df_old)

    # 查询当前最新版本
    scan_latest = table.scan()
    df_latest = scan_latest.to_arrow().to_pandas()
    print("\n📦 当前最新快照数据：")
    print(df_latest)


if __name__ == "__main__":
    main()
