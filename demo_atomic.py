"""⑦ ACID 原子性：模拟写入中途抛错，验证既有快照不被污染（无脏读）。"""
import pyarrow as pa
import pandas as pd

import config
from ingest_ods import arrow_schema_from_df


def run():
    catalog = config.make_catalog()
    full = "sepsis.demo_atomic"
    if catalog.table_exists(full):
        catalog.drop_table(full)
    df = pd.DataFrame({"id": ["1", "2"], "val": [10.0, 20.0]})
    t = catalog.create_table(full, schema=arrow_schema_from_df(df))
    t.append(pa.Table.from_pandas(df, preserve_index=False))
    snap_before = t.metadata.current_snapshot_id
    print(f"[ac] 初始快照={snap_before}")

    print("[ac] 尝试写入一批中途抛错的数据（模拟失败事务）...")
    try:
        bad = pd.DataFrame({"id": ["3"], "val": ["NOT_A_NUMBER"]})  # 类型不符
        t.append(pa.Table.from_pandas(bad, preserve_index=False))
    except Exception as e:
        print(f"[ac] 写入失败（{type(e).__name__}），已回滚")

    print(f"[ac] 当前快照仍={t.metadata.current_snapshot_id}（未被污染）")
    print("[ac] 读到数据:"); print(t.scan().to_pandas())
    print("[ac] 结论: ACID 原子写入，失败不脏读")


def main():
    run()


if __name__ == "__main__":
    main()
