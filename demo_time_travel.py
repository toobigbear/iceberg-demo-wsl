"""④ 快照 & 时间旅行：演示科研数据可复现、可回滚。
步骤：写入 v1 → 模拟误改写入 v2（覆盖） → 用历史快照回滚到 v1。"""
import pyarrow as pa
import pandas as pd

import config
from ingest_ods import arrow_schema_from_df


class TimeTravelDemo:
    def __init__(self, catalog=None):
        self.catalog = catalog or config.make_catalog()
        self.full = "sepsis.demo_time_travel"

    def _reset(self):
        if self.catalog.table_exists(self.full):
            self.catalog.drop_table(self.full)

    def run(self):
        self._reset()
        # ---- 快照 v1：初始正确数据 ----
        df1 = pd.DataFrame({"id": ["1", "2", "3"], "age": [65, 72, 58], "surv28": [1, 0, 1]})
        t = self.catalog.create_table(self.full, schema=arrow_schema_from_df(df1))
        t.append(pa.Table.from_pandas(df1, preserve_index=False))
        v1_snapshot = t.metadata.current_snapshot_id
        print(f"[tt] v1 写入完成，快照={v1_snapshot}")
        print("[tt] v1 内容:"); print(t.scan().to_pandas())

        # ---- 快照 v2：模拟"误改"——把 age 全置 0 ----
        df2 = df1.copy(); df2["age"] = 0
        t.append(pa.Table.from_pandas(df2, preserve_index=False))  # 追加第二份
        print(f"[tt] v2 追加入湖（模拟误改），当前快照={t.metadata.current_snapshot_id}")
        print("[tt] 当前（被污染）内容:"); print(t.scan().to_pandas())

        # ---- 时间旅行：回到 v1 历史快照 ----
        print("[tt] ⏳ 时间旅行回滚到 v1 快照...")
        hist = t.scan(snapshot_id=v1_snapshot).to_pandas()
        print("[tt] 历史快照内容（数据可复现）:"); print(hist)
        print("[tt] 结论: 写错数据不靠备份，快照秒级回滚，科研可复现")


def main():
    TimeTravelDemo().run()


if __name__ == "__main__":
    main()
