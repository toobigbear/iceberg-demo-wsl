"""⑤ Schema 演进：后期新增"28天结局标签"列，旧数据自动补 NULL，无需重写 Parquet。"""
import pyarrow as pa
import pandas as pd
from pyiceberg.types import LongType, StringType

import config
from ingest_ods import arrow_schema_from_df


class SchemaEvolveDemo:
    def __init__(self, catalog=None):
        self.catalog = catalog or config.make_catalog()
        self.full = "sepsis.demo_schema_evolve"

    def run(self):
        if self.catalog.table_exists(self.full):
            self.catalog.drop_table(self.full)
        # 初始 schema：只有 id / bnp
        df1 = pd.DataFrame({"id": ["1", "2"], "bnp": [300.0, 120.0]})
        t = self.catalog.create_table(self.full, schema=arrow_schema_from_df(df1))
        t.append(pa.Table.from_pandas(df1, preserve_index=False))
        print("[se] 初始表，字段:", [f.name for f in t.schema().fields])

        # ---- Schema 演进：新增 surviv_28 标签列 ----
        print("[se] 📌 Schema 演进：新增 surviv_28 列...")
        with t.update_schema() as update:
            update.add_column("surviv_28", LongType())
        t = self.catalog.load_table(self.full)
        print("[se] 演进后字段:", [(f.name, f.field_type) for f in t.schema().fields])

        # 写入带新列的增量数据
        df2 = pd.DataFrame({"id": ["3"], "bnp": [50.0], "surviv_28": [1]})
        t.append(pa.Table.from_pandas(df2, preserve_index=False))
        print("[se] 全量查询（旧行 surviv_28 自动补 NULL）:")
        print(t.scan().to_pandas())
        print("[se] 结论: 加列不改历史文件，Schema 演进零成本")


def main():
    SchemaEvolveDemo().run()


if __name__ == "__main__":
    main()
