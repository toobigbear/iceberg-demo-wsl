"""① 原始入湖：将 MySQL/CSV 源数据以全量追加方式写入 Iceberg ODS 层。
演示能力：ACID 原子写入 + 追加快照。写入失败不影响既有快照。"""
import os
import pyarrow as pa
import pandas as pd
import numpy as np
import config
import data_reader


def ensure_namespace(catalog, ns: str = "sepsis") -> None:
    from pyiceberg.exceptions import NamespaceAlreadyExistsError
    try:
        catalog.create_namespace(ns)
        print(f"[ods] 创建命名空间 {ns}")
    except NamespaceAlreadyExistsError:
        pass


def arrow_schema_from_df(df: pd.DataFrame) -> pa.Schema:
    """把 pandas 列转为 pyarrow schema，全部 nullable=True 避免 required 报错"""
    fields = []
    for col, dt in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dt):
            t = pa.int64()
        elif pd.api.types.is_float_dtype(dt):
            t = pa.float64()
        elif pd.api.types.is_bool_dtype(dt):
            t = pa.bool_()
        elif pd.api.types.is_datetime64_any_dtype(dt):
            t = pa.timestamp("us")
        else:
            t = pa.string()
        fields.append(pa.field(str(col), t, nullable=True))
    return pa.schema(fields)


def ingest_ods(catalog=None) -> None:
    catalog = catalog or config.make_catalog()
    ensure_namespace(catalog, "sepsis")

    for ods_table in config.SOURCE_TABLES:
        df = data_reader.load_source(ods_table)
        if df.empty:
            print(f"[ods] 跳过 {ods_table}（空）")
            continue
        #每次都是"先删旧表→重建→写入"
        full_name = f"sepsis.{ods_table}"
        if catalog.table_exists(full_name):
            catalog.drop_table(full_name)

        # 用转换好的 Arrow Table 建表，保证 schema 一致
        arrow_tbl = df_to_arrow(df, arrow_schema_from_df(df))
        table = catalog.create_table(full_name, schema=arrow_tbl.schema)
        table.append(arrow_tbl)
        print(f"[ods] 入湖完成 {full_name}: {df.shape[0]} 行 x {df.shape[1]} 列")

    print("[ods] ODS 原始层入湖结束")

import math

def coerce_to_str(v):
    """把任意值安全转成字符串或 None：兼容 float/bool/NaN，去掉整数的 .0 尾巴。"""
    if v is None:
        return None
    if isinstance(v, float) and (pd.isna(v) or math.isnan(v)):
        return None
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, float) and v.is_integer():
        return str(int(v))          # 123.0 -> "123"
    return str(v)


def df_to_arrow(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """把 DataFrame 转成与 schema 一致的 pyarrow Table，兼容 float/NaN/object 混列。"""
    arrays = []
    for field in schema:
        col = df[field.name]
        col = col.where(pd.notna(col), None)          # NaN/NaT 统一转 None
        vals = col.astype(object).tolist()

        if field.type == pa.string():
            arr = pa.array([coerce_to_str(v) for v in vals], type=pa.string())
        elif field.type == pa.int64():
            # int 列若出现 float(如 1.0) 也规整掉，None 保留为空
            arr = pa.array([None if v is None else pd.to_numeric(v, errors="raise") for v in vals],
                           type=pa.int64(), from_pandas=True)
        elif field.type == pa.float64():
            arr = pa.array(vals, type=pa.float64(), from_pandas=True)
        elif field.type == pa.bool_():
            arr = pa.array(vals, type=pa.bool_(), from_pandas=True)
        elif pa.types.is_timestamp(field.type):
            arr = pa.array(vals, type=field.type, from_pandas=True)
        else:
            arr = pa.array(vals, from_pandas=True)
        arrays.append(arr)

    return pa.Table.from_arrays(arrays, schema=schema)



if __name__ == "__main__":
    ingest_ods()