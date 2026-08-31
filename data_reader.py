"""数据读取层：优先从 MySQL 业务库读取，连不上则回退读取清洗后的 CSV。
所有函数统一返回 pandas.DataFrame，主键列统一为 'id'。"""
import glob
import os
import pandas as pd

import config

'''
def _read_from_mysql(table_name: str) -> pd.DataFrame:
    import pymysql
    from sqlalchemy import create_engine  # 可选，若未装则用 pymysql 直连

    conn = pymysql.connect(**config.MYSQL, charset="utf8mb4")
    try:
        return pd.read_sql(f"SELECT * FROM `{table_name}`", conn)
    finally:
        conn.close()
'''
def _read_from_mysql(table_name: str) -> pd.DataFrame:
    from sqlalchemy import create_engine

    url = (
        f"mysql+pymysql://{config.MYSQL['user']}:{config.MYSQL['password']}"
        f"@{config.MYSQL['host']}:{config.MYSQL['port']}"
        f"/{config.MYSQL['database']}?charset=utf8mb4"
    )
    engine = create_engine(url)
    try:
        return pd.read_sql(f"SELECT * FROM `{table_name}`", engine)
    finally:
        engine.dispose()

def _read_from_csv(pattern: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(config.CSV_FALLBACK, pattern)))
    if not files:
        raise FileNotFoundError(f"在 {config.CSV_FALLBACK} 未找到 {pattern}")
    # 多个 clinical_cleaned_*.csv 合并成一个 DataFrame（横向拼接不同页签清洗结果）
    frames = [pd.read_csv(f, encoding="utf-8-sig", low_memory=False) for f in files]
    # 按 'id' 纵向去重合并（若存在 id 列）
    combined = pd.concat(frames, ignore_index=True)
    if "id" in combined.columns:
        combined = combined.drop_duplicates(subset=["id"], keep="first")
    return combined


def load_source(ods_table: str) -> pd.DataFrame:
    """加载某源表数据：先试 MySQL，失败回退 CSV。"""
    mysql_table, csv_pattern, id_col = config.SOURCE_TABLES[ods_table]
    # 尝试 MySQL
    try:
        df = _read_from_mysql(mysql_table)
        print(f"[reader] 从 MySQL 读取 {mysql_table}: {df.shape}")
        if df.empty:
            raise ValueError("空表")
        return _normalize_id(df, id_col)
    except Exception as e:
        print(f"[reader] MySQL 不可用（{type(e).__name__}: {e}），回退 CSV {csv_pattern}")
        df = _read_from_csv(csv_pattern)
        print(f"[reader] 从 CSV 读取: {df.shape}")
        return _normalize_id(df, id_col)


def _normalize_id(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """统一主键为 'id'，转字符串去掉 .0 后缀（与临床清洗口径一致）"""
    if id_col in df.columns and id_col != "id":
        df = df.rename(columns={id_col: "id"})
    if "id" in df.columns:
        df["id"] = df["id"].astype(str).str.replace(r"\.0$", "", regex=True)
    return df
