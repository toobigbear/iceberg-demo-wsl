"""
build_dwd.py — ODS → DWD（方案A：长表透视成宽表 + 白名单字段筛选）

DWD（明细层）：
- ods_clinical_metrics 是"长表"（一行=一个患者的一个指标 metric_name/metric_value），
  先透视成"宽表"（一行=一个患者，每个指标一列），再按白名单保留关键临床字段。
- ods_radiomics_features 只做"剔除全空列"清洗。

幂等：入湖前先 drop 旧 DWD 再重建，可安全反复运行。
"""

import os
import re
import pandas as pd
import pyarrow as pa
import config  # 复用全局 catalog 配置，保证与 ingest_ods.py 指向同一个 catalog

# ============ 0. 配置（统一走 config，不再各自硬编码路径） ============
NAMESPACE = "sepsis"
ODS_CLINICAL = f"{NAMESPACE}.ods_clinical_metrics"    # 长表
ODS_RADIOMICS = f"{NAMESPACE}.ods_radiomics_features" # 宽表
DWD_CLINICAL = f"{NAMESPACE}.dwd_clinical"            # 目标：宽表
DWD_RADIOMICS = f"{NAMESPACE}.dwd_radiomics"

# 临床关键字段白名单（宽表目标列，中文业务名）
CLINICAL_KEY_COLS = [
    "性别", "年龄", "收缩压", "舒张压", "脉搏", "心率", "肌钙蛋白", "NT-proBNP", "BNP",
    "CRP", "PCT", "乳酸", "乳酸脱氢酶", "肌红蛋白", "肌酸激酶", "AST", "ALT", "Cr",
    "SOFA评分", "PLT", "PaO2", "FiO2", "PaO2/FiO2", "平均动脉压", "GCS评分",
    "心超EF%", "左心室舒张末期容积", "左心室收缩末期容积", "E/A比值",
    "三尖瓣环收缩期位移", "左心房前后径",
]

# 显式别名：把 ODS 里不同的 metric_name 写法映射到白名单标准名（避免"肌钙蛋白 vs TNI"这类对不上）
ALIAS_MAP = {
    "TNI": "肌钙蛋白", "肌钙蛋白TNI": "肌钙蛋白",
    "LAC": "乳酸", "乳酸LAC": "乳酸",
    "SOFA": "SOFA评分", "SOFA评分评分": "SOFA评分",
    "EF": "心超EF%", "EF%": "心超EF%", "心超EF": "心超EF%",
    "PaO2FiO2": "PaO2/FiO2", "氧合指数": "PaO2/FiO2",
    "GCS": "GCS评分", "TAPSE": "三尖瓣环收缩期位移",
    "NTproBNP": "NT-proBNP", "NT-proBNP": "NT-proBNP",
}


def get_catalog():
    # ⭐ 关键修复：与 ingest_ods.py 用同一个 catalog（config.make_catalog()）
    return config.make_catalog()


def _coerce_to_str(series: pd.Series) -> pd.Series:
    """去掉数值列的 .0 尾巴，NaN 转 None，其余转字符串（防 ArrowTypeError）。"""
    return series.map(lambda v: None if pd.isna(v)
                      else (str(int(v)) if isinstance(v, float) and v.is_integer()
                            else str(v)))


def _df_to_arrow(df: pd.DataFrame) -> pa.Table:
    """pandas -> pyarrow，object 列统一转 string，规避 string 列混入 float 的报错。"""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = _coerce_to_str(df[col])
    return pa.Table.from_pandas(df, preserve_index=False)


def _norm(s: str) -> str:
    """标准化：去空白、去括号内单位/参考值、去标点、统一小写。"""
    s = re.sub(r"[（(].*?[)）]", "", str(s))          # 去掉 括号内单位/参考值
    s = re.sub(r"[\s·×*/%°、,，:：\-]", "", s)          # 去空白与常见符号
    return s.lower()


def _match_whitelist(metric_name: str, key_cols: list) -> str | None:
    """把一个 metric_name 匹配到白名单标准名（先别名表，再模糊包含）。"""
    raw = str(metric_name).strip()
    if raw in ALIAS_MAP:
        return ALIAS_MAP[raw]
    n = _norm(raw)
    for k in key_cols:
        if n and (_norm(k) in n or n in _norm(k)):
            return k
    return None


def _read_ods(name: str) -> pd.DataFrame:
    catalog = get_catalog()
    tbl = catalog.load_table(name)
    return tbl.scan().to_pandas()


def _clean_clinical(df: pd.DataFrame):
    """
    长表(patient_id/metric_name/metric_value/is_flag/recorded_at) -> 宽表。
    返回 (宽表DataFrame, 质量报告dict)。
    """
    # 0) 缺失率报告（遍历 ODS 原始列，仅信息输出）
    report = {"missing_pct": df.isna().mean().round(4).to_dict(),
              "n_rows": len(df)}

    # 1) patient_id 标准化（去掉 .0）
    df = df.copy()
    df["patient_id"] = _coerce_to_str(df["patient_id"]).fillna("").astype(str)
    df = df[df["patient_id"] != ""].copy()

    # 2) 长表 -> 宽表：每个 patient 一行，每个指标一列（取 first，值已清洗）
    wide = df.pivot_table(
        index="patient_id",
        columns="metric_name",
        values="metric_value",
        aggfunc="first",
    ).reset_index()
    wide.columns = [str(c) for c in wide.columns]
    report["wide_n_rows"] = len(wide)

    # 3) recorded_at：每个患者取最早一条时间（不转数值）
    rec = (df.dropna(subset=["recorded_at"])
             .groupby("patient_id")["recorded_at"].first())
    wide["recorded_at"] = wide["patient_id"].map(rec)

    # 4) 白名单筛选：匹配到的指标列 + patient_id + recorded_at
    keep = ["patient_id"]
    matched = {}
    for col in wide.columns:
        if col in ("patient_id", "recorded_at"):
            continue
        std = _match_whitelist(col, CLINICAL_KEY_COLS)
        if std:
            matched[std] = col
    # 统一成白名单标准名
    rename = {orig: std for std, orig in matched.items()}
    out = wide[keep + list(rename)].rename(columns=rename)
    out["recorded_at"] = wide["recorded_at"]

    # 5) 数值列类型清洗：只对指标列强转数值，patient_id/recorded_at 除外
    for col in out.columns:
        if col not in ("patient_id", "recorded_at"):
            out[col] = pd.to_numeric(out[col], errors="coerce")

    report["matched_fields"] = list(matched.keys())
    return out, report

# ========== 新增：影像表真实患者主键候选列（比 ADS 名单更宽）==========
RAD_ID_CANDIDATES = [
    "patient_id", "患者id", "患者ID", "患者编号", "病例号", "住院号",
    "病历号", "ID", "pid", "case_id", "编号", "id",
]


def _find_rad_patient_id(df: pd.DataFrame) -> str:
    """从候选名单找出影像表的真实患者主键列；找不到就报错并打印前25列名。"""
    for col in RAD_ID_CANDIDATES:
        if col in df.columns:
            return col
    raise KeyError(
        "[dwd_radiomics] 未找到影像患者主键列，实际列名前25个: "
        + str(list(df.columns)[:25])
    )

def _clean_radiomics(df: pd.DataFrame):
    """
    影像 ODS 是长表：(id, patient_id, roi, feature_name, feature_value)
    需透视成宽表：一行=一个患者，每列=一个特征，值=feature_value。
    同一患者多个 ROI 段的同名特征取均值（等价原 radiomics_prep 的聚合口径）。
    """
    # 1) 找患者主键列
    pid_col = "patient_id"
    if pid_col not in df.columns:
        for c in ["patient_id", "患者ID", "患者id", "ID", "id"]:
            if c in df.columns:
                pid_col = c
                break
        else:
            raise KeyError("[dwd_radiomics] 找不到患者主键列，实际列名: "
                           + str(list(df.columns)))

    # 2) 长表必需列校验（缺了就直接报错，别把长表当宽表吞掉）
    for need in ["feature_name", "feature_value"]:
        if need not in df.columns:
            raise KeyError(f"[dwd_radiomics] 长表缺少 '{need}' 列，实际列名: "
                           + str(list(df.columns)))

    sub = df[[pid_col, "feature_name", "feature_value"]].copy()

    # 3) 患者ID标准化（与临床 patient_id 同一口径）
    sub[pid_col] = _coerce_to_str(sub[pid_col]).fillna("").astype(str)
    sub = sub[sub[pid_col] != ""]

    # 4) 透视：行=患者，列=feature_name，值=feature_value（同名取均值）
    wide = sub.pivot_table(index=pid_col, columns="feature_name",
                           values="feature_value", aggfunc="mean")
    wide = wide.reset_index().rename(columns={pid_col: "patient_id"})

    # 5) 剔除全空特征列
    feat_cols = [c for c in wide.columns if c != "patient_id"]
    feat_cols = [c for c in feat_cols if wide[c].notna().any()]
    wide = wide[["patient_id"] + feat_cols]

    # report 键名：把调用方 build_dwd() 可能引用的命名变体全部补上，避免 KeyError
    report = {
        # 列数相关
        "total_cols": int(df.shape[1]),  # 原始列数
        "n_cols": int(df.shape[1]),
        "kept_cols": int(wide.shape[1]),  # 最终保留列数(含patient_id)
        # 行数/患者/特征
        "total_rows": int(len(df)),  # 原始长表行数
        "n_rows": int(len(df)),
        "n_patients": int(len(wide)),  # 透视后患者数
        "n_features": int(len(feat_cols)),  # 特征数(不含patient_id)
        "matched_features": int(len(feat_cols)),
    }
    return wide, report


def build_dwd():
    catalog = get_catalog()
    for ns in (NAMESPACE,):
        if not catalog.namespace_exists(ns):
            catalog.create_namespace(ns)

    # ---- clinical ----
    print(f"[dwd] 读取 {ODS_CLINICAL} ...")
    ods_clin = _read_ods(ODS_CLINICAL)
    clin, report_c = _clean_clinical(ods_clin)
    print(f"[dwd] clinical 长表 {len(ods_clin)} 行 -> 宽表 {clin.shape}")
    print(f"[dwd] clinical 质量报告: 缺失率={report_c['missing_pct']} "
          f"匹配字段={report_c['matched_fields']}")

    if catalog.table_exists(DWD_CLINICAL):
        catalog.drop_table(DWD_CLINICAL)
    tbl = catalog.create_table(DWD_CLINICAL, schema=_df_to_arrow(clin).schema)
    tbl.append(_df_to_arrow(clin))
    print(f"[dwd] 写入 {DWD_CLINICAL}: {clin.shape}")

    # ---- radiomics ----
    print(f"[dwd] 读取 {ODS_RADIOMICS} ...")
    ods_rad = _read_ods(ODS_RADIOMICS)
    rad, report_r = _clean_radiomics(ods_rad)
    print(f"[dwd] radiomics 清洗: 原始{report_r['total_cols']}列 "
          f"-> 保留{report_r['kept_cols']}列, "
          f"{report_r['n_rows']}行 -> 聚合{report_r['n_patients']}例")

    if catalog.table_exists(DWD_RADIOMICS):
        catalog.drop_table(DWD_RADIOMICS)
    tbl2 = catalog.create_table(DWD_RADIOMICS, schema=_df_to_arrow(rad).schema)
    tbl2.append(_df_to_arrow(rad))
    print(f"[dwd] 写入 {DWD_RADIOMICS}: {rad.shape}")

    print("[dwd] 完成 ✅")


if __name__ == "__main__":
    build_dwd()