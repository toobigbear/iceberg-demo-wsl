"""③ DWD → ADS：将临床与影像按患者 id 交集融合为特征宽表，供建模/大模型。
演示能力：多模态融合 + 时间旅行前的数据就绪态。"""
import os
import pyarrow as pa
import pandas as pd

import config
from ingest_ods import arrow_schema_from_df


# ========== 新增：患者主键候选列名（覆盖常见命名习惯）==========
ID_CANDIDATES = ["id", "ID", "patient_id", "患者id", "患者ID",
                 "pid", "case_id", "患者编号"]


def _find_id_column(df: pd.DataFrame, name: str) -> str:
    """从候选名单里找出 DataFrame 的患者主键列名；找不到就报错并打印实际列名。"""
    for col in ID_CANDIDATES:
        if col in df.columns:
            return col
    raise KeyError(
        f"[{name}] 未找到患者主键列，实际列名前 15 个: {list(df.columns)[:15]}"
    )


def build_ads(catalog=None) -> None:
    catalog = catalog or config.make_catalog()
    cli = catalog.load_table("sepsis.dwd_clinical").scan().to_pandas()
    rad = catalog.load_table("sepsis.dwd_radiomics").scan().to_pandas()

    # ========== ①：自动识别两表主键列 ==========
    cli_id = _find_id_column(cli, "dwd_clinical")
    rad_id = _find_id_column(rad, "dwd_radiomics")

    # ========== ②：统一重命名为 id 再合并 ==========
    cli = cli.rename(columns={cli_id: "id"})
    rad = rad.rename(columns={rad_id: "id"})
    print(f"[ads] 主键列: 临床='{cli_id}' → id, 影像='{rad_id}' → id")

    # ③：这时两边都叫 id，merge 不再报 KeyError
    fused = cli.merge(rad, on="id", how="inner", suffixes=("_cli", "_rad"))
    full = "sepsis.ads_fused_feature"
    if catalog.table_exists(full):
        catalog.drop_table(full)
    table = catalog.create_table(full, schema=arrow_schema_from_df(fused))
    table.append(pa.Table.from_pandas(fused, preserve_index=False))
    print(f"[ads] 融合宽表 {full}: {fused.shape[0]} 例 x {fused.shape[1]} 特征")
    print(f"[ads] 说明: 临床∩影像交集患者，可直接供建模与大模型解读")


if __name__ == "__main__":
    build_ads()