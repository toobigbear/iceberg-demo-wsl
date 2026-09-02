#!/usr/bin/env python3
"""
数据契约（Data Contract）Hello World —— 修复版
===============================================
面向 HIS/EMR 质控域的数据接入契约：
1. 字段级约束（类型、非空、值域、枚举、正则、唯一性）
2. 写入前拦截脏数据，隔离到 quarantine 表
3. 契约版本演进（v1 -> v2），联动 Iceberg Schema Evolution

修复内容：
- 修复质控报告时间戳错乱问题：校验开始瞬间记录时间，打印报告直接复用，不受IO延迟干扰
"""

import os
import json
import copy
from datetime import datetime
from typing import Dict, Any, Tuple

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, NestedField
from pyiceberg.exceptions import NamespaceAlreadyExistsError

# ========== 0. 路径初始化 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE = os.path.join(BASE_DIR, "contract_warehouse")
os.makedirs(WAREHOUSE, exist_ok=True)

catalog = load_catalog("demo", **{
    "type": "sql",
    "uri": f"sqlite:///{os.path.join(BASE_DIR, 'contract_catalog.db')}",
    "warehouse": WAREHOUSE,
})

try:
    catalog.create_namespace("qc")
except NamespaceAlreadyExistsError:
    pass


# ========== 1. 数据契约定义层 ==========
class DataContract:
    """数据契约：质控域患者接入标准"""

    V1 = {
        "version": "1.0.0",
        "domain": "quality_control",
        "description": "HIS/EMR 患者主数据接入契约V1",
        "fields": {
            "patient_id": {
                "type": "long",
                "required": True,
                "unique": True,
                "label": "患者主键"
            },
            "name": {
                "type": "string",
                "required": True,
                "max_length": 50,
                "label": "姓名"
            },
            "age": {
                "type": "int",
                "required": True,
                "min": 0,
                "max": 150,
                "label": "年龄"
            },
            "gender": {
                "type": "string",
                "required": True,
                "enum": ["M", "F", "O"],
                "label": "性别"
            },
            "diagnosis_code": {
                "type": "string",
                "required": False,
                "pattern": r"^[A-Z]\d{2}(\.\d{1,2})?$",
                "label": "诊断编码(ICD-10)"
            }
        }
    }

    # 必须用深拷贝，否则修改 V2 会污染 V1
    V2 = copy.deepcopy(V1)
    V2["version"] = "2.0.0"
    V2["description"] = "HIS/EMR 患者主数据接入契约V2：新增手机号，放宽年龄上限"
    V2["fields"]["age"]["max"] = 200
    V2["fields"]["phone"] = {
        "type": "string",
        "required": False,
        "pattern": r"^1[3-9]\d{9}$",
        "label": "手机号"
    }


# ========== 2. 质控校验引擎 ==========
class QualityEngine:
    """数据质量校验引擎"""

    def __init__(self, contract: Dict[str, Any]):
        self.contract = contract
        self.rules = contract["fields"]

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = df.copy()
        df["_reject_reason"] = ""

        for col, rule in self.rules.items():
            if col not in df.columns:
                if rule.get("required"):
                    df["_reject_reason"] += f"[{col}]字段缺失;"
                continue

            # 2.1 非空校验
            if rule.get("required"):
                null_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
                df.loc[null_mask, "_reject_reason"] += f"[{col}]不能为空;"

            # 2.2 值域校验（数值型）
            if "min" in rule and "max" in rule:
                numeric_vals = pd.to_numeric(df[col], errors="coerce")
                range_mask = (numeric_vals < rule["min"]) | (numeric_vals > rule["max"])
                df.loc[range_mask, "_reject_reason"] += (
                    f"[{col}]值域越界({rule['min']}~{rule['max']});"
                )

            # 2.3 枚举校验
            if "enum" in rule:
                enum_mask = ~df[col].isin(rule["enum"])
                df.loc[enum_mask & df[col].notna(), "_reject_reason"] += (
                    f"[{col}]枚举值非法，允许{rule['enum']};"
                )

            # 2.4 正则校验（仅对非空值）
            if "pattern" in rule:
                non_null = df[col].notna() & (df[col].astype(str).str.strip() != "")
                regex_mask = non_null & ~df[col].astype(str).str.match(rule["pattern"])
                df.loc[regex_mask, "_reject_reason"] += f"[{col}]格式不匹配;"

            # 2.5 长度校验
            if "max_length" in rule:
                len_mask = df[col].astype(str).str.len() > rule["max_length"]
                df.loc[len_mask, "_reject_reason"] += f"[{col}]超长;"

        # 2.6 唯一性校验
        for col, rule in self.rules.items():
            if rule.get("unique") and col in df.columns:
                # keep=False：全部重复行都标记脏；如需保留第一条改为 keep="first"
                dup_mask = df[col].duplicated(keep=False)
                df.loc[dup_mask, "_reject_reason"] += f"[{col}]主键重复;"

        valid_mask = df["_reject_reason"] == ""
        valid_df = df[valid_mask].drop(columns=["_reject_reason"]).reset_index(drop=True)
        quarantine_df = df[~valid_mask].reset_index(drop=True)
        return valid_df, quarantine_df

    def generate_report(self, valid_df: pd.DataFrame, quarantine_df: pd.DataFrame, check_time: datetime) -> str:
        """
        生成质控报告
        :param valid_df: 合格数据集
        :param quarantine_df: 脏数据隔离集
        :param check_time: 校验发生时刻（外部传入，不在函数内部取now，避免IO延迟时间错乱）
        :return: 格式化报告字符串
        """
        total = len(valid_df) + len(quarantine_df)
        if total == 0:
            return "无数据"
        return f"""
╔══════════════════════════════════════════╗
║ 数据契约质控报告 v{self.contract['version']}             ║
╠══════════════════════════════════════════╣
  契约域: {self.contract['domain']}
  校验时间: {check_time.strftime('%Y-%m-%d %H:%M:%S')}
  总记录数: {total}
  合格入湖: {len(valid_df)} ({len(valid_df)/total*100:.1f}%)
  隔离异常: {len(quarantine_df)} ({len(quarantine_df)/total*100:.1f}%)
╚══════════════════════════════════════════╝
        """


# ========== 3. Iceberg Schema 工具 ==========
def to_iceberg_schema(contract_fields: Dict, extra_cols: Dict = None) -> Schema:
    """将契约字段转为 Iceberg Schema"""
    type_mapping = {
        "long": LongType(),
        "int": LongType(),
        "string": StringType(),
    }
    nested = []
    idx = 1
    for name, rule in contract_fields.items():
        iceberg_type = type_mapping.get(rule["type"], StringType())
        required = rule.get("required", False)
        nested.append(NestedField(idx, name, iceberg_type, required=required))
        idx += 1
    if extra_cols:
        for name, (iceberg_type, required) in extra_cols.items():
            nested.append(NestedField(idx, name, iceberg_type, required=required))
            idx += 1
    return Schema(*nested)


def to_arrow_table(df: pd.DataFrame, iceberg_schema: Schema) -> pa.Table:
    """将 DataFrame 转成与 Iceberg schema 兼容的 pyarrow Table：
    契约中 required 的字段在 Arrow 里也设为非空(nullable=False)"""
    table = pa.Table.from_pandas(df, preserve_index=False)
    required_map = {f.name: f.required for f in iceberg_schema.fields}
    new_fields = []
    for field in table.schema:
        # 表里 required 的字段，这里置为非空；其余保持可空
        is_required = required_map.get(field.name, False)
        new_fields.append(pa.field(field.name, field.type, nullable=not is_required))
    return table.cast(pa.schema(new_fields))


def get_or_create_table(ident: str, schema: Schema):
    """获取或创建 Iceberg 表（修复：不传 partition_spec=None）"""
    if catalog.table_exists(ident):
        catalog.drop_table(ident)
    return catalog.create_table(ident, schema=schema)


# ========== 4. 主流程 ==========
def main():
    print("=" * 60)
    print("🛡️  数据契约 Hello World：质控域患者数据接入")
    print("=" * 60)

    # ---------- 4.1 模拟 HIS/EMR 原始数据（含脏数据）----------
    raw_data = pd.DataFrame([
        {"patient_id": 1001, "name": "张三", "age": 45, "gender": "M", "diagnosis_code": "I25.1"},
        {"patient_id": 1002, "name": "李四", "age": 32, "gender": "F", "diagnosis_code": "J44.9"},
        {"patient_id": 1003, "name": "王五", "age": 78, "gender": "M", "diagnosis_code": None},
        {"patient_id": 1004, "name": "赵六" * 20, "age": 45, "gender": "M", "diagnosis_code": "ABC"},
        {"patient_id": 1005, "name": "孙七", "age": -5, "gender": "X", "diagnosis_code": "E11"},
        {"patient_id": 1001, "name": "重复ID", "age": 30, "gender": "F", "diagnosis_code": "A01"},
    ])

    print("\n📥 原始 HIS/EMR 数据（含脏数据）：")
    print(raw_data.to_string())
    print(f"\n总计 {len(raw_data)} 条")

    # ---------- 4.2 V1 契约校验 ----------
    print("\n" + "=" * 60)
    print("📋 执行数据契约 V1 校验")
    print("=" * 60)

    engine_v1 = QualityEngine(DataContract.V1)
    # ✅在校验开始前捕获时间戳，校验完成直接复用，不受后续IO读写影响
    check_time_v1 = datetime.now()
    valid_df, dirty_df = engine_v1.validate(raw_data)
    print(engine_v1.generate_report(valid_df, dirty_df, check_time_v1))

    # ---------- 4.3 合格数据入湖 ----------
    iceberg_schema_v1 = to_iceberg_schema(DataContract.V1["fields"])
    table_main = get_or_create_table("qc.patients", iceberg_schema_v1)
    table_main.append(to_arrow_table(valid_df, iceberg_schema_v1))

    print("✅ 合格数据已写入 Iceberg: qc.patients")

    # ---------- 4.4 脏数据隔离入湖 ----------
    if not dirty_df.empty:
        # 隔离表 Schema = 契约字段 + _reject_reason
        quarantine_schema = to_iceberg_schema(
            DataContract.V1["fields"],
            extra_cols={"_reject_reason": (StringType(), False)}
        )
        table_q = get_or_create_table("qc.patients_quarantine", quarantine_schema)
        table_q.append(to_arrow_table(dirty_df, quarantine_schema))

        print("🚫 脏数据已隔离入湖: qc.patients_quarantine")
        print("\n📋 隔离数据明细：")
        print(dirty_df.to_string())

    # ---------- 4.5 查询入湖数据 ----------
    print("\n📖 查询 qc.patients 入湖数据：")
    print(table_main.scan().to_arrow().to_pandas().to_string())

    # ---------- 4.6 契约演进 V2 + Iceberg Schema Evolution ----------
    print("\n" + "=" * 60)
    print("📋 数据契约演进 V2：新增 phone 字段，年龄上限放宽至 200")
    print("=" * 60)

    table_main = catalog.load_table("qc.patients")
    with table_main.update_schema() as update:
        update.add_column("phone", StringType(), required=False)
    table_main = catalog.load_table("qc.patients")
    print("✅ Iceberg Schema 已演进：新增 phone 字段")

    # V2 新数据（带手机号）
    new_data = pd.DataFrame([
        {"patient_id": 1006, "name": "周八", "age": 160, "gender": "F", "diagnosis_code": "N18.5", "phone": "13800138000"},
        {"patient_id": 1007, "name": "吴九", "age": 25, "gender": "M", "diagnosis_code": "K70", "phone": "13900139000"},
        {"patient_id": 1008, "name": "郑十", "age": 180, "gender": "O", "diagnosis_code": "Z51.1", "phone": "123456"},
    ])

    engine_v2 = QualityEngine(DataContract.V2)
    # ✅V2校验开始时刻捕获时间戳
    check_time_v2 = datetime.now()
    valid_v2, dirty_v2 = engine_v2.validate(new_data)
    print(engine_v2.generate_report(valid_v2, dirty_v2, check_time_v2))

    if not valid_v2.empty:
        # 注意：此时表已 add_column(phone)，要用演进后的 V2 schema（含 phone）
        iceberg_schema_v2 = to_iceberg_schema(DataContract.V2["fields"])
        table_main.append(to_arrow_table(valid_v2, iceberg_schema_v2))
        print("✅ V2 合格数据追加写入")

    print("\n📖 最终 qc.patients 全量数据（旧数据 phone=NULL）：")
    final_df = table_main.scan().to_arrow().to_pandas()
    print(final_df.to_string())

    # ---------- 4.7 元数据归档 ----------
    print("\n" + "=" * 60)
    print("📦 契约元数据归档")
    print("=" * 60)
    contract_meta = {
        "contract_version": DataContract.V2["version"],
        "applied_at": check_time_v2.isoformat(),   # ✅复用V2校验时间，不再重复调用now()
        "iceberg_table": "qc.patients",
        "schema_snapshot": str(table_main.current_snapshot().snapshot_id) if table_main.current_snapshot() else None
    }
    print(json.dumps(contract_meta, indent=2, ensure_ascii=False))

    print("\n🏁 Data Contract Hello World 完成！")


if __name__ == "__main__":
    main()
