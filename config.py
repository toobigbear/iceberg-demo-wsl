"""sepsis_lakehouse 全局配置：路径 / Catalog / 数据源"""
import os

# ---- 基础路径（所有路径基于脚本所在目录，保证可移植） ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")          # 模拟对象存储
OUTPUT_DIR = os.path.join(BASE_DIR, "output")                 # 演示日志/报告
os.makedirs(WAREHOUSE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---- Catalog 配置（SQLite 本地 Catalog，WSL 下可稳定运行） ----
CATALOG_NAME = "sepsis_lakehouse"
CATALOG_URI = f"sqlite:///{os.path.join(BASE_DIR, 'sepsis_lakehouse_catalog.db')}"

# ---- MySQL 业务库（源）配置；连不上会自动回退 CSV
# 注意WSL2 里 localhost 不是 Windows 的 localhost，这里的IP必须是 WSL 的网络的IP
#需要在 WSL 里执行 ：ip route show | grep default----
#还要放行防火墙，管理员 PowerShell下：netsh advfirewall firewall add rule name="MySQL 3306 WSL" dir=in action=allow protocol=TCP localport=3306
MYSQL = dict(
    host=os.environ.get("MYSQL_HOST", "172.17.208.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", "root"),
    database=os.environ.get("MYSQL_DB", "sepsis_cardio"),
)

# ---- CSV 回退源（sepsis_cardio_test 导出的清洗数据，无 MySQL 时用） ----
CSV_FALLBACK = os.environ.get(
    "SEPSIS_CSV_DIR",
    "/mnt/d/PythonProject/sepsis_cardio_test/data/processed",
)

# ---- 数据表定义：每个源表对应一个 ODS 表 ----
# 格式: ODS表名 -> (MySQL表名, CSV文件名, 主键列)
SOURCE_TABLES = {
    "ods_patients": ("patients", "clinical_cleaned_*.csv", "id"),
    "ods_clinical_metrics": ("clinical_metrics", "clinical_cleaned_*.csv", "id"),
    "ods_radiomics_features": ("radiomics_features", "radiomics_cleaned.csv", "id"),
}


def make_catalog():
    """返回 PyIceberg SQLite Catalog 实例"""
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        CATALOG_NAME,
        **{"type": "sql", "uri": CATALOG_URI, "warehouse": WAREHOUSE_DIR},
    )
