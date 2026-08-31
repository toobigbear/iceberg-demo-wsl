# debug_catalog.py —— 放在 sepsis_lakehouse/ 目录下运行
import os
from pyiceberg.catalog import load_catalog

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ★ 把下面这段 load_catalog 的配置原样改成 build_dwd.py 里用的那一段
catalog = load_catalog(
    "demo_catalog",
    **{
        "type": "sql",
        "uri": f"sqlite:///{os.path.join(BASE_DIR, 'iceberg_catalog.db')}",
        "warehouse": os.path.join(BASE_DIR, "iceberg_warehouse"),
    }
)

print("=== 当前 catalog 里所有的命名空间和表 ===")
for ns in catalog.list_namespaces():
    print("namespace:", ns)
    for t in catalog.list_tables(ns):
        print("   ", t)