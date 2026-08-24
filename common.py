"""
公共模块：统一初始化Iceberg SqlCatalog，全局路径常量
湖仓一体化项目公共依赖
"""
import os
import sys

# common.py自身所在目录
COMMON_DIR = os.path.dirname(os.path.abspath(__file__))
# 将common所在目录加入Python搜索路径，无论从哪里启动脚本都能import
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

BASE_DIR = COMMON_DIR
WAREHOUSE_ROOT = os.path.join(BASE_DIR, "warehouse")
CATALOG_SQLITE_URI = f"sqlite:///{os.path.join(BASE_DIR, 'catalog.db')}"

os.makedirs(WAREHOUSE_ROOT, exist_ok=True)

from pyiceberg.catalog import load_catalog

def get_catalog():
    """获取全局SqlCatalog实例，所有脚本统一调用"""
    catalog = load_catalog(
        "lakehouse_catalog",
        **{
            "type": "sql",
            "uri": CATALOG_SQLITE_URI,
            "warehouse": WAREHOUSE_ROOT,
        }
    )
    return catalog
