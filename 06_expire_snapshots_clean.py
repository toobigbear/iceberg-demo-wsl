from common import get_catalog
from datetime import datetime, timedelta


def main():
    catalog = get_catalog()
    table_ident = "nyc.taxis_basic"
    table = catalog.load_table(table_ident)

    snap_list = table.snapshots()
    print(f"清理前快照数量：{len(snap_list)}")

    # ✅正确入口：table.maintenance 维护任务入口
    cutoff = datetime.now() - timedelta(seconds=60)
    table.maintenance.expire_snapshots().older_than(cutoff).commit()

    # 重新加载刷新元数据
    table = catalog.load_table(table_ident)
    print(f"清理后快照数量：{len(table.snapshots())}")
    print("""
⚠️注意：
expire_snapshots会物理删除不再被快照引用的parquet数据文件；
一旦清理，被过期的快照无法再做时间旅行，生产务必设置合理保留窗口。
👉测试提示：需要等待超过60秒，旧快照才会被过期回收；刚生成立刻运行不会删除。
""")


if __name__ == "__main__":
    main()
