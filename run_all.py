"""一键执行全部：入湖 + 分层 + 能力演示"""
import os
import config
from ingest_ods import ingest_ods
from build_dwd import build_dwd
from build_ads import build_ads
from demo_time_travel import TimeTravelDemo
from demo_schema_evolve import SchemaEvolveDemo
from demo_partition import run as run_partition
from demo_atomic import run as run_atomic


def main():
    print("=" * 60); print("① ODS 原始入湖"); print("=" * 60)
    ingest_ods()
    print("=" * 60); print("② DWD 清洗标准化"); print("=" * 60)
    build_dwd()
    print("=" * 60); print("③ ADS 融合建模层"); print("=" * 60)
    build_ads()
    print("=" * 60); print("④ 快照&时间旅行"); print("=" * 60)
    TimeTravelDemo().run()
    print("=" * 60); print("⑤ Schema 演进"); print("=" * 60)
    SchemaEvolveDemo().run()
    print("=" * 60); print("⑥ 隐藏分区"); print("=" * 60)
    run_partition()
    print("=" * 60); print("⑦ ACID 原子性"); print("=" * 60)
    run_atomic()
    print("\n✅ sepsis_lakehouse 全链路演示完成")


if __name__ == "__main__":
    main()
