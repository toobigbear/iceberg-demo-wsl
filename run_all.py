import sys
import os

# 把base文件夹加入模块路径
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "base"))

import subprocess

scripts = [
    "base/01_create_table_basic.py",
    "base/02_partition_write_read.py",
    "base/03_snapshot_time_travel.py",
]

if __name__ == "__main__":
    for s in scripts:
        print(f"\n>>>>>>>>>> 执行 {s}")
        subprocess.run([sys.executable, s], check=True)
