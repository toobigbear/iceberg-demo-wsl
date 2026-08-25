"""
Spark + Iceberg 本地模式
========================
用 Spark SQL 操作 Iceberg，支持更复杂的 SQL
⚠️前置：WSL安装JDK17，配置JAVA_HOME
warehouse 放到/tmp，规避WSL hadoop权限坑
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("IcebergSpark") \
.master("local[*]") \
.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
.config("spark.sql.catalog.local.type", "hadoop") \
.config("spark.sql.catalog.local.warehouse", "/tmp/spark-iceberg-warehouse") \
.getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 创建数据库
spark.sql("CREATE DATABASE IF NOT EXISTS local.nyc")

# 创建表
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.nyc.taxis (
        vendor_id BIGINT,
        trip_id BIGINT,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        pickup_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(pickup_time))  -- 按天隐藏分区
""")

# 插入数据
spark.sql("""
    INSERT INTO local.nyc.taxis VALUES
    (1, 1001, 2.5, 12.5, TIMESTAMP '2026-08-20 10:00:00'),
    (2, 1002, 5.0, 25.0, TIMESTAMP '2026-08-20 11:00:00'),
    (1, 1003, 1.0, 8.0, TIMESTAMP '2026-08-21 09:00:00')
""")

# 查询
print("📖 全部数据：")
spark.sql("SELECT * FROM local.nyc.taxis").show()

# ⭐ 时间旅行，查看快照
print("📸 快照历史：")
spark.sql("SELECT * FROM local.nyc.taxis.history").show()

# ⭐ MERGE INTO 演示：先建临时视图
print("📌 MERGE INTO 演示：")
spark.sql("""
CREATE OR REPLACE TEMP VIEW merge_source AS
SELECT * FROM VALUES 
(1, 1001, 3.0, 15.0, TIMESTAMP '2026-08-20 10:00:00')
AS s(vendor_id, trip_id, trip_distance, fare_amount, pickup_time)
""")

spark.sql("""
    MERGE INTO local.nyc.taxis t
    USING merge_source s
    ON t.trip_id = s.trip_id
    WHEN MATCHED THEN UPDATE SET t.fare_amount = s.fare_amount
    WHEN NOT MATCHED THEN INSERT *
""")

print("📖 MERGE 后：")
spark.sql("SELECT * FROM local.nyc.taxis").show()

# ⭐ DELETE
print("📌 DELETE 演示：")
spark.sql("DELETE FROM local.nyc.taxis WHERE vendor_id = 2")

print("📖 DELETE 后：")
spark.sql("SELECT * FROM local.nyc.taxis").show()

spark.stop()
