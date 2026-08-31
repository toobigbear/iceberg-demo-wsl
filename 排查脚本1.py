import config

catalog = config.make_catalog()
cli = catalog.load_table("sepsis.dwd_clinical").scan().to_pandas()
rad = catalog.load_table("sepsis.dwd_radiomics").scan().to_pandas()

print("=== 临床表 ===")
print("patient_id dtype:", cli["patient_id"].dtype)
print("样本前10个:", list(cli["patient_id"].astype(str).head(10)))
print("去重后唯一数:", cli["patient_id"].nunique(), "/ 总行数", len(cli))

print("\n=== 影像表 ===")
print("id dtype:", rad["id"].dtype)
print("样本前10个:", list(rad["id"].astype(str).head(10)))
print("去重后唯一数:", rad["id"].nunique(), "/ 总行数", len(rad))

# 类型都转成字符串再比，看交集多大
cli_set = set(cli["patient_id"].astype(str))
rad_set = set(rad["id"].astype(str))
print("\n=== 对齐检查 ===")
print("临床唯一:", len(cli_set), " 影像唯一:", len(rad_set))
print("字符串交集:", len(cli_set & rad_set))
print("临床∩影像, 只属于临床:", len(cli_set - rad_set))