import config
catalog = config.make_catalog()

rad = catalog.load_table("sepsis.ods_radiomics_features").scan().to_pandas()
print("ODS 影像表形状:", rad.shape)
print("ODS 影像表列名:", list(rad.columns))
print("patient_id 唯一数:", rad["patient_id"].nunique())