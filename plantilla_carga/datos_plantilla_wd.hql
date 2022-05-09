DROP TABLE IF EXISTS ${hiveconf:PARAM_BU_WORKDAY}.datos_pklowercase_wd;
CREATE EXTERNAL TABLE ${hiveconf:PARAM_BU_WORKDAY}.datos_pklowercase_wd(
--valores_columnas_plantilla
)
PARTITIONED BY (`data_date_part` string)
STORED AS PARQUET LOCATION '${hiveconf:HDFS_BUSINESS_WORKDAY}/datos_pklowercase_wd';