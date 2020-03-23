CREATE TABLE IF NOT EXISTS sdc_detail.ppe_dpilog_abnormal (
	starttime string,
	abnormal_traffic string
)
PARTITIONED BY(day int, minute string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS ORC
;