create table if not exists sdc_detail.security_lm_nips_log(
    time string,
    report_equipment string,
    attack_means string,
    event_name string,
    events_number string,
    source_ip string,
    source_interfance string,
    destination_ip string,
    destination_interfance string,
    agreement_summary string
)
partitioned by (pt_d string)
row format delimited
fields terminated by '\t'
;


##load data local inpath '/home/brd/jqs/data/offline_data/security_lm_nips_log.txt' overwrite into table security_lm_nips_log partition(pt_d='20150112');