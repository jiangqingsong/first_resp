create table security_sanfor_af_log(
    time string,
    types  string,
    protocol string,
    method string,
    URL string,
    source_area string,
    source_ip string,
    source_ip_local string,
    source_port string,
    destination_area string,
    destination_ip string,
    destination_port string,
    rule_id_number string,
    reply_status_code string,
    match_policy_name string,
    description string,
    severity_level string,
    action string,
    hazard_statement string
)
partitioned by (pt_d string)
row format delimited
fields terminated by '\t'
;


##load data local inpath '/home/brd/jqs/data/offline_data/sanfor_af.txt' overwrite into table security_af partition(pt_d='20190927');