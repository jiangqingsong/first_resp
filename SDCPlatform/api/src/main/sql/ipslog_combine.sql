create table if not exists security_ipslog_combine(
    time string,
    report_equipment string,
    attack_means string,
    event_name string,
    source_address string,
    source_port string,
    destination_address string,
    destination_port string,
    action string,
    rule_number string,
    events_number string,
    agreement_summary string,
    popularity string,
    degree_of_danger string,
    service_type string,
    network_interface string,
    vlan_id string,
    source_mac string,
    destination_mac string
)
partitioned by (day string)
row format delimited
fields terminated by '\t'
;

##load data local inpath '/home/brd/jqs/data/offline_data/ips_combine_20150112' overwrite into table security_ipslog_combine partition(day='20150112');