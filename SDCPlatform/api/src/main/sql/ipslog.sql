create table security_ipslog(
    time string,
    report_equipment string,
    attack_means string,
    event_name string,
    source_address string,
    action string,
    rule_number string,
    events_number string,
    agreement_Summary string,
    popularity string,
    degree_of_danger string,
    service_type string,
    network_interface string,
    VLAN_ID string,
    destination_address string,
    source_mac string,
    destination_mac string,
    source_port string,
    destination_port string,
    original_message string
)
partitioned by (pt_d string)
row format delimited
fields terminated by '\t'
;

##load data local inpath '/home/brd/jqs/data/offline_data/security_ipslog.txt' overwrite into table security_ipslog partition(pt_d='20190927');