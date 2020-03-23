create table if not exists sdc_detail.security_equipment_log(
    time string,
    source_ip string,
    attack_type string,
    source_interface string,
    attack_packets string,
    vpn string,
    times string,
    reason string,
    local_interface string,
    local_mac string,
    local_vlan string,
    local_ce_vlan string,
    receive_interface string,
    receive_mac string,
    receive_vlan string,
    receive_ce_vlan string,
    ip_conflict_type string
)
partitioned by (pt_d string)
row format delimited
fields terminated by '\t'
;


##load data local inpath '/home/brd/jqs/data/offline_data/security_equipment.txt' overwrite into table security_equipment_log partition(pt_d='20150926');