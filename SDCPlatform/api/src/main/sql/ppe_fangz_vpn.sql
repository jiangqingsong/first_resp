create table if not exists sdc_detail.ppe_fz_vpn_log(
    time string,
    exchange_type string,
    isakmp_version string,
    flags string,
    cookies string,
    message_id string,
    packet_length string,
    payloads string,
    payload_data_length string,
    doi string,
    ip string,
    protocol_id  string,
    spi_size string,
    transform_id string,
    encryption_algorithm string,
    hash_algorithm  string,
    authentication_method string,
    group_description string,
    life_type string,
    life_duration  string,
    vendor_id string,
    description string
)
partitioned by (pt_d string)
row format delimited
fields terminated by '\t'
;


##load data local inpath '/home/brd/jqs/data/offline_data/ppe_fz_vpn.txt' overwrite into table ppe_fz_vpn_log partition(pt_d='20190917');