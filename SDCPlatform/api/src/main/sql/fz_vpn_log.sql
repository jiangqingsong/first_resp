create table fz_vpn_log(
    times string COMMENT '时间',
    exchange_type string COMMENT '交换类型',
    isakmp_version string COMMENT 'ISAKMP版本',
    flags string COMMENT '标志',
    cookies string COMMENT 'Cookies',
    message_id string COMMENT '信息编号',
    packet_length string COMMENT '包长',
    payloads string COMMENT '有效载荷',
    payload_data_length string COMMENT '有效载荷数据长度',
    doi string COMMENT 'DOI',
    ip string COMMENT 'IP',
    protocol_id  string COMMENT '协议ID',
    spi_size string COMMENT 'SPI长度',
    transform_id string COMMENT '转换编号',
    encryption_algorithm string COMMENT '加密演算法',
    hash_algorithm  string COMMENT 'hash算法',
    authentication_method string COMMENT '身份验证方法',
    group_description string COMMENT '组织描述',
    life_type string COMMENT 'Life类型',
    life_duration  string COMMENT 'Life长度',
    notification string COMMENT '通知',
    vendor_id string COMMENT '供应商ID',
    description string COMMENT '供应商描述'
)
row format delimited
fields terminated by ','
collection items terminated by '-'
map keys terminated by ':'
;