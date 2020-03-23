create table if not exists ppe_sangfor_ssl_vpn_log(
    time string commeNT '时间',
    sem_id string commENT '信息编号',
    ip_source string cOMMENT '源IP',
    port string commenT '端口',
    url string comment 'url',
    language string coMMENT '语言',
    ea_port string comMENT 'EA端口',
    login_mode string COMMENT '登录模式',
    app_count string cOMMENT 'APP数量',
    remote_app_count string COMMENT '远程app数量'
)
row format delimited
fields terminated by '\t'
;


load data local inpath '/home/brd/jqs/data/offline_data/sangfor_ssl_vpn.txt' overwrite into table ppe_sangfor_ssl_vpn_log partition(pt_d='20190917');