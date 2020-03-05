create table sangfor_ssl_vpn(
    times string commeNT '时间',
    sem_id string commENT '信息编号',
    ip_source string cOMMENT '源IP',
    port string commenT '端口',
    url string comment 'url',
    cookie string commENT 'cookie',
    language string coMMENT '语言',
    ea_port string comMENT 'EA端口',
    login_mode string COMMENT '登录模式',
    app_count string cOMMENT 'APP数量',
    remote_app_count string COMMENT '远程app数量',
    service_state string COMMENT '服务状态'
)
row format delimited
fields terminated by ','
collection items terminated by '-'
map keys terminated by ':'
;