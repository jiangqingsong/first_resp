create table ppe_h3c_firewalllog(
    time string,
    ike_p1_sa_establish_fail string,
    reason string,
    sa_information string,
    role string,
    local_ip string,
    local_id_type string,
    local_id string,
    local_port string,
    retransmissions string,
    remote_ip string,
    remote_id_type string,
    remote_id string,
    remote_port string,
    recived_retransmissions string,
    inside_vpn_instance string,
    outside_vpn_instance string,
    initiator_cookie string,
    responder_cookie string,
    connection_id string,
    tunnel_id string,
    ike_profile_name  string
)
COMMENT 'h3c 防火墙日志'
partitioned by (pt_d string)
row format delimited
fields terminated by '\t'
;


