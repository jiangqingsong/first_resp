package com.brd.sdc.api.controller;

import com.brd.sdc.api.beans.Ipslog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangqingsong
 * @description  test
 * @date 2020-03-18 10:18
 */
@RestController
public class PrestoOperController {
    @Autowired
    JdbcTemplate prestoTemplate;

    public Ipslog ret2Ipslog(ResultSet rs) throws SQLException {
        return getIpslog(rs);
    }
    public Ipslog getIpslog(ResultSet rs) throws SQLException {
        Ipslog ipslog = new Ipslog();
        ipslog.setTime(rs.getString("time"));
        ipslog.setReport_equipment(rs.getString("report_equipment"));
        ipslog.setAttack_means(rs.getString("attack_means"));
        ipslog.setEvent_name(rs.getString("event_name"));
        ipslog.setSource_address(rs.getString("source_address"));
        ipslog.setAction(rs.getString("action"));
        ipslog.setRule_number(rs.getString("rule_number"));
        ipslog.setEvents_number(rs.getString("events_number"));
        ipslog.setAgreement_summary(rs.getString("agreement_summary"));
        ipslog.setPopularity(rs.getString("popularity"));
        ipslog.setDegree_of_danger(rs.getString("degree_of_danger"));
        ipslog.setService_type(rs.getString("service_type"));
        ipslog.setNetwork_interface(rs.getString("network_interface"));
        ipslog.setVlan_id(rs.getString("vlan_id"));
        ipslog.setDestination_address(rs.getString("destination_address"));
        ipslog.setSource_mac(rs.getString("source_mac"));
        ipslog.setSource_port(rs.getString("source_port"));
        ipslog.setDestination_port(rs.getString("destination_port"));
        ipslog.setOriginal_message(rs.getString("original_message"));
        return ipslog;
    }
    @GetMapping("/presto/test")
    public String testPrestConn(){
        String sql = "SELECT * FROM sdc_detail.security_ipslog LIMIT 5";
        List<Ipslog> ipslogs = prestoTemplate.query(sql, new ResultSetExtractor<List<Ipslog>>() {
            @Override
            public List<Ipslog> extractData(ResultSet rs) throws SQLException, DataAccessException {
                ArrayList<Ipslog> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(ret2Ipslog(rs));
                }
                return list;
            }

        });
        return ipslogs.toString();
    }

    /**
     * test  diff databases
     * @return
     */
    @GetMapping("/presto/diffDb/test")
    public String testDiffDb(){
        String sql = "select count(1) from sdc_detail_pre.ppe_dpilog_pre where day=20200317 and minute='1200'";
        return prestoTemplate.queryForObject(sql, Integer.class).toString();
    }
}
