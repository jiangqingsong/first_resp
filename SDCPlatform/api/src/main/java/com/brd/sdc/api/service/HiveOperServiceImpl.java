package com.brd.sdc.api.service;

import com.brd.sdc.api.beans.Ipslog;
import com.brd.sdc.api.beans.PpeDpi;
import com.brd.sdc.api.beans.PpeScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangqingsong
 * @description 连接hive，通过jdbc方式获取数据
 * @date 2020-03-16 12:41
 */
@Service
public class HiveOperServiceImpl implements HiveOperService {
    @Autowired
    JdbcTemplate hiveJdbcTemplate;

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

    @Override
    public List<Ipslog> testDesc() {
        String sql = "SELECT * FROM sdc_detail.security_ipslog LIMIT 5";
        List<Ipslog> ipslogs = hiveJdbcTemplate.query(sql, new ResultSetExtractor<List<Ipslog>>() {
            @Override
            public List<Ipslog> extractData(ResultSet rs) throws SQLException, DataAccessException {
                ArrayList<Ipslog> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(ret2Ipslog(rs));
                }
                return list;
            }

        });
        return ipslogs;
    }

    @Override
    public List<PpeScan> getPpeScanByTaskid(String taskId) {

        String sql = "SELECT * FROM sdc_detail.ppe_scanlog WHERE task_id=" + taskId;

        List<PpeScan> scanList = hiveJdbcTemplate.query(sql, new ResultSetExtractor<List<PpeScan>>() {
            @Override
            public List<PpeScan> extractData(ResultSet rs) throws SQLException, DataAccessException {
                List<PpeScan> ppeScans = new ArrayList<>();
                while (rs.next()) {
                    PpeScan ppeScan = new PpeScan();
                    ppeScan.setTaskId(rs.getString("task_id"));
                    //todo  填充字段信息
                    ppeScans.add(ppeScan);
                }
                return ppeScans;
            }
        });
        return scanList;
    }

    @Override
    public List<PpeDpi> getDpiByTaskidAndTime(String taskId, String startTime, String endTime) {

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM sdc_detail_pre.ppe_dpilog_pre WHERE taskid=");
        sql.append("'");
        sql.append(taskId);
        sql.append("'");
        sql.append(" AND starttime =");
        sql.append("'");
        sql.append(startTime);
        sql.append("'");
        sql.append(" AND endtime =  ");
        sql.append("'");
        sql.append(endTime);
        sql.append("'");
        /*String sql = "SELECT * FROM sdc_detail_pre.ppe_dpilog WHERE task_id=" + taskId + " AND starttime =" + startTime
                + " AND endtime =  " + endTime;*/
        List<PpeDpi> ppeDpiList = hiveJdbcTemplate.query(sql.toString(), new ResultSetExtractor<List<PpeDpi>>() {
            @Override
            public List<PpeDpi> extractData(ResultSet rs) throws SQLException, DataAccessException {
                List<PpeDpi> ppeDpis = new ArrayList<>();
                while (rs.next()) {
                    PpeDpi ppeDpi = new PpeDpi();
                    ppeDpi.setTaskid(rs.getString("taskid"));
                    ppeDpi.setSrciptype(rs.getString("srciptype"));
                    ppeDpi.setDstiptype(rs.getString("dstiptype"));
                    ppeDpi.setTrproto(rs.getString("trproto"));
                    ppeDpi.setAssettype(rs.getString("assettype"));
                    ppeDpi.setProtocol(rs.getString("protocol"));
                    ppeDpi.setSrcport(rs.getString("srcport"));
                    ppeDpi.setDstport(rs.getString("dstport"));
                    ppeDpi.setStarttime(rs.getString("starttime"));
                    ppeDpi.setEndtime(rs.getString("endtime"));
                    ppeDpi.setUtraffic(rs.getString("utraffic"));
                    ppeDpi.setUpacktes(rs.getString("dtraffic"));
                    ppeDpi.setUpacktes(rs.getString("upacktes"));
                    ppeDpi.setUpacktes(rs.getString("dpackets"));
                    ppeDpi.setSrcip(rs.getString("srcip"));
                    ppeDpi.setDstip(rs.getString("dstip"));
                    ppeDpi.setDevtype(rs.getString("devtype"));
                    ppeDpi.setDevname(rs.getString("devname"));
                    ppeDpi.setSoftname(rs.getString("softname"));
                    ppeDpi.setSoftver(rs.getString("softver"));
                    ppeDpi.setVendor(rs.getString("vendor"));
                    ppeDpi.setOs(rs.getString("os"));
                    ppeDpi.setOsver(rs.getString("osver"));
                    ppeDpi.setNettype(rs.getString("nettype"));
                    ppeDpis.add(ppeDpi);
                }
                return ppeDpis;
            }
        });
        return ppeDpiList;
    }
}
