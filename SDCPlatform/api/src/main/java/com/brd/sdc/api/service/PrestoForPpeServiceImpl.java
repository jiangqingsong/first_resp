package com.brd.sdc.api.service;

import com.brd.sdc.api.beans.*;
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
public class PrestoForPpeServiceImpl implements PrestoForPpeService {
    @Autowired
    JdbcTemplate prestoTemplate;

    @Override
    public List<PpeScanAggr> getScanByTaskid(String taskId) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM sdc_detail.ppe_scanlog WHERE taskid=");
        sql.append("'");
        sql.append(taskId);
        sql.append("'");
        List<PpeScanAggr> ppeDpiList = prestoTemplate.query(sql.toString(), new ResultSetExtractor<List<PpeScanAggr>>() {
            @Override
            public List<PpeScanAggr> extractData(ResultSet rs) throws SQLException, DataAccessException {
                List<PpeScanAggr> ppeDpis = new ArrayList<>();
                while (rs.next()) {
                    PpeScanAggr ppeScan = new PpeScanAggr();
                    ppeScan.setTaskid(rs.getString("taskid"));

                    ppeScan.setIp(rs.getString("ip"));
                    ppeScan.setIptype(rs.getString("iptype"));
                    ppeScan.setIphplace(rs.getString("iphplace"));
                    ppeScan.setMac(rs.getString("mac"));
                    ppeScan.setAssettype(rs.getString("assettype"));
                    ppeScan.setOsver(rs.getString("osver"));
                    ppeScan.setPort(rs.getString("port"));
                    ppeScan.setOpstype(rs.getString("opstype"));
                    ppeScan.setSoftver(rs.getString("softver"));
                    ppeScan.setHttpwarever(rs.getString("httpwarever"));
                    ppeScan.setSfinfo(rs.getString("sfinfo"));
                    ppeScan.setTime(rs.getString("time"));
                    //漏洞库信息
                    ppeScan.setNumber(rs.getString("number"));
                    ppeScan.setTitle(rs.getString("title"));
                    ppeScan.setServerity(rs.getString("serverity"));
                    ppeScan.setProducts(rs.getString("products"));
                    ppeScan.setIsevent(rs.getString("isevent"));
                    ppeScan.setSubmittime(rs.getString("submittime"));
                    ppeScan.setOpentime(rs.getString("opentime"));
                    ppeScan.setDiscoverername(rs.getString("discoverername"));
                    ppeScan.setFormalway(rs.getString("formalway"));
                    ppeScan.setDescription(rs.getString("description"));
                    ppeScan.setPatchname(rs.getString("patchname"));
                    ppeScan.setPatchdescription(rs.getString("patchdescription"));
                    ppeDpis.add(ppeScan);
                }
                return ppeDpis;
            }
        });
        return ppeDpiList;
    }

    @Override
    public List<PpeDpiAggr> getDpiByTaskidAndTime(String taskId, String startTime, String endTime) {

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM sdc_detail.ppe_dpilog WHERE taskid=");
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
        List<PpeDpiAggr> ppeDpiList = prestoTemplate.query(sql.toString(), new ResultSetExtractor<List<PpeDpiAggr>>() {
            @Override
            public List<PpeDpiAggr> extractData(ResultSet rs) throws SQLException, DataAccessException {
                List<PpeDpiAggr> ppeDpis = new ArrayList<>();
                while (rs.next()) {
                    PpeDpiAggr ppeDpi = new PpeDpiAggr();
                    ppeDpi.setTaskid(rs.getString("taskid"));
                    ppeDpi.setSrciptype(rs.getString("srciptype"));
                    ppeDpi.setDstiptype(rs.getString("dstiptype"));
                    ppeDpi.setTrproto(rs.getString("trproto"));
                    ppeDpi.setProtocol(rs.getString("protocol"));
                    ppeDpi.setSrcport(rs.getString("srcport"));
                    ppeDpi.setDstport(rs.getString("dstport"));
                    ppeDpi.setStarttime(rs.getString("starttime"));
                    ppeDpi.setEndtime(rs.getString("endtime"));
                    ppeDpi.setUtraffic(rs.getString("utraffic"));
                    ppeDpi.setDtraffic(rs.getString("dtraffic"));
                    ppeDpi.setUpacktes(rs.getString("upacktes"));
                    ppeDpi.setDpackets(rs.getString("dpackets"));
                    ppeDpi.setSrcip(rs.getString("srcip"));
                    ppeDpi.setDstip(rs.getString("dstip"));
                    ppeDpi.setDevname(rs.getString("devname"));
                    ppeDpi.setSoftname(rs.getString("softname"));
                    ppeDpi.setSoftver(rs.getString("softver"));
                    ppeDpi.setVendor(rs.getString("vendor"));
                    ppeDpi.setOs(rs.getString("os"));
                    ppeDpi.setOsver(rs.getString("osver"));

                    //漏洞库信息
                    ppeDpi.setNumber(rs.getString("number"));
                    ppeDpi.setTitle(rs.getString("title"));
                    ppeDpi.setServerity(rs.getString("serverity"));
                    ppeDpi.setProducts(rs.getString("products"));
                    ppeDpi.setIsevent(rs.getString("isevent"));
                    ppeDpi.setSubmittime(rs.getString("submittime"));
                    ppeDpi.setOpentime(rs.getString("opentime"));
                    ppeDpi.setDiscoverername(rs.getString("discoverername"));
                    ppeDpi.setFormalway(rs.getString("formalway"));
                    ppeDpi.setDescription(rs.getString("description"));
                    ppeDpi.setPatchname(rs.getString("patchname"));
                    ppeDpi.setPatchdescription(rs.getString("patchdescription"));
                    ppeDpis.add(ppeDpi);
                }
                return ppeDpis;
            }
        });
        return ppeDpiList;
    }
}
