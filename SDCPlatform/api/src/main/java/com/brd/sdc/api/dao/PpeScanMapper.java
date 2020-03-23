package com.brd.sdc.api.dao;

import com.brd.sdc.api.beans.PpeScan;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-03-16 13:45
 */
public class PpeScanMapper implements RowMapper<PpeScan> {
    @Override
    public PpeScan mapRow(ResultSet rs, int i) throws SQLException {
        PpeScan ppeScan = new PpeScan();
        ppeScan.setTaskId(rs.getString("task_id"));
        ppeScan.setSourceIp(rs.getString(""));
        //todo  填充字段
        return ppeScan;
    }
}
