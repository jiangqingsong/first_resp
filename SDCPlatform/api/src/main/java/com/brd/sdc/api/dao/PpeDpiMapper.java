package com.brd.sdc.api.dao;

import com.brd.sdc.api.beans.PpeDpi;
import com.brd.sdc.api.beans.PpeScan;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-03-16 13:45
 */
public class PpeDpiMapper implements RowMapper<PpeDpi> {
    @Override
    public PpeDpi mapRow(ResultSet rs, int i) throws SQLException {
        PpeDpi ppeDpi = new PpeDpi();
        /*ppeDpi.setTaskId(rs.getString("task_id"));
        ppeDpi.setStartTime(rs.getString("time"));*/
        //todo  填充字段
        return ppeDpi;
    }
}
