package com.brd.sdc.api.dao;

import com.brd.sdc.api.beans.TestRet;
import org.springframework.jdbc.core.RowMapper;

import javax.swing.tree.TreePath;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author jiangqingsong
 * @description
 * @date 2020-03-16 13:58
 */
public class TestRetMapper implements RowMapper<TestRet> {
    @Override
    public TestRet mapRow(ResultSet rs, int i) throws SQLException {
        TestRet testRet = new TestRet();
        testRet.setMsg("ok");
        testRet.setCnt(rs.getInt(0));
        return testRet;
    }
}
