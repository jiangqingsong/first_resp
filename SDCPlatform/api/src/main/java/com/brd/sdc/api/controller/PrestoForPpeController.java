package com.brd.sdc.api.controller;

import com.brd.sdc.api.beans.PpeDpiAggr;
import com.brd.sdc.api.beans.PpeScanAggr;
import com.brd.sdc.api.response.ResponseBean;
import com.brd.sdc.api.response.UnicomResponseEnums;
import com.brd.sdc.api.service.PrestoForPpeServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jiangqingsong
 * @description 通过presto查询资产数据
 * @date 2020-03-18 10:18
 */
@RestController
public class PrestoForPpeController {
    @Autowired
    PrestoForPpeServiceImpl prestoForPpeServiceImpl;

    @PostMapping("/asset-sdc/task/queryForDpi")
    @ResponseBody
    public ResponseBean<List<PpeDpiAggr>> getDpiListByTaskidAndTime(@RequestParam Map<String, String> map){
        String taskId = map.get("taskId");
        String startTime = map.get("startTime");
        String endTime = map.get("endTime");
        List<PpeDpiAggr> dpiByTaskidAndTime = prestoForPpeServiceImpl.getDpiByTaskidAndTime(taskId, startTime, endTime);
        ResponseBean retData = new ResponseBean(dpiByTaskidAndTime, UnicomResponseEnums.SUCCESS);
        return retData;
    }

    @GetMapping("asset-sdc/task/query/{taskId}")
    @ResponseBody
    public ResponseBean<List<PpeScanAggr>> getScanListByTaskid(@PathVariable String taskId){
        List<PpeScanAggr> dpiByTaskidAndTime = prestoForPpeServiceImpl.getScanByTaskid(taskId);
        ResponseBean retData = new ResponseBean(dpiByTaskidAndTime, UnicomResponseEnums.SUCCESS);
        return retData;
    }
}
