package com.brd.sdc.api.controller;

import com.brd.sdc.api.beans.Ipslog;
import com.brd.sdc.api.beans.PpeDpi;
import com.brd.sdc.api.beans.PpeScan;
import com.brd.sdc.api.service.HiveOperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * @author jiangqingsong
 * @description hive数据查询
 * @date 2020-03-16 13:09
 */
@RestController
@ResponseBody
public class HiveOperController {
    @Autowired
    HiveOperServiceImpl hiveOperServiceImpl;

   /* *//**
     * 根据采集任务唯一标识，获取已采集的资产列表数据（主动探测）
     *//*
    @GetMapping("/asset-sdc/task/query/{taskId}")
    public List<PpeScan> getPpeScanByTaskid(@RequestParam String taskId){
        List<PpeScan> ppeScanList = hiveOperServiceImpl.getPpeScanByTaskid(taskId);
        return ppeScanList;
    }

    *//**
     * 根据采集任务唯一标识，获取已采集的资产列表数据（DPI探测）
     * @param map
     *       taskId:    【String】任务ID
     *       startTime: 【String】数据入库起始时间(毫秒)
     *       endTime:   【String】数据入库结束时间(毫秒)
     *//*
    @PostMapping("/asset-sdc/task/queryForDpi")
    public List<PpeDpi> getDpiByTaskidAndTime(@RequestParam Map<String, String> map){
        String taskId = map.get("taskId");
        String startTime = map.get("startTime");
        String endTime = map.get("endTime");
        return hiveOperServiceImpl.getDpiByTaskidAndTime(taskId, startTime, endTime);
    }*/

    /**
     * 测试controller
     * @return
     */
    @GetMapping("/testConnect")
    public List<Ipslog> testConnHive(){
        List<Ipslog> ipslogList = hiveOperServiceImpl.testDesc();
        return ipslogList;
    }
}
