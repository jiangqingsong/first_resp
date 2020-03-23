package com.brd.sdc.api.service;

import com.brd.sdc.api.beans.Ipslog;
import com.brd.sdc.api.beans.PpeDpi;
import com.brd.sdc.api.beans.PpeScan;

import java.util.List;

public interface HiveOperService {

    List<Ipslog> testDesc();
    List<PpeScan> getPpeScanByTaskid(String taskId);
    List<PpeDpi> getDpiByTaskidAndTime(String taskId, String startTime, String endTime);
}
