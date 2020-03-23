package com.brd.sdc.api.service;

import com.brd.sdc.api.beans.*;

import java.util.List;

public interface PrestoForPpeService {

    List<PpeDpiAggr> getDpiByTaskidAndTime(String taskId, String startTime, String endTime);

    List<PpeScanAggr> getScanByTaskid(String taskId);
}
