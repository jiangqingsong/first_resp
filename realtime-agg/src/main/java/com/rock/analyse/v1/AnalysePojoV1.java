package com.rock.analyse.v1;

import com.rock.analyse.pojo.MessageType;
import com.rock.analyse.util.RockConstants;
import org.joda.time.DateTime;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

public class AnalysePojoV1 {

    private BigInteger tagId;

    private Float maxVal = 0F;
    private Float minval = Float.MAX_VALUE;

    private String dataCode;

    private Float sumVal = 0F;

    private int originCount = 0;
    private int mergeCount = 0;

    private long startTime;

    private long endTime;

    private long lastEventTime; // 有可能是延迟的元素


    private Set<String> deviceIds = new HashSet<>();

    // 0 分钟 1 小时 2 天
    private byte type = 0;

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public long getLastEventTime() {
        return lastEventTime;
    }

    public void setLastEventTime(long lastEventTime) {
        this.lastEventTime = lastEventTime;
    }


    public BigInteger getTagId() {
        return tagId;
    }

    public void setTagId(BigInteger tagId) {
        this.tagId = tagId;
    }

    public Float getMaxVal() {
        return maxVal;
    }

    public void setMaxVal(Float maxVal) {
        this.maxVal = maxVal;
    }

    public Float getMinval() {
        return minval;
    }

    public void setMinval(Float minval) {
        this.minval = minval;
    }

    public String getDataCode() {
        return dataCode;
    }

    public void setDataCode(String dataCode) {
        this.dataCode = dataCode;
    }

    public Float getSumVal() {
        return sumVal;
    }

    public void setSumVal(Float sumVal) {
        this.sumVal = sumVal;
    }

    public Integer getOriginCount() {
        return originCount;
    }

    public void setOriginCount(Integer originCount) {
        this.originCount = originCount;
    }

    public Integer getMergeCount() {
        return mergeCount;
    }

    public void setMergeCount(Integer mergeCount) {
        this.mergeCount = mergeCount;
    }


    public void setOriginCount(int originCount) {
        this.originCount = originCount;
    }

    public void setMergeCount(int mergeCount) {
        this.mergeCount = mergeCount;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public AnalysePojoV1 calculation(

            MessageType messageType) {


        if (messageType.getTime() == lastEventTime) {
            messageType = null;
            return this;
        }

        Float val = messageType.getValue();
        if (val == null) {
            val = 0F;
        }
        if (val > maxVal) {
            maxVal = val;
        }

        if (val < minval) {

            minval = val;
        }


        sumVal = sumVal + val;

        originCount++;

        deviceIds.add(messageType.getDeviceId());

        mergeCount = deviceIds.size();

        tagId = messageType.getTag();

        this.setType((byte) 0);

        // 计算窗口时间

        messageType = null;
        return this;
    }

    public AnalysePojoV1 merge(AnalysePojoV1 analysePojoV1) {

        Float maxVal = analysePojoV1.getMaxVal();
        // 最大值
        if (maxVal > this.getMaxVal()) {
            this.setMaxVal(maxVal);
        }

        // 最小值
        Float minVal = analysePojoV1.getMinval();
        if (minVal < this.getMinval()) {
            this.setMinval(maxVal);
        }

        sumVal = sumVal + analysePojoV1.getSumVal();

        originCount = originCount + analysePojoV1.getOriginCount();
        deviceIds.addAll(analysePojoV1.deviceIds);
        mergeCount = deviceIds.size();
        tagId = analysePojoV1.tagId;
        this.setType((byte) 1);
        return this;
    }

    @Override
    public String toString() {

        DateTime dateTime = new DateTime(startTime);

        DateTime dateTime1 = new DateTime(endTime);


        String type = "";
        if (this.type == 0) {
            type = "分钟数据";
        }
        if (this.type == 1) {
            type = "小时数据";
        }
        if (this.type == 2) {
            type = "天数据";
        }
        String info = "AnalysePojoV2{" +
                "tagId='" + tagId + '\'' +
                ", maxVal=" + maxVal +
                ", minval=" + minval +
                ", sumVal=" + sumVal +
                ", originCount=" + originCount +
                ", mergeCount=" + mergeCount +
                ", startTime=" + startTime +
                '}';
        DateTime start = new DateTime(startTime);
        DateTime end = new DateTime(endTime);

        StringBuilder infoStr = new StringBuilder();

        infoStr.append(type).append("  ").append("所属时间窗口: [ " + start.toString(RockConstants.DATE_TIME_PATTERN))
                .append("----" + end.toString(RockConstants.DATE_TIME_PATTERN) + "]")
                .append(info);

        return infoStr.toString();
    }
}
