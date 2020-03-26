package com.rock.analyse.v2;

import com.rock.analyse.pojo.MessageType;
import com.rock.analyse.util.RockConstants;
import org.joda.time.DateTime;

import java.util.*;

/**
 * 分钟聚合 小时聚合 天聚合
 */
public class AnalysePojoV2 {

    private String tagId;

    private Float maxVal = Float.MIN_NORMAL;

    private Float minval = Float.MAX_VALUE;

    private String dataCode;

    private Float sumVal = 0F;

    private int originCount = 0;

    private int mergeCount = 0;

    private long startTime;

    private long endTime;

    private boolean updated;

    public boolean isUpdated() {
        return updated;
    }

    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    private long lastHourTime = Long.MIN_VALUE;


    private Set<String> devices = new HashSet<>(256);


    // 记录计算的小时时间点
    private Set<Long> houred = new HashSet<>();

    public Set<Long> getHoured() {
        return houred;
    }

    public void setHoured(Set<Long> houred) {
        this.houred = houred;
    }

    public long getLastHourTime() {
        return lastHourTime;
    }

    public void setLastHourTime(long lastHourTime) {
        this.lastHourTime = lastHourTime;
    }

    // 0 分钟  1 小时  2 天
    private byte type;

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    // 窗口水位线
    private TreeMap<Long, AnalysePojoV2> minutesWindow;

    public TreeMap<Long, AnalysePojoV2> getMinutesWindow() {
        return minutesWindow;
    }

    public void setMinutesWindow(TreeMap<Long, AnalysePojoV2> minutesWindow) {
        this.minutesWindow = minutesWindow;
    }

    public String getTagId() {
        return tagId;
    }

    public void setTagId(String tagId) {
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

    public AnalysePojoV2 calculation(MessageType messageType) {
        Float val = messageType.getValue();
        if (this.tagId == null) {
            this.setTagId(messageType.getTag().toString());
        }
        // 分钟级别的聚合
        if (this.getType() == 0 || this.getType() == 1) {

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

            devices.add(messageType.getDeviceId());
            mergeCount = devices.size();
            this.setUpdated(true);
        } else {
            // 天的汇聚方式
            Long eventTime = messageType.getTime();
            long startMinWin = eventTime - (eventTime % RockConstants.TEN_MINUTES_MILLISECOND);

            if (minutesWindow == null) {
                minutesWindow = new TreeMap<>(new Comparator<Long>() {
                    @Override
                    public int compare(Long o1, Long o2) {
                        return o1.compareTo(o2);
                    }
                });

            }

            long hourTime = eventTime - (eventTime % RockConstants.ONE_HOUR_MILLISECOND);

            // 保留最小的小时时间起点
            if (lastHourTime < hourTime) {
                lastHourTime = hourTime;
            }

            AnalysePojoV2 mAnalysePojoV2 = minutesWindow.get(startMinWin);

            if (mAnalysePojoV2 == null) {
                mAnalysePojoV2 = new AnalysePojoV2();
                // 分钟级别
                mAnalysePojoV2.setType((byte) 0);
                minutesWindow.put(startMinWin, mAnalysePojoV2);
            }
            mAnalysePojoV2.calculation(messageType);

        }
        return this;
    }


    /**
     * merge 小时 天的数据
     *
     * @return
     */
    public AnalysePojoV2 merge(AnalysePojoV2... analysePojoV2s) {

        for (AnalysePojoV2 analysePojoV2 : analysePojoV2s) {
            Float maxVal = analysePojoV2.getMaxVal();
            // 最大值
            if (maxVal > this.getMaxVal()) {
                this.setMaxVal(maxVal);
            }

            // 最小值
            Float minVal = analysePojoV2.getMinval();
            if (minVal < this.getMinval()) {
                this.setMinval(maxVal);
            }

            sumVal = sumVal + analysePojoV2.getSumVal();
            originCount = originCount + analysePojoV2.getOriginCount();
            devices.addAll(analysePojoV2.devices);
            mergeCount = devices.size();
        }
        return this;

    }

    @Override
    public String toString() {
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
