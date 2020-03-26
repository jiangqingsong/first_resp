package com.rock.analyse.pojo;

import java.io.Serializable;
import java.math.BigInteger;


public class MessageType implements Serializable {

    private String deviceId;

    private Long time;

    private Long dataCode;

    private Float value;

    private BigInteger regionId;

    private BigInteger tag;

    // 0 表示点数据 1 表示groupCode数据
    private byte tagType;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getDataCode() {
        return dataCode;
    }

    public void setDataCode(Long dataCode) {
        this.dataCode = dataCode;
    }

    public Float getValue() {
        return value;
    }

    public void setValue(Float value) {
        this.value = value;
    }

    public BigInteger getRegionId() {
        return regionId;
    }

    public void setRegionId(BigInteger regionId) {
        this.regionId = regionId;
    }

    public BigInteger getTag() {
        return tag;
    }

    public void setTag(BigInteger tag) {
        this.tag = tag;
    }

    public byte getTagType() {
        return tagType;
    }

    public void setTagType(byte tagType) {
        this.tagType = tagType;
    }
}



