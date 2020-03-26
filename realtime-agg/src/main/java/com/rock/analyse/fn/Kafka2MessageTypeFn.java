package com.rock.analyse.fn;

import com.alibaba.fastjson.JSONObject;
import com.rock.analyse.pojo.MessageType;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigInteger;

public class Kafka2MessageTypeFn implements MapFunction<String, MessageType> {

    @Override
    public MessageType map(String value)  {
        MessageType rtn = null;
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            MessageType messageType1 = new MessageType();
            BigInteger deviceId = jsonObject.getBigInteger("deviceId");
            BigInteger tag = jsonObject.getBigInteger("tag");
            BigInteger regionId = jsonObject.getBigInteger("regionId");
            Byte tagType = jsonObject.getByte("tagType");
            if(tagType == null){
                tagType = (byte)0;
            }
            messageType1.setDeviceId(deviceId.toString());
            messageType1.setTime(jsonObject.getLong("time")*1000);
            messageType1.setValue(jsonObject.getFloat("value"));
            messageType1.setTag(tag);
            messageType1.setRegionId(regionId);
            messageType1.setTagType(tagType);
            rtn = messageType1;

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("###### Parsing error ！！！ " + value);

        }
        return rtn;
    }
}
