package com.rock.analyse.fn;

import com.rock.analyse.pojo.MessageType;
import org.apache.flink.api.java.functions.KeySelector;

public class MessageTypeKeySelector implements KeySelector<MessageType, String> {

    @Override
    public String getKey(MessageType value) throws Exception {
        return value.getTag() + "";
    }
}
