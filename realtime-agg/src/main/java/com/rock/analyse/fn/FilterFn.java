package com.rock.analyse.fn;

import com.rock.analyse.pojo.MessageType;

public class FilterFn implements org.apache.flink.api.common.functions.FilterFunction<MessageType> {
    @Override
    public boolean filter(MessageType value) throws Exception {
        return value != null;
    }
}
