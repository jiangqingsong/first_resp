package com.rock.analyse.fn;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * 数据格式转换成 kafka的数据格式
 */
public class Rs2SinkType implements MapFunction<Object, Object> {


    @Override
    public Object map(Object value) throws Exception {
        return null;
    }
}
