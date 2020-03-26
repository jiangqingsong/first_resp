package com.rock.analyse.v2;

import com.rock.analyse.pojo.MessageType;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 一个key 一个对象
 */
public class RockAggregateFunctionV2
        implements AggregateFunction<MessageType, AnalysePojoV2, AnalysePojoV2> {

    @Override
    public AnalysePojoV2 createAccumulator() {
        // 保存天的计算结果
        AnalysePojoV2 AnalysePojoV2 = new AnalysePojoV2();
        AnalysePojoV2.setType((byte) 2);
        return AnalysePojoV2;
    }

    @Override
    public AnalysePojoV2 add(MessageType messageType, AnalysePojoV2 accumulator) {
        return accumulator.calculation(messageType);

    }

    @Override
    public AnalysePojoV2 getResult(AnalysePojoV2 accumulator) {
        return accumulator;
    }

    @Override
    public AnalysePojoV2 merge(AnalysePojoV2 a, AnalysePojoV2 b) {
        return a.merge(b);
    }
}
