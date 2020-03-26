package com.rock.analyse.v1;

import com.rock.analyse.pojo.MessageType;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MRockAggregateFunction
        implements AggregateFunction<MessageType, AnalysePojoV1, AnalysePojoV1> {

    @Override
    public AnalysePojoV1 createAccumulator() {
        return new AnalysePojoV1();
    }

    @Override
    public AnalysePojoV1 add(MessageType messageType, AnalysePojoV1 accumulator) {
        return accumulator.calculation(messageType);

    }

    @Override
    public AnalysePojoV1 getResult(AnalysePojoV1 accumulator) {
        return accumulator;
    }

    @Override
    public AnalysePojoV1 merge(AnalysePojoV1 a, AnalysePojoV1 b) {
        return a.merge(b);
    }
}
