package com.rock.analyse.v1;

import org.apache.flink.api.common.functions.AggregateFunction;

public class HRockAggregateFunction implements AggregateFunction<AnalysePojoV1, AnalysePojoV1, AnalysePojoV1> {


    @Override
    public AnalysePojoV1 createAccumulator() {
        return new AnalysePojoV1();
    }

    @Override
    public AnalysePojoV1 add(AnalysePojoV1 analysePojo, AnalysePojoV1 accumulator) {
        return accumulator.merge(analysePojo);

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
