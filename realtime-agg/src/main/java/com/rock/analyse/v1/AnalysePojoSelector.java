package com.rock.analyse.v1;

import org.apache.flink.api.java.functions.KeySelector;

public class AnalysePojoSelector implements KeySelector<AnalysePojoV1, String> {
    @Override
    public String getKey(AnalysePojoV1 value) throws Exception {
        return value.getTagId().toString();
    }
}
