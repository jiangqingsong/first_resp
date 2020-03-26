package com.rock.analyse.v2;

import org.apache.flink.api.java.functions.KeySelector;

public class AnalysePojoV2Selector implements KeySelector<AnalysePojoV2, String> {
    @Override
    public String getKey(AnalysePojoV2 value) throws Exception {
        return value.getTagId();
    }
}
