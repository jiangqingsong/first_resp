package com.rock.analyse.v1;

import com.rock.analyse.util.RockConstants;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MRockProcessWindowFunction extends ProcessWindowFunction<AnalysePojoV1, AnalysePojoV1, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<AnalysePojoV1> elements, Collector<AnalysePojoV1> out) throws Exception {
        AnalysePojoV1 analysePojoV1 = elements.iterator().next();
        TimeWindow timeWindow = context.window();

        analysePojoV1.setStartTime(timeWindow.getStart());
        analysePojoV1.setEndTime(timeWindow.getEnd());

        context.output(RockConstants.HOUR_OUTPUT_TAG, elements.iterator().next());

        out.collect(analysePojoV1);


    }


}
