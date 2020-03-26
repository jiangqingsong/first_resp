package com.rock.analyse.v2;
import com.rock.analyse.util.RockConstants;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class RockProcessWindowFunctionV2 extends ProcessWindowFunction<AnalysePojoV2, AnalysePojoV2, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<AnalysePojoV2> elements,
                        Collector<AnalysePojoV2> out) throws Exception {

        AnalysePojoV2 dayAnalysePojoV2 = elements.iterator().next();

        long currentWatermark = context.currentWatermark();

        long hourWatermark = currentWatermark - (currentWatermark % RockConstants.ONE_HOUR_MILLISECOND); // start


        TreeMap<Long, AnalysePojoV2> minutesWindow = dayAnalysePojoV2.getMinutesWindow();

        TimeWindow timeWindow = context.window();

        if (currentWatermark > timeWindow.maxTimestamp()) { // 天的数据

            for (AnalysePojoV2 mAnalysePojoV2 : minutesWindow.values()) {
                dayAnalysePojoV2.merge(mAnalysePojoV2);
            }
            dayAnalysePojoV2.setStartTime(timeWindow.getStart());
            dayAnalysePojoV2.setEndTime(timeWindow.getEnd());
            // 发射天的数据
            System.out.println("###### 发射天的数据！！！！ " + currentWatermark);

            out.collect(dayAnalysePojoV2);

        } else {

            // 是否计算小时数据
            boolean hourCalculationFlag = hourWatermark > dayAnalysePojoV2.getLastHourTime();

            TreeMap<Long, AnalysePojoV2> hourRs = new TreeMap<>();

            for (Long startMin : minutesWindow.navigableKeySet()) {

                // 分钟数据
                AnalysePojoV2 tenAnalysePojoV2 = minutesWindow.get(startMin);

                tenAnalysePojoV2.setStartTime(startMin);
                tenAnalysePojoV2.setEndTime(startMin + RockConstants.TEN_MINUTES_MILLISECOND);

                out.collect(tenAnalysePojoV2); // 发出所有10分钟的数据

                if (hourCalculationFlag) { // 同时计算小时数据

                    long startHour = startMin - (startMin % RockConstants.ONE_HOUR_MILLISECOND);

                    long endHour = startHour + RockConstants.ONE_HOUR_MILLISECOND;

                    if (startHour < hourWatermark) {


                        AnalysePojoV2 hourAnalysePojoV2 = hourRs.get(startHour);
                        if (hourAnalysePojoV2 == null) {
                            hourAnalysePojoV2 = new AnalysePojoV2();
                            hourAnalysePojoV2.setType((byte) 1);
                            hourAnalysePojoV2.setStartTime(startHour);
                            hourAnalysePojoV2.setEndTime(endHour);
                            hourRs.put(startHour, hourAnalysePojoV2);
                        }

                        hourAnalysePojoV2.merge(minutesWindow.get(startMin));

                    }

                }
            }

            if (hourCalculationFlag) {
                dayAnalysePojoV2.setLastHourTime(hourWatermark + RockConstants.ONE_HOUR_MILLISECOND);
            }


            // 发出小时数据
            for (Long key : hourRs.navigableKeySet()) {
                out.collect(hourRs.get(key));

            }

            // 保存最近一个小时的 10分钟的数据
            Iterator<Map.Entry<Long, AnalysePojoV2>> entrySets = minutesWindow.entrySet().iterator();

            while (entrySets.hasNext()) {
                Map.Entry<Long, AnalysePojoV2> entry = entrySets.next();
                long time = entry.getKey();
                long startHour = time - time % RockConstants.ONE_HOUR_MILLISECOND;
                if (hourWatermark - startHour >= RockConstants.ONE_HOUR_MILLISECOND) {
                    entrySets.remove();
                }
            }
        }

    }


}
