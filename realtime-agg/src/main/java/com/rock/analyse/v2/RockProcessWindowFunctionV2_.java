package com.rock.analyse.v2;

import com.rock.analyse.util.RockConstants;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class RockProcessWindowFunctionV2_ extends ProcessWindowFunction<AnalysePojoV2, AnalysePojoV2, String, TimeWindow> {

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
            System.out.println("发射天的数据！！！！ " + currentWatermark);

            out.collect(dayAnalysePojoV2);

        } else {

            TreeMap<Long, AnalysePojoV2> hourRs = new TreeMap<>();

            Iterator<Map.Entry<Long, AnalysePojoV2>> entrySets = minutesWindow.entrySet().iterator();
            Set<Long> houred = dayAnalysePojoV2.getHoured();

            while (entrySets.hasNext()) {

                Map.Entry<Long, AnalysePojoV2> entry = entrySets.next();

                long startMin = entry.getKey();

                long startHour = startMin - (startMin % RockConstants.ONE_HOUR_MILLISECOND);// 小时窗口开始时间

                long endHour = startHour + RockConstants.ONE_HOUR_MILLISECOND;// 小时窗口结束时间

                // 分钟数据
                AnalysePojoV2 tenAnalysePojoV2 = minutesWindow.get(startMin);

                tenAnalysePojoV2.setStartTime(startMin);
                tenAnalysePojoV2.setEndTime(startMin + RockConstants.TEN_MINUTES_MILLISECOND);

                if (houred.contains(startHour) || hourWatermark - startHour >= 2) {
                    // TODO 已经发射出小时数据 迟到元素 移除掉 使用侧输出流 处理迟到事件 小于2小时的数据移除
                    entrySets.remove();
                    continue;

                } else {
                    if (tenAnalysePojoV2.isUpdated()) {
                        out.collect(tenAnalysePojoV2); // 发出所有10分钟的数据
                        tenAnalysePojoV2.setUpdated(false); // 防止发出没有修改的数据
                    }

                }


                if (startHour < hourWatermark && !houred.contains(startHour)) {
                    // 如果发了小时数据那么就没有必要再发10分钟了的
                    AnalysePojoV2 hourAnalysePojoV2 = hourRs.get(startHour);
                    if (hourAnalysePojoV2 == null) {
                        hourAnalysePojoV2 = new AnalysePojoV2();
                        hourAnalysePojoV2.setType((byte) 1);
                        hourAnalysePojoV2.setStartTime(startHour);
                        hourAnalysePojoV2.setEndTime(endHour);
                        hourRs.put(startHour, hourAnalysePojoV2);
                    }
                    hourAnalysePojoV2.merge(entry.getValue());
                    // 删除10分钟的数据
                    entrySets.remove();
                }
            }

            // 发出小时数据
            for (Long key : hourRs.navigableKeySet()) {
                // 记录已经计算过的小时数据
                dayAnalysePojoV2.getHoured().add(key);
                out.collect(hourRs.get(key));

            }

        }

    }


}
