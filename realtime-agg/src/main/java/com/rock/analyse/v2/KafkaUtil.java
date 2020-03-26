package com.rock.analyse.v2;

import com.alibaba.fastjson.JSONObject;
import com.rock.analyse.util.RockConstants;
import org.apache.commons.collections.map.HashedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaUtil {

    /**
     * @return
     */
    public static String toKafkaStr(AnalysePojoV2 analysePojoV2) {
        Float minVal = analysePojoV2.getMinval();
        Float maxVal = analysePojoV2.getMaxVal();
        Float sumVal = analysePojoV2.getSumVal();
        Float avgVal = sumVal / analysePojoV2.getOriginCount();

        Integer count = analysePojoV2.getOriginCount();
        Integer merge = analysePojoV2.getMergeCount();
        byte type = analysePojoV2.getType();
        List<List<Object>> paras = new ArrayList<>();

        String dataCode = "7%s4321";

        for (int i = 0; i < 4; i++) {
            List<Object> paras_ = new ArrayList<>();
            paras_.add(analysePojoV2.getTagId());
            paras_.add(analysePojoV2.getStartTime());
            if (i == 0) { // 最小

                if (type == (byte) 0) {
                    paras_.add(String.format(dataCode, "01"));
                } else if (type == (byte) 1) {
                    paras_.add(String.format(dataCode, "02"));
                } else if (type == (byte) 2) {
                    paras_.add(String.format(dataCode, "03"));
                } else {
                    break;
                }
                paras_.add(minVal);

            }

            if (i == 1) { // 最大

                if (type == (byte) 0) {
                    paras_.add(String.format(dataCode, "07"));
                } else if (type == (byte) 1) {
                    paras_.add(String.format(dataCode, "08"));
                } else if (type == (byte) 2) {
                    paras_.add(String.format(dataCode, "09"));
                } else {
                    break;
                }
                paras_.add(maxVal);
            }

            if (i == 2) { // sum

                if (type == (byte) 0) {
                    paras_.add(String.format(dataCode, "19"));
                } else if (type == (byte) 1) {
                    paras_.add(String.format(dataCode, "20"));
                } else if (type == (byte) 2) {
                    paras_.add(String.format(dataCode, "21"));
                } else {
                    break;
                }
                paras_.add(sumVal);
            }

            if (i == 3) {// avg
                paras_.add(avgVal);
                if (type == (byte) 0) {
                    paras_.add(String.format(dataCode, "04"));
                } else if (type == (byte) 1) {
                    paras_.add(String.format(dataCode, "05"));
                } else if (type == (byte) 2) {
                    paras_.add(String.format(dataCode, "06"));
                } else {
                    break;
                }
                paras_.add(avgVal);
            }

            paras_.add(count);
            paras_.add(merge);

            if (type == (byte) 0) {
                paras_.add("1");
            } else if (type == (byte) 1) {
                paras_.add("2");
            } else if (type == (byte) 2) {
                paras_.add("3");
            } else {
                break;
            }
            paras.add(paras_);
        }

        Map<String, Object> rtnMap = new HashedMap();
        rtnMap.put("dbName", "");
        rtnMap.put("sql", RockConstants.INSERT_SQL_POINT);
        rtnMap.put("defaultKV", "{}");
        rtnMap.put("paras", paras);
        return JSONObject.toJSONString(rtnMap);
    }
}
