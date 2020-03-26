package com.rock.analyse.util;

import com.rock.analyse.v1.AnalysePojoV1;
import org.apache.flink.util.OutputTag;

public class RockConstants {

    public final static String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public final static Integer ONE_HOUR_MILLISECOND = 60 * 60 * 1000;

    public final static Integer TEN_MINUTES_MILLISECOND = 10 * 60 * 1000;


    public final static String INSERT_SQL_POINT = "INSERT  REPLACE INTO " +
            "t_data_point_record(f_data_point_id,f_time,f_data_code," +
            "f_value,f_original_data_number,f_effect_data_number,f_time_type)values(?,?,?,?,?,?,?)";

    public final static OutputTag<AnalysePojoV1> HOUR_OUTPUT_TAG = new OutputTag<AnalysePojoV1>("HOUR") {
    };

    public final static OutputTag<AnalysePojoV1> DAY_OUTPUT_TAG = new OutputTag<AnalysePojoV1>("DAY") {
    };
}
