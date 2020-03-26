package com.rock.analyse.fn;

import com.rock.analyse.pojo.MessageType;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class RockAssignerWithPeriodicWatermarks extends AscendingTimestampExtractor<MessageType> {

    @Override
    public long extractAscendingTimestamp(MessageType element) {
        return element.getTime();
    }
}
