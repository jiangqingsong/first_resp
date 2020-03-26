package com.rock.analyse.v2;
import com.rock.analyse.MessageTypeKeySelector;
import com.rock.analyse.fn.FilterFn;
import com.rock.analyse.fn.Kafka2MessageTypeFn;
import com.rock.analyse.fn.RockAssignerWithPeriodicWatermarks;
import com.rock.analyse.util.RockConstants;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class LauncherDriver02 {
    public static void main(String[] args) throws Exception {

        InputStream inputStream = LauncherDriver02.class.getResourceAsStream("/cfg.properties");
        Properties properties = new Properties();
        properties.load(inputStream);

        System.out.println(properties.get("kafka.brokers"));

        System.out.println(properties.get("pkafka.brokers"));

        // 默认不做checkpoint
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig.GlobalJobParameters globalJobParameters = new ExecutionConfig.GlobalJobParameters();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // 设置水位线定期出发频率
        env.getConfig().setAutoWatermarkInterval(RockConstants.TEN_MINUTES_MILLISECOND);

        // TODO  保存更多的数据可以写磁盘
//        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:/data/data1/", true);
//        env.setStateBackend(rocksDBStateBackend);


        env.getConfig().setGlobalJobParameters(globalJobParameters);
        Properties kafkaPro = new Properties();
        kafkaPro.setProperty("bootstrap.servers", properties.getProperty("kafka.brokers"));
        kafkaPro.setProperty("group.id", properties.getProperty("kafka.group_id") + new Random().nextFloat());
        kafkaPro.setProperty("fetch.min.bytes", "100000");

        // 第一次消费从最初位置开始消费
        properties.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(properties.getProperty("kafka.topic"),
                new SimpleStringSchema(), kafkaPro);

        // 添加数据源
        DataStreamSource<String> dataStreamSource
                = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator dataStream = dataStreamSource

                .map(new Kafka2MessageTypeFn())

                .filter(new FilterFn())

                .assignTimestampsAndWatermarks(new RockAssignerWithPeriodicWatermarks())
                .keyBy(0)

                .keyBy(new MessageTypeKeySelector())

                .window(TumblingEventTimeWindows.of(Time.days(1L), Time.hours(-8))) //天的话8小时时区的问题


                .trigger(ContinuousEventTimeTrigger.of(Time.milliseconds(RockConstants.TEN_MINUTES_MILLISECOND)))

                .aggregate(new RockAggregateFunctionV2(), new RockProcessWindowFunctionV2_())

                .map(new MapFunction<AnalysePojoV2, String>() {
                    @Override
                    public String map(AnalysePojoV2 value) throws Exception {
                        return KafkaUtil.toKafkaStr(value);
                    }
                });

//        Properties kafkaProp = new Properties();
//        kafkaProp.setProperty("bootstrap.servers", properties.getProperty("pkafka.brokers"));
//        kafkaProp.setProperty("fetch.min.bytes", "100000");
//        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(properties.getProperty("pkafka.brokers"),
//                new SimpleStringSchema(),kafkaProp);
//
//        dataStream.addSink(flinkKafkaProducer);



        dataStream.print();

        env.execute(" Analysis ");
    }


}
