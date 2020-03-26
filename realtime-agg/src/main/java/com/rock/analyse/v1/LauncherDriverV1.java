package com.rock.analyse.v1;

import com.rock.analyse.MessageTypeKeySelector;
import com.rock.analyse.fn.FilterFn;
import com.rock.analyse.fn.Kafka2MessageTypeFn;
import com.rock.analyse.fn.RockAssignerWithPeriodicWatermarks;
import com.rock.analyse.util.RockConstants;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.InputStream;
import java.util.Properties;



public class LauncherDriverV1 {


    public static void main(String[] args) throws Exception {

        InputStream inputStream = LauncherDriverV1.class.getResourceAsStream("/cfg.properties");
        Properties properties = new Properties();
        properties.load(inputStream);

        System.out.println(properties.get("kafka.brokers"));

        // 默认不做checkpoint
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig.GlobalJobParameters globalJobParameters = new ExecutionConfig.GlobalJobParameters();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);

        /**
         *  demo  start.
         */
        IntCounter intCounter = new IntCounter();
        intCounter.add(1);
        DataStreamSource<String> textSource = env.readTextFile("");
        textSource.map(x -> x.split("\t")).map(x -> x[0]);
        textSource.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return null;
            }
        });
        SingleOutputStreamOperator<String> map = textSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s + "\"test\"";
            }
        });

        env.enableCheckpointing(5000);//set checkpoint.

        // 保存更多的数据可以写磁盘
//        StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:/data/data1/", true);
//        env.setStateBackend(rocksDBStateBackend);

        env.getConfig().setGlobalJobParameters(globalJobParameters);
        Properties kafkaPro = new Properties();
        kafkaPro.setProperty("bootstrap.servers", properties.getProperty("kafka.brokers"));
        kafkaPro.setProperty("group.id", properties.getProperty("kafka.group_id"));
        kafkaPro.setProperty("fetch.min.bytes", "100000");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(properties.getProperty("kafka.topic"), new SimpleStringSchema(), kafkaPro);
        flinkKafkaConsumer.setStartFromEarliest();
        // 添加数据源
        DataStreamSource<String> dataStreamSource
                = env.addSource(flinkKafkaConsumer); // kafka的并行度


        SingleOutputStreamOperator<AnalysePojoV1> dataStream = dataStreamSource
                .map(new Kafka2MessageTypeFn()).uid("kafka->messageType")
                .filter(new FilterFn()).uid("filter->null")
                .assignTimestampsAndWatermarks(new RockAssignerWithPeriodicWatermarks()).uid("RockAssignerWithPeriodicWatermarks")
                .keyBy(new MessageTypeKeySelector())
                .timeWindow(Time.milliseconds(3*60*1000))
                .aggregate(new MRockAggregateFunction(), new MRockProcessWindowFunction()).uid("");


        dataStream.print(); //
        DataStream<AnalysePojoV1> hourDataStream = dataStream.getSideOutput(RockConstants.HOUR_OUTPUT_TAG);

        SingleOutputStreamOperator<AnalysePojoV1> dataStream1 = hourDataStream
                .keyBy(new AnalysePojoSelector())
                .timeWindow(Time.milliseconds(6*60*1000))
                .aggregate(new HRockAggregateFunction(), new HRockProcessWindowFunction());


        DataStream<AnalysePojoV1> dataStream2 = dataStream1.getSideOutput(RockConstants.DAY_OUTPUT_TAG);
        dataStream2.print();


        SingleOutputStreamOperator dataStream3 = dataStream2
                .keyBy(new AnalysePojoSelector())
                .window(TumblingEventTimeWindows.of(Time.days(1L),Time.hours(-8))) //天有时区问题
                .aggregate(new HRockAggregateFunction(), new HRockProcessWindowFunction());
        dataStream3.print();

        env.execute("test kafka ");
    }
}
