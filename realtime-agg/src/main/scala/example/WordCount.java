package example;

import com.alibaba.fastjson.JSONObject;
import com.rock.analyse.pojo.MessageType;
import com.rock.analyse.v1.AnalysePojoV1;
import com.rock.analyse.v2.AnalysePojoV2;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.*;

public class WordCount {


    // 构建命令 线上   mvn clean scala:compile package -Pprd
    //         测试   mvn clean scala:compile package

    public static void main(String[] args) throws Exception {

        JSONObject jsonObject =new JSONObject();
        BigInteger bigInteger = new BigInteger("18446744073709551615");
        BigInteger bigInteger1 = new BigInteger("18446744073709551615");
        System.out.println(bigInteger.equals(bigInteger1));
        String dataCode = "7%s4321";
       System.out.println( String.format(dataCode,"01"));

        System.out.println(JSONObject.toJSONString(new HashedMap()));

        TreeMap<Long, String> hourRs = new TreeMap<>();
        hourRs.put(1000L,"asjbdkjasd");
        hourRs.put(1000L,"asjbdkjasd");
        hourRs.put(1000L,"asjbdkjasd");
        hourRs.put(new Long(1000),"asjbdkjasd");
        System.out.println(hourRs.keySet().size());

//        // Checking input parameters
//        final ParameterTool params = ParameterTool.fromArgs(args);
//
//        // set up the execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // make parameters available in the web interface
//        env.getConfig().setGlobalJobParameters(params);
//
//        // get input data
//        DataStream<String> text;
//        if (params.has("input")) {
//            // read the text file from given input path
////            text = env.readTextFile(params.get("input"));
//        } else {
//            System.out.println("Executing WordCount example with default input data set.");
//            System.out.println("Use --input to specify file input.");
////			// get default test text data
//            text = env.addSource(new SourceFunction<String>() {
//
//                 Random random = new Random();
//                @Override
//                public void run(SourceContext<String> ctx) throws Exception {
//
//                    Map<Long, AnalysePojoV1> rtn = new HashedMap();
//                    long start = System.currentTimeMillis();
//                    while (true) {
//                        long ind = Long.valueOf(random.nextInt(1)+"");
//                        MessageType messageType = new MessageType();
//                        messageType.setTime(System.currentTimeMillis());
////                        messageType.setDataCode(70098);
////                        messageType.setDeviceId(Long.valueOf(random.nextInt(5)+""));
////                        messageType.setRegionId("");
////                        messageType.setTag(ind);
//                        messageType.setValue(random.nextFloat());
//                        if(rtn.get(ind) == null){
//                            rtn.put(ind,new AnalysePojoV1());
//                        }
//                        rtn.get(ind).calculation(messageType);
//                        ctx.collect(JSONObject.toJSONString(messageType));
//
//                        long end = System.currentTimeMillis();
//
//                        if(end - start > 1000*60){
//                            System.out.println(JSONObject.toJSONString(rtn));
//
//                        }
//                    }
//
//                }
//
//                @Override
//                public void cancel() {
//
//                }
//            });

//            Properties props = new Properties();
//            props.setProperty("bootstrap.servers", "localhost:9092");
//
//            FlinkKafkaProducer producer = new FlinkKafkaProducer(
//                    "test01",new SimpleStringSchema(),props);
//
//            text.addSink(producer);
//        }

        // execute program
//        env.execute("Streaming WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}
