package com.boyun.mr.orc;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
public class OrcFileReadMapper extends Mapper<NullWritable, OrcRow, Text, NullWritable> {

    private Text outputKey = new Text();
    @Override
    protected void map(NullWritable key, OrcRow value, Context context) throws IOException, InterruptedException {

        OrcStruct orcStruct = value.getOrcStruct();

        //获取String类型
        Text province = (Text) orcStruct.getFieldValue("province");
        //获取map中的k，v
        OrcMap<Text, Text> mapResult = (OrcMap<Text, Text>) orcStruct.getFieldValue("aa");
        Text t1 = mapResult.get(new Text("11"));
        Text t2 = mapResult.get(new Text("22"));
        Text t3 = mapResult.get(new Text("33"));

        t1.clear();
        System.out.println(
                "k(11) -> " + t1 + "|" +
                "k(22) -> " + t2 + "|" +
                "k(33) -> " + t3 + "|"
        );

    }
}
