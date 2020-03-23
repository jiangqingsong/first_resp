package com.boyun.mr.orc;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

public class OrcFileWriteReducer extends Reducer<Text,IntWritable,NullWritable,OrcStruct> {
    private MultipleOutputs<NullWritable, OrcStruct> multipleOutputs;

    private OrcStruct pair;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String schema = context.getConfiguration().get("orc.mapred.output.schema");
        pair = (OrcStruct)OrcStruct.createValue(TypeDescription.fromString(schema));
        TypeDescription.createString();
        multipleOutputs = new MultipleOutputs<NullWritable, OrcStruct>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        String[] lineSplit = line.trim().split(",");

        List<String> provinces = new ArrayList(){{
            add("江苏省");
            add("四川省");
            /*add("广东省");
            add("贵州省");
            add("江西省");
            add("湖南省");
            add("广西省");
            add("甘肃省");
            add("青海省");*/
        }};
        int index = new Random().nextInt(2);
        List<String> dates = new ArrayList(){{
            add("20191210");
            add("20191211");
        }};

        int index1 = new Random().nextInt(2);
        String province = provinces.get(index);
        String date = dates.get(index1);
        pair.setFieldValue("province", new Text(province));
        pair.setFieldValue("date",new Text(date));
        pair.setFieldValue("content",new Text("context_test" + System.currentTimeMillis()));

        TypeDescription map = TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createString());
        TreeMap<WritableComparable, WritableComparable> orcmap = new OrcMap<>(map);
        orcmap.put(new Text("11"),new Text("11"));
        orcmap.put(new Text("22"),new Text("22"));
        orcmap.put(new Text("33"),new Text("33"));
        pair.setFieldValue("aa",(OrcMap)orcmap);


        OrcList<OrcStruct> orclist = new OrcList<>(TypeDescription.createStruct()
                .addField("name", TypeDescription.createString())
                .addField("sec", TypeDescription.createString())
                .addField("age", TypeDescription.createInt()));
        OrcStruct orcStruct = new OrcStruct(TypeDescription.createStruct()
                .addField("name", TypeDescription.createString())
                .addField("sex", TypeDescription.createString())
                .addField("age", TypeDescription.createInt()));
        orcStruct.setFieldValue("name",new Text("zhangsan"));
        orcStruct.setFieldValue("sex",new Text("nan"));
        orcStruct.setFieldValue("age",new IntWritable(21));
        orclist.add(orcStruct);
        pair.setFieldValue("arry",orclist);

        String path = "C:\\Users\\jqs\\Desktop\\orc_code\\data\\out\\" + province + "\\" + date + "\\apart" ;
//        pair.setFieldValue(5,new IntWritable(Integer.parseInt(lineSplit[5])));
        System.out.println(pair.getSchema()+"=================");
        //context.write(NullWritable.get(),pair);
        //multipleOutputs.write(NullWritable.get(), pair, path);
        multipleOutputs.write( NullWritable.get(), pair, path );
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
