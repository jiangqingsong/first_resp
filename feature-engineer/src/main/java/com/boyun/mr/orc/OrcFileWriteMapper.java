package com.boyun.mr.orc;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrcFileWriteMapper  extends Mapper<LongWritable,Text,Text,IntWritable>  {
    Text outputKey = new Text();
    IntWritable outputValue = new IntWritable(1);

/*    @Override
    protected void setup(Context context) {
        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        System.out.println("filePath:" + filePath);
    }*/

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        outputKey = value;
        context.write(outputKey,outputValue);
    }
}
