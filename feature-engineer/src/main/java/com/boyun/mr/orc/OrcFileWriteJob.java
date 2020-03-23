package com.boyun.mr.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
public class OrcFileWriteJob extends Configured implements Tool{
    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();



        String schema="struct<province:string,content:string,date:string,aa:map<string,string>,arry:array<struct<name:string,sec:string,age:int>>>";
        conf.set("orc.mapred.output.schema",schema);

        String input = "C:\\Users\\jqs\\Desktop\\orc_code\\data\\word.txt";
        String output = "C:\\Users\\jqs\\Desktop\\orc_code\\data\\out";

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrcFileWriteJob.class);
        job.setMapperClass(OrcFileWriteMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(OrcFileWriteReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);

        job.setOutputFormatClass(OrcOutputFormat.class);


        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        boolean rt = job.waitForCompletion(true);
        System.out.println(rt);
        return rt?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int retnum = ToolRunner.run(conf,new OrcFileWriteJob(),args);
    }
}
