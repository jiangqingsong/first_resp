package com.boyun.mr.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcProto;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;
public class OrcFileReadJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = getConf();

        String input = "C:\\Users\\jqs\\Desktop\\orc_code\\orc_spark";
        String output = "C:\\Users\\jqs\\Desktop\\orc_code\\read_o\\" + System.currentTimeMillis();

        //conf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, "filed1");
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrcFileReadJob.class);
        job.setMapperClass(OrcFileReadMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(CombineOrcInputFormat.class);      //设置输入格式为Orc格式
        job.setOutputFormatClass(TextOutputFormat.class);       //输出格式为文本格式

        CombineOrcInputFormat.addInputPath(job, new Path(input));      //以Orc的方式加载输入路径
        CombineOrcInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean rt = job.waitForCompletion(true);

        System.out.println(rt);
        return rt ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int retnum = ToolRunner.run(conf,new OrcFileReadJob(),args);
    }
}
