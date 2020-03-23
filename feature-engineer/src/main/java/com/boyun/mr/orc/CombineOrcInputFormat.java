package com.boyun.mr.orc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapreduce.OrcInputFormat;


import java.io.IOException;

public class CombineOrcInputFormat extends OrcInputFormat<OrcRow> {



    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, OrcRow> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        Path path = split.getPath();
        Reader file = OrcFile.createReader(split.getPath(),
                OrcFile.readerOptions(conf)
                        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)));

        String schema="struct<province:string,date:string,content:string,aa:map<string,string>,arry:array<struct<name:string,sec:string,age:int>>>";
        TypeDescription typeSchema = TypeDescription.fromString(schema);

        return new OrcRecordReader(file,
                org.apache.orc.mapred.OrcInputFormat.buildOptions(conf,
                        file, split.getStart(), split.getLength()).schema(typeSchema),path);
    }
}
