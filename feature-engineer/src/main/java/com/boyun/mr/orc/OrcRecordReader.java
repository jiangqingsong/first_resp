package com.boyun.mr.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.List;

public class OrcRecordReader extends RecordReader<NullWritable, OrcRow> {
    private final TypeDescription schema;
    private final org.apache.orc.RecordReader batchReader;
    private final VectorizedRowBatch batch;
    private int rowInBatch;
    private FileSplit split;
    private Path path;

    private WritableComparable orcStruct;
    private OrcRow orcrow = new OrcRow();


/*
    public OrcRecordReader(CombineFileSplit combineFileSplit, org.apache.orc.RecordReader reader, TaskAttemptContext context,
                           TypeDescription schema, Integer index) throws IOException {
        this.split = new FileSplit(combineFileSplit.getPath(index), combineFileSplit.getOffset(index),
                combineFileSplit.getLength(index), combineFileSplit.getLocations());
        path = split.getPath();
        Configuration conf = context.getConfiguration();
        this.batchReader = reader;
        this.batch = schema.createRowBatch();
        this.schema = schema;
        rowInBatch = 0;
        this.orcStruct = OrcStruct.createValue(schema);
    }*/


    public OrcRecordReader(Reader fileReader,
                                    Reader.Options options,Path path) throws IOException {
        this.batchReader = fileReader.rows(options);
        this.path=path;
        if (options.getSchema() == null) {
            schema = fileReader.getSchema();
        } else {
            schema = options.getSchema();
        }
        this.batch = schema.createRowBatch();
        rowInBatch = 0;
        this.orcStruct = OrcStruct.createValue(schema);
    }

/*    public OrcRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index) throws IOException {

        this.split = new FileSplit(combineFileSplit.getPath(index), combineFileSplit.getOffset(index),
                combineFileSplit.getLength(index), combineFileSplit.getLocations());
        path = split.getPath();

        Configuration conf = context.getConfiguration();
        Reader fileReader = OrcFile.createReader(split.getPath(),
                OrcFile.readerOptions(conf)
                        .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)));

        Reader.Options options = OrcInputFormat.buildOptions(conf,
                fileReader, split.getStart(), split.getLength());
        this.batchReader = fileReader.rows(options);
        if (options.getSchema() == null) {
            schema = fileReader.getSchema();
        } else {
            schema = options.getSchema();
        }
        this.batch = schema.createRowBatch();
        rowInBatch = 0;
        this.orcStruct = OrcStruct.createValue(schema);
    }*/

    boolean ensureBatch() throws IOException {

        if (rowInBatch >= batch.size) {
            rowInBatch = 0;
            return batchReader.nextBatch(batch);
        }
        return true;
    }


    @Override
    public void close() throws IOException {

        batchReader.close();
    }

    @Override
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext taskAttemptContext) {

        // nothing required
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!ensureBatch()) {
            return false;
        }
        if (schema.getCategory() == TypeDescription.Category.STRUCT) {
            OrcStruct result = (OrcStruct) orcStruct;
            List<TypeDescription> children = schema.getChildren();
            int numberOfChildren = children.size();
            for (int i = 0; i < numberOfChildren; ++i) {
                result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], rowInBatch,
                        children.get(i), result.getFieldValue(i)));
            }
        } else {
            OrcMapredRecordReader.nextValue(batch.cols[0], rowInBatch, schema, orcStruct);
        }
        rowInBatch += 1;
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public OrcRow getCurrentValue() throws IOException, InterruptedException {

        orcrow.setPath(path);
        orcrow.setOrcStruct((OrcStruct) orcStruct);
        return orcrow;
    }

    @Override
    public float getProgress() throws IOException {

        return batchReader.getProgress();
    }
}
