package com.boyun.mr.orc;

import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcStruct;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrcStructPkg implements WritableComparable {
    OrcStruct orcStruct;
    String path;

    public OrcStructPkg(OrcStruct orcStruct, String path) {
        this.orcStruct = orcStruct;
        this.path = path;
    }

    public OrcStruct getOrcStruct() {
        return orcStruct;
    }

    public void setOrcStruct(OrcStruct orcStruct) {
        this.orcStruct = orcStruct;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
