package com.boyun.mr.orc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcStruct;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrcRow implements WritableComparable<OrcRow> {
    private OrcStruct orcStruct;
    private Path path;
    private String table;
    int date;
    int minute;

    public OrcStruct getOrcStruct() {
        return orcStruct;
    }

    public void setOrcStruct(OrcStruct orcStruct) {
        this.orcStruct = orcStruct;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public int getDate() {
        return date;
    }

    public void setDate(int date) {
        this.date = date;
    }

    public int getMinute() {
        return minute;
    }

    public void setMinute(int minute) {
        this.minute = minute;
    }

    public OrcRow getTime() {
        Path parent = path.getParent();
        while (true) {
            String child = parent.getName();
            if (child.endsWith(".db"))
                break;
            if (child.startsWith("minute=")) {
                minute = Integer.parseInt(child.substring(child.indexOf("=") + 1));
            } else if (child.startsWith("day=")) {
                date = Integer.parseInt(child.substring(child.indexOf("=") + 1));
            }
            parent = parent.getParent();
        }
        return this;
    }

    @Override
    public int compareTo(OrcRow o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }


}
