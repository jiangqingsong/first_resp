package com.boyun.mr.orc;

import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;

public class OrcWriteKey {
    TypeDescription schema;
    Path path;
    String child;
    public void checkorSetPath(Path outPath) {
        if (path == null) path = outPath;
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public void setSchema(TypeDescription schema) {
        this.schema = schema;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public String getChild() {
        return child;
    }

    public void setChild(String child) {
        this.child = child;
    }
}
