package com.rock.analyse.pojo;
import org.apache.commons.collections.map.HashedMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaMessage {

    private String dbName;

    private String sql;

    private Map<String,String> defaultKV = new HashedMap();

    private List<List<String>> paras = new ArrayList<>(5000);

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, String> getDefaultKV() {
        return defaultKV;
    }

    public void setDefaultKV(Map<String, String> defaultKV) {
        this.defaultKV = defaultKV;
    }

    public List<List<String>> getParas() {
        return paras;
    }

    public void setParas(List<List<String>> paras) {
        this.paras = paras;
    }
}
