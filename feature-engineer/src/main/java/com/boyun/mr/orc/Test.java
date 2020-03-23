package com.boyun.mr.orc;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.util.*;

public class Test {
    public static void main1(String[] args) {
        TypeDescription string = TypeDescription.fromString("struct<c1:string,c2:string>");
        TypeDescription string2 = TypeDescription.fromString("struct<c1:string>");
        TypeDescription string3 = TypeDescription.fromString("struct<c1:string,c2:string,c3:string>");

        TreeMap<Text, Text> orcMap = new OrcMap(string);
        orcMap.put(new Text("testK"), new Text("testV"));
        System.out.println(orcMap.toString());


        OrcList<WritableComparable> orcList = new OrcList<>(string2);
        orcList.add(new Text("testList"));
        orcList.add(new Text("testList2"));
        System.out.println(orcList.toString());

        OrcStruct orcStruct = new OrcStruct(string3);
        orcStruct.setFieldValue("c1", new Text("t1"));
        orcStruct.setFieldValue("c2", new Text("t2"));
        orcStruct.setFieldValue("c3", new Text("t3"));
        System.out.println(orcStruct.toString());


    }

    public static void main(String[] args) {
        List<Text> texts = new ArrayList<>();
        Text aa = new Text("aa");
        texts.add(aa);
        System.out.println(aa);
        System.out.println(StringUtils.join(texts, ","));
        System.out.println("---------------------");
        aa.clear();
        System.out.println(aa);
        System.out.println(StringUtils.join(texts, ","));
    }

    public static void test(String... strs) {
        System.out.println(StringUtils.join(strs, ","));

    }
}