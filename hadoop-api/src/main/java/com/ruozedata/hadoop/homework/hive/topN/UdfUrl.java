package com.ruozedata.hadoop.homework.hive.topN;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName UdfUrl
 * @Description
 * @Author suguoming
 * @Date 2020/7/20 4:51 下午
 */
public class UdfUrl extends UDF {

    private static final String REGEX = "[^0-9]";
    public String evaluate(String msg) {
        Pattern p = Pattern.compile(REGEX);
        Matcher m = p.matcher(msg);
        return m.replaceAll(" ").trim().replaceAll(" ", "_");
    }

}
