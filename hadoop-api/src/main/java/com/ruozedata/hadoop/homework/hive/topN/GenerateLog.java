package com.ruozedata.hadoop.homework.hive.topN;

import java.io.*;
import java.text.MessageFormat;
import java.util.Random;

/**
 * @ClassName GenerateLog
 * @Description
 * @Author suguoming
 * @Date 2020/7/20 10:37 上午
 */
public class GenerateLog {
    public static void main(String[] args) throws Exception {
//        generateQuestion();
        generateCourse();

    }

    private static void generateCourse() throws IOException {
        String[] fileContent = new String[]{"http://ruozedata.com/course/{0}.html", "http://ruozedata.com/course/{0}/{1}.html?a=b&c=d   "};
        String logFormat = "http://ruozedata.com/question/{0}";
        long fileSize = 70 * 1024 * 1024;
        long rowNum = fileSize / logFormat.length();
        Random random = new Random();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("data/course.log"));
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        for (int i = 0; i < rowNum; i++) {
            int i1 = i % 2;
            String format = MessageFormat.format(fileContent[i1], random.nextInt(200), random.nextInt(3));
            bufferedWriter.write(format);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    private static void generateQuestion() throws IOException {
        String logFormat = "http://ruozedata.com/question/{0}";
        long fileSize = 70 * 1024 * 1024;
        long rowNum = fileSize / logFormat.length();
        Random random = new Random();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("data/question.log"));
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        for (int i = 0; i < rowNum; i++) {
            String format = MessageFormat.format(logFormat, random.nextInt(200));
            bufferedWriter.write(format);
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }


}
