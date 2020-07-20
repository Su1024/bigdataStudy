package com.ruozedata.hadoop.towjob;

import com.ruozedata.hadoop.study02.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * @ClassName TowJobDriver
 * @Description
 * @Author suguoming
 * @Date 2020/7/18 4:00 下午
 *
 *
 */
public class TowJobDriver {
    public static void main(String[] args) throws Exception {
        String input = "data/wc";
        String output = "out";
        String output1 = "out1";

        // 1 获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        FileUtils.deleteOutput(configuration, output);
        FileUtils.deleteOutput(configuration, output1);

        // 2 设置主类
        job.setJarByClass(TowJobDriver.class);

        // 3 设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce阶段输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));


        // 1 获取Job
        Job job1 = Job.getInstance(configuration);

        // 2 设置主类
        job1.setJarByClass(TowJobDriver.class);

        // 3 设置Mapper和Reducer
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);

        // 4 设置Mapper阶段输出的key和value类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce阶段输出的key和value类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job1, new Path(output));
        FileOutputFormat.setOutputPath(job1, new Path(output1));

        boolean result = job.waitForCompletion(true);
        if (result){
            boolean b = job1.waitForCompletion(true);
        }
        System.exit(result? 0:1 );

    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");

            Random random = new Random(10);
            for (String word : splits) {
                String randomKey = random.nextInt(10) + "_" + word;
                context.write(new Text(randomKey), ONE);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }


    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String resKey = splits[0].split("_")[1];
            context.write(new Text(resKey), new IntWritable(Integer.parseInt(splits[1])));
        }
    }


    public static class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }


}
