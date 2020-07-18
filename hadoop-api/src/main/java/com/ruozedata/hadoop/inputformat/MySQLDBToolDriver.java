package com.ruozedata.hadoop.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName MySQLDBToolDriver
 * @Description
 * @Author suguoming
 * @Date 2020/7/14 9:29 下午
 *
 *
 * export LIBJARS=/Users/sugm/dev/lib/mysql-connector-java-5.1.27.jar
 * export HADOOP_CLASSPATH=/Users/sugm/dev/lib/mysql-connector-java-5.1.27.jar
 * hadoop jar hadoop-api-1.0.0.jar com.ruozedata.hadoop.inputformat.MySQLDBToolDriver -libjars ${LIBJARS}
 *
 * 参考：http://grepalex.com/2013/02/25/hadoop-libjars/
 */
public class MySQLDBToolDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        DBConfiguration.configureDB(configuration,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/ruozedata",
                "root",
                "root");
        System.exit(ToolRunner.run(configuration, new MySQLDBToolDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        // 1 获取Job
        Job job = Job.getInstance(super.getConf());

        String output = "/out";

        // 2 设置主类
        job.setJarByClass(MySQLDBToolDriver.class);

        // 3 设置Mapper
        job.setMapperClass(MyMapper.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DeptWritable.class);


        // 6 设置输入和输出路径
        String[] fields = new String[]{"deptno", "dname", "loc"};
        DBInputFormat.setInput(job, DeptWritable.class, "dept", null, null, fields);

        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交Job
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }


    public static class MyMapper extends Mapper<LongWritable, DeptWritable, NullWritable, DeptWritable> {
        @Override
        protected void map(LongWritable key, DeptWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }
}
