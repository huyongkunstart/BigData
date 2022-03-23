package com.hu.mapreduce.ETL_12;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-20 17:40
 * @description：
 */
public class WebLogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联jar包
        job.setJarByClass(WebLogDriver.class);
        //3.关联map 不需要reduce阶段
        job.setMapperClass(WebLogMapper.class);
        job.setNumReduceTasks(0);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.设置最终的输出的kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("E:\\MapReduceDemoInput\\input\\inputETL"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\MapReduceDemoInput\\output\\outputETL"));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
