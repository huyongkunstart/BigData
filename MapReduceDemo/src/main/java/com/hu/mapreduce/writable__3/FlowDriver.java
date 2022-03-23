package com.hu.mapreduce.writable__3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-14 23:07
 * @description： 序列化和反序列化
 * 需求：统计每一个手机号耗费的总上行流量、总下行流量、总流量
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(FlowDriver.class);
        //3.关联mapper和reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置输入路径和输入路径
        FileInputFormat.setInputPaths(job,new Path("E:\\MapReduceDemoInput\\input\\inputflow"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\MapReduceDemoInput\\output\\outputflow"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
