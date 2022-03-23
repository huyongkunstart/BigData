package com.hu.mapreduce.WritableComparable__7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-17 11:29
 * @description： WritableComparable 排序案例实操（区内排序）
 * 需求：要求每个省份手机号输出的文件中按照总流量内部排序。
 * 需求分析
 * 	    基于前一个需求，增加自定义分区类，分区按照省份手机号设置。
 */
public class FLowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(FLowDriver.class);
        //3.关联mapper和reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);
        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //5.设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //引入分区类
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("E:\\MapReduceDemoInput\\input\\inputWritableComparable"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\MapReduceDemoInput\\output\\outputWritableComparable2"));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
