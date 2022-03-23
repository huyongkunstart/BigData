
package com.hu.mapreduce.combineTextInputformat__4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-13 15:56
 * @description： CombineTextInputFormat 切片机制
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //3.关联mapper和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置 InputFormat，它默认用的是 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置 4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //6.设置输入路径和输出路径
        //此程序并没有调用linux的集群，而是使用的maven包本地运行
        FileInputFormat.setInputPaths(job, new Path("E:\\MapReduceDemoInput\\input\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\MapReduceDemoInput\\output\\outputcombinetextinputformat"));

        //7.提交job
//        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
