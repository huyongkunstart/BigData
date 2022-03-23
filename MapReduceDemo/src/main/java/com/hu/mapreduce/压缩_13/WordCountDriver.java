
package com.hu.mapreduce.压缩_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-13 15:56
 * @description： 开启 map 端输出压缩
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //Map输出端压缩
        // 开启 map 端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置 map 端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


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


        //6.设置输入路径和输出路径
        //此程序并没有调用linux的集群，而是使用的maven包本地运行
        FileInputFormat.setInputPaths(job, new Path("E:\\MapReduceDemoInput\\input\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\MapReduceDemoInput\\output\\outputword"));

        // 设置 reduce 端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        // FileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);


        //7.提交job
//        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
