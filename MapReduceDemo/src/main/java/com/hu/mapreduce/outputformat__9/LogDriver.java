package com.hu.mapreduce.outputformat__9;

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
 * @create 2022-03-18 10:22
 * @description：
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar包路径
        job.setJarByClass(LogDriver.class);

        //3.关联mapper和reduce
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //4.设置map输出的kv类型

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogOutputFormat.class);

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("E:\\MapReduceDemoInput\\input\\inputoutputformat"));
        //虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录

        FileOutputFormat.setOutputPath(job,new Path("E:\\MapReduceDemoInput\\output\\outputformat"));


        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
