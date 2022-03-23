package com.hu.mapreduce.reduce_join__10;

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
 * @create 2022-03-20 12:11
 * @description： Reduce join
 *
 * 通过将关联条件作为Map输出的key，将两表满足Join条件的数据并携带数据所来源的文件信息，发往同一个ReduceTask，在Reduce中进行数据的串联
 */
public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration cong = new Configuration();
        Job job = Job.getInstance(cong);
        //2.关联jar包
        job.setJarByClass(TableDriver.class);
        //3.关联map和reduce类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce.class);
        //4.设置map阶段输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        //5.设置最终阶段输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        //6.设置输出输出路径
        FileInputFormat.setInputPaths(job,new Path("E:\\MapReduceDemoInput\\input\\inputtable"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\MapReduceDemoInput\\input\\outputtable"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
