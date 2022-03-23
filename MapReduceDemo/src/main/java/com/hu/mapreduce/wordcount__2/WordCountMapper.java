package com.hu.mapreduce.wordcount__2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-14 13:14
 * @description：
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text keyvalue = new Text();
    private IntWritable outvalue = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //分割
        String[] words = line.split(" ");
        for (String word : words) {
            keyvalue.set(word);
            context.write(keyvalue,outvalue);
        }

    }
}
