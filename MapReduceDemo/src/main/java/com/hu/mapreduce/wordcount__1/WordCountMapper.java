package com.hu.mapreduce.wordcount__1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-13 15:56
 * @description：
 */

/**
 * KEYIN,  map阶段输入的key的类型（偏移量）：LongWritable
 * VALUEIN, map阶段输入value类型：Text（String）
 * KEYOUT, map阶段输出的key类型：Text
 * VALUEOUT map阶段输出的value类型： IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outkey = new Text();
    private IntWritable outvalue = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();

        //2.对字符串切割
        String[] words = line.split(" ");

        //3.循环写出
        for (String word : words){
            outkey.set(word);
            context.write(outkey,outvalue);
        }
    }
}
