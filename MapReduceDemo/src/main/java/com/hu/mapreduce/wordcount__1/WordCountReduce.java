package com.hu.mapreduce.wordcount__1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-13 15:56
 * @description：
 */

/**
 * KEYIN,  reduce阶段输入的key的类型：LongWritable
 * VALUEIN, reduce阶段输入value类型：Text（String）
 * KEYOUT, reduce阶段输出的key类型：Text
 * VALUEOUT reduce阶段输出的value类型： IntWritable
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outvalue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum =0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outvalue.set(sum);
        //写出
        context.write(key,outvalue);

    }
}
