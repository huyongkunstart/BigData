package com.hu.mapreduce.wordcount__2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author shkstart
 * @create 2022-03-14 13:16
 * @description：
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable outvalue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outvalue.set(sum);
        context.write(key,outvalue);
    }
}
