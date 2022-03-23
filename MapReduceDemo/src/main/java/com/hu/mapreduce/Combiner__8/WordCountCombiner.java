package com.hu.mapreduce.Combiner__8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-17 16:50
 * @description： 在map阶段进行分组操作
 */
//泛型为 map阶段输出的kv类型 要输出的kv类型
public class WordCountCombiner  extends Reducer<Text, IntWritable,Text,IntWritable> {
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
