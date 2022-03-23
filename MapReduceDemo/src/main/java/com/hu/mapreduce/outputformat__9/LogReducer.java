package com.hu.mapreduce.outputformat__9;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-18 9:58
 * @description：
 */
public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //防止有相同数据，丢数据
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
