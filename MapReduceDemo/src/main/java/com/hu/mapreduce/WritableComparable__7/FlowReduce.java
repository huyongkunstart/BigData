package com.hu.mapreduce.WritableComparable__7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-17 11:30
 * @description：
 */
public class FlowReduce extends Reducer<FlowBean ,Text , Text , FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
