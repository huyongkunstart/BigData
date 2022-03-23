package com.hu.mapreduce.writable__3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-14 22:56
 * @description：
 */
public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean outvalue = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //遍历集合累加值 values
        long totalUp = 0;
        long totaldown = 0;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totaldown += value.getDownFlow();
        }

        //封装 outkey outvalue
        outvalue.setUpFlow(totalUp);
        outvalue.setDownFlow(totaldown);
        outvalue.setSumFlow();

        //写出
        context.write(key,outvalue);

    }
}
