package com.hu.mapreduce.WritableComparable__7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-17 11:30
 * @description：
 */
public class FlowMapper  extends Mapper<LongWritable, Text,FlowBean , Text> {
    private FlowBean outkey = new FlowBean();
    private Text outvalue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切分
        String[] split = line.split("\t");

        //获取
        outkey.setFlowUp(Long.parseLong(split[1]));
        outkey.setFlowDown(Long.parseLong(split[2]));
        outkey.setFlowSum();

        outvalue.set(split[0]);

        context.write(outkey,outvalue);

    }
}
