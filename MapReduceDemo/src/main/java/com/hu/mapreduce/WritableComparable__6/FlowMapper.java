package com.hu.mapreduce.WritableComparable__6;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    private FlowBean outkey = new FlowBean();
    private Text outvalue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //
        String[] split = line.split("\t");

        outkey.setUpFlow(Long.parseLong(split[1]));
        outkey.setDownFlow(Long.parseLong(split[2]));
        outkey.setSumFlow();

        outvalue.set(split[0]);

        context.write(outkey,outvalue);
    }
}