package com.hu.mapreduce.partitioner__5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-14 22:37
 * @description：
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outkey = new Text();
    private FlowBean outvalue = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] split = line.split("\t");

        //抓取想要的数据  手机号 上行流量 下行流量
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length -2];

        //封装
        outkey.set(phone);
        outvalue.setUpFlow(Long.parseLong(up));
        outvalue.setDownFlow(Long.parseLong(down));
        outvalue.setSumFlow();

        context.write(outkey,outvalue);

    }



}
