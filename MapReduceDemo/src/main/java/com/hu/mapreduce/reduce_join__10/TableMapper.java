package com.hu.mapreduce.reduce_join__10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-20 10:48
 * @description：
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String filename;
    private Text outkey = new Text();
    private TableBean outvalue = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //判断是哪个文件
        if (filename.contains("order")){ //处理的是订单表
            String[] split = line.split("\t");
            outkey.set(split[1]);
            outvalue.setId(split[0]);
            outvalue.setPid(split[1]);
            outvalue.setAmount(Integer.parseInt(split[2]));
            outvalue.setPname("");
            outvalue.setFlag("order");

        }else { //处理的是商品表
            String[] split = line.split("\t");

            outkey.set(split[0]);

            outvalue.setId(" ");
            outvalue.setPid(split[0]);
            outvalue.setAmount(0);
            outvalue.setPname(split[1]);
            outvalue.setFlag("pd");
        }
        //封装kv
        context.write(outkey,outvalue);
    }
}
