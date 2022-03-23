package com.hu.mapreduce.ETL_12;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-20 17:39
 * @description：
 */
public class WebLogMapper  extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //2.ETL
        boolean result = parseLog(line,context);

        if (!result){
            return;
        }

        //写出
        context.write(value,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {

        //切割
        String[] split = line.split(" ");

        //判断字段长度是否大于11
        if(split.length > 11){
            return true;
        }else {

            return false;
        }


    }
}
