package com.hu.mapreduce.Map_join__11;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author shkstart
 * @create 2022-03-20 16:56
 * @description：
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<String,String>();
    private Text outkey = new Text();

    //获取缓存的文件pd.txt，并把文件内容封装到集合
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        //获取流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        //从流中读取数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            //切割
            String[] split = line.split("\t");

            //存入集合
            pdMap.put(split[0],split[1]);
        }
        //关流
        IOUtils.closeStream(reader);
    }

    //处理order.txt
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("\t");

        //获取pid
        String pname = pdMap.get(split[1]);

        //获取订单IP和订单数量
        outkey.set(split[0]+"\t"+pname+"\t"+split[2]);

        context.write(outkey,NullWritable.get());

    }
}
