package com.hu.mapreduce.Map_join__11;

import com.hu.mapreduce.reduce_join__10.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author shkstart
 * @create 2022-03-20 12:11
 * @description： Mapper join

 * 在Map端缓存多张表，提前处理业务逻辑，这样增加Map端业务，减少Reduce端数据的压力，尽可能的减少数据倾斜。
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //1.获取job
        Configuration cong = new Configuration();
        Job job = Job.getInstance(cong);
        //2.关联jar包
        job.setJarByClass(MapJoinDriver.class);
        //3.关联map
        job.setMapperClass(MapJoinMapper.class);

        //4.设置map阶段输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //5.设置最终阶段输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);


        //加载缓存数据
        job.addCacheFile(new URI("file:///E:/MapReduceDemoInput/pd.txt"));
        //map端join的逻辑不需要Reduce阶段，设置Reduce task数量为 0
        job.setNumReduceTasks(0);

        //6.设置输出输出路径
        FileInputFormat.setInputPaths(job,new Path("E:\\MapReduceDemoInput\\input\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\MapReduceDemoInput\\output\\outputtable2"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
