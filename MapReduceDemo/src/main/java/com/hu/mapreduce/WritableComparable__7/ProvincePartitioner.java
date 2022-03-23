package com.hu.mapreduce.WritableComparable__7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author shkstart
 * @create 2022-03-17 11:59
 * @description： 分区： 实现接口
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();
        String str = phone.substring(0, 3);
        int partition;
        if ("136".equals(str)){
            partition = 0;
        }else if ("137".equals(str)){
            partition = 1;
        }else if ("138".equals(str)){
            partition = 2;
        }else if ("139".equals(str)){
            partition = 3;
        }else {
            partition = 4;
        }

        return partition;

    }
}
