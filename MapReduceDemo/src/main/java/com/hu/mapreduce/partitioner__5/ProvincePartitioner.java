package com.hu.mapreduce.partitioner__5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author shkstart
 * @create 2022-03-15 21:55
 * @description： Partition分区
 */

//泛型参数： map阶段输出的kv类型
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //text 手机号
        String phone = text.toString();
        //获取手机号前三位
        String prePhoen = phone.substring(0, 3);

        int partition; //分区
        if ("136".equals(prePhoen)){
            partition = 0;
        }else if ("137".equals(prePhoen)){
            partition =1;
        }else if ("138".equals(prePhoen)){
            partition =2;
        }else if ("139".equals(prePhoen)){
            partition =3;
        }else {
            partition = 4;
        }

        return partition;
    }
}
