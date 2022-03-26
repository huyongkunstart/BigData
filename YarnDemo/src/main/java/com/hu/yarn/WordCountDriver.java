package com.hu.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * @author shkstart
 * @create 2022-03-25 23:17
 * @description：
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        // 1. 创建配置文件
        Configuration conf = new Configuration();

        // 2. 判断是否有tool接口
        WordCount tool;
        switch (args[0]){
            case "wordcount":
                tool = new WordCount();
                break;
            default:
                throw new RuntimeException(" No such tool: "+ args[0] );
        }
        // 3. 用Tool执行程序
        // Arrays.copyOfRange 将老数组的元素放到新数组里面
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));

        System.exit(run);

    }
}
