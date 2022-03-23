package com.hu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author shkstart
 * @create 2022-03-08 17:19
 * @description：
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException {
        //连接的集群nn的地址
        URI uri = new URI("hdfs://bigdata101:9000");
        //创建一个配置文件
        Configuration con = new Configuration();
        //用户
        String user = "hu";
        //获取客户端对象
        fs = FileSystem.get(uri, con, user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }


    //创建目录
    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException {

        fs.mkdirs(new Path("/user/"));
    }

    //上传
    @Test
    public void test2() throws IOException {
        //参数一：是否删除原数据
        //参数二：是否允许覆盖
        //参数三：原数据路径
        //参数四：目的地路径

        fs.copyFromLocalFile(false, false, new Path("E:\\Desktop\\input"), new Path("hdfs://bigdata101:9000/"));
    }

    //下载
    @Test
    public void test3() throws IOException {
        //参数一：原文件是否删除
        //参数二：原文件路径HDFS
        //参数三：目标地址路径windows
        //参数四：是否开启校验
        fs.copyToLocalFile(false, new Path("hdfs://bigdata101:9000/user"), new Path("E:\\Desktop"), false);
    }

    //删除
    @Test
    public void test4() throws IOException {
        //参数一：要删除的路径   参数二：是否递归删除
        fs.delete(new Path("hdfs://bigdata101:9000/user"), true);
    }

    //文件的更名和移动
    @Test
    public void test5() throws IOException {
        //参数一：原文件路径

        //参数二：目标文件路径
        fs.rename(new Path("/你好.txt"), new Path("/user/nihao.txt"));
    }

    //获取文件详细信息
    @Test
    public void test6() throws IOException {
        //获取所有文件 参数二：是否递归
        //listFiles 返回一个可迭代对象
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        //遍历文件
        while (files.hasNext()) {
            LocatedFileStatus next = files.next();
            System.out.println("=======================文件名:" + next.getPath().getName() + "=======================");
            System.out.println("文件路径:" + next.getPath());
            System.out.println("文件权限:" + next.getPermission());
            System.out.println("文件拥有者:" + next.getOwner());
            System.out.println("文件组:" + next.getGroup());
            System.out.println("文件大小:" + next.getLen());
            System.out.println("文件上次修改时间:" + next.getModificationTime());
            System.out.println("文件副本:" + next.getReplication());
            System.out.println("文件块大小:" + next.getBlockSize());

            //获取块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //判断是文件夹还是文件
    @Test
    public void test7() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }
    }


}
