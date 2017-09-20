package com.oaec.bigData;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

//测试文件上船
public class TestToFS {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //创建Configuration对象
        Configuration cfg = new Configuration();
        //根据hdfs的core-site.xml指定的ip和端口
        URI uri = new URI("hdfs://localhost:9000");
        //通过静态方法得到核心操作对象
        FileSystem fs = FileSystem.get(uri, cfg, "hadoop");
        //文件创建（上传）
        FSDataOutputStream out = fs.create(new Path("/words.txt"));
        FileInputStream in = new FileInputStream(new File("/home/hadoop/document/words.txt"));
        IOUtils.copyBytes(in,out,2048,true);

    }
}
