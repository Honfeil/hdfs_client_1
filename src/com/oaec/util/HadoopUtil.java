package com.oaec.util;

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

public class HadoopUtil {
    private static URI uri;
    private static FileSystem fs;
    static {
        try {
            //根据hdfs的core-site.xml指定的ip和端口
            uri = new URI("hdfs://localhost:9000");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public static FileSystem getFS(){
        //创建Configuration对象
        Configuration cfg = new Configuration();
        //通过静态方法得到核心操作对象
        try {
            fs = FileSystem.get(uri, cfg, "hadoop");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }

}
