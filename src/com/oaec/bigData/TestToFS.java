package com.oaec.bigData;


import com.oaec.util.CommHadoop;
import com.oaec.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

//测试文件上传
public class TestToFS{
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        CommHadoop hdfs = new CommHadoop();
        //文件上传
        File file = new File("/home/hadoop/document/words.txt");
        Path path = new Path("/words.txt");
        hdfs.upLoad(file,path);


        //文件检测存在
        FileSystem fs = HadoopUtil.getFS();
        //boolean e = fs.exists(path);
        //System.out.printf(e?"文件存在":"文件不存在");

        //修改hdfs中文件的名称
        //fs.rename(path,new Path("/myWords.txt"));

        //下载hdfs中的文件
        //File local = new File("/home/hadoop/test/download.txt");
        Path primPath = new Path("/words");
        //FSDataInputStream open = fs.open(primPath);
        //IOUtils.copyBytes(open,new FileOutputStream(local),1024,false);
        //IOUtils.closeStream(open);

        //删除hdfs中的文件
        //boolean rs = fs.delete(primPath, true);
        //System.out.printf(rs?"文件已被删除":"文件删除失败");


    }
}
