package com.oaec.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class CommHadoop {

    public void upLoad(File file,Path path){
        FileSystem fs = HadoopUtil.getFS();
        //文件创建（上传）
        FSDataOutputStream out = null;
        try {
            out = fs.create(path);
            FileInputStream in = new FileInputStream(file);
            IOUtils.copyBytes(in,out,2048,true);
            in.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
