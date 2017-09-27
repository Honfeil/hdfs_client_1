package com.oaec.MaperReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建任务
        Job job = Job.getInstance(new Configuration(),"myJob");
        //指定任务入口
        job.setJarByClass(MyJob.class);
        //设置任务的mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置任务的reducer
        job.setReducerClass(MyReducer.class);
        //设置任务的输入输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定输入输出
        //设置mapReduce整个的输入输出
        FileInputFormat.addInputPath(job,new Path("/home/hadoop/document/words.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/home/hadoop/test/count"));
        //提交运行任务，true-打印日志
        job.waitForCompletion(true);

    }
}
