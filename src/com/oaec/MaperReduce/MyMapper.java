package com.oaec.MaperReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//拆分任务的mapper后面的参数：（输入键名值类型）（输入键值值类型）（输出键名值类型）（输出键值值类型）
public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * map的上下文
     * 上文：HDFS
     * 下文：Reducer
     */

    public void map(LongWritable key1, Text value1, Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
        //获取hdfs分配的待处理信息
        String info = value1.toString();
        //对获取的信息做简单分词，按空格分词
        String[] words = info.split(" ");
        //将拆分的单词放在上下文中
        try {
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
