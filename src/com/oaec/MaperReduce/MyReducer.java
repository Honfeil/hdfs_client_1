package com.oaec.MaperReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//拆分任务的mapper后参数：（输入键名值类型-hdfs文件信息位置偏移量）（键入值值类型）（输出键名值类型）（输出键值值类型）
public class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    /**
     * reducer上下文context
     * 上文 mapper
     * 下文 hdfs
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws IOException, InterruptedException {
        long total=0;
        //从values中的集和类对象中获取值进行累加
        for (LongWritable v:values){
            total+=v.get();
        }
        //输出到上下文中，最终写入到hdfs中
        context.write(key,new LongWritable(total));
    }
}
