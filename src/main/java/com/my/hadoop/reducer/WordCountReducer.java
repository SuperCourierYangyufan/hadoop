package com.my.hadoop.reducer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 @author 杨宇帆
 @create 2024-01-01
 * Text, IntWritable,Text, IntWritable
 * 输入和mapper一致
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    /**
     * 提取
     */
    private IntWritable count = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //返回为 key:yangyufan value:[1,1,1,1,1,1]
        int sum = 0;
        for(IntWritable value:values){
            sum+=value.get();
        }
        count.set(sum);
        context.write(key,count);
    }
}
