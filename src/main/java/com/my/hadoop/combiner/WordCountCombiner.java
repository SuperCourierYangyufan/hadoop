package com.my.hadoop.combiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 @author 杨宇帆
 @create 2024-01-07
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable returnValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum++;
        }
        returnValue.set(sum);
        context.write(key, returnValue);
    }
}
