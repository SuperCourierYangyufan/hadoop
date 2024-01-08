package com.my.hadoop.reducer;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
public class WebReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values,
                          Reducer<Text, NullWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, value);
        }
    }
}
