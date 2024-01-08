package com.my.hadoop.output;

import com.my.hadoop.customize.CustomizeWebRecordWriter;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
public class WebOutPutFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        CustomizeWebRecordWriter customizeWebRecordWriter = new CustomizeWebRecordWriter(job);
        return customizeWebRecordWriter;
    }
}
