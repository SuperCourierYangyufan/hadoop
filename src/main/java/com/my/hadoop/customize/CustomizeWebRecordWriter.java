package com.my.hadoop.customize;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
@Slf4j
public class CustomizeWebRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream guiguStream;

    private FSDataOutputStream otherStream;

    /**
     * 初始化事情
     * @param job
     */
    public CustomizeWebRecordWriter(TaskAttemptContext job) {
        try {
            /**
             * 必须使用原先job的配置
             */
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            guiguStream = fileSystem.create(
                    new Path("E:\\code\\hadoop\\src\\main\\resources\\out\\guigu.txt"));

            otherStream = fileSystem.create(new Path("E:\\code\\hadoop\\src\\main\\resources\\out\\other.txt"));
        } catch (Exception e) {
            log.error("CustomizeWebRecordWriter.CustomizeWebRecordWriter error", e);
        }
    }

    /**
     * 自定义逻辑,可以是DB,ES
     * @param text
     * @param nullWritable
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().contains("atguigu")) {
            guiguStream.writeBytes(text.toString());
        } else {
            otherStream.writeBytes(text.toString()+"\n");
        }
    }

    /**
     * 关闭
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(guiguStream);
        IOUtils.closeStream(otherStream);
    }
}
