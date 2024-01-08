package com.my.hadoop.mapper;

import com.my.hadoop.writable.TableJoinBean;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
public class TableJoinMapper extends Mapper<LongWritable, Text, Text, TableJoinBean> {

    private String fileName;

    private TableJoinBean returnValue = new TableJoinBean();

    private Text returnKey = new Text();

    /**
     * 初始化执行一次
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableJoinBean>.Context context)
            throws IOException, InterruptedException {
        //获取切分信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //获取文件名
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableJoinBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        if (fileName.contains("order")) {
            returnKey.set(split[1]);
            returnValue.setId(split[0]);
            returnValue.setPid(split[1]);
            returnValue.setPname("");
            returnValue.setAmount(Integer.valueOf(split[2]));
        } else {
            returnKey.set(split[0]);
            returnValue.setId("");
            returnValue.setPid(split[0]);
            returnValue.setPname(split[1]);
            returnValue.setAmount(0);
        }
        returnValue.setFlag(fileName);
        context.write(returnKey,returnValue);
    }
}
