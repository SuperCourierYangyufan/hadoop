package com.my.hadoop.mapper;

import com.my.hadoop.writable.FlowBean;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 @author 杨宇帆
 @create 2024-01-02
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    private Text returnKey = new Text();
    private FlowBean returnValue = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        String phone = split[1];

        String up = split[split.length-3];

        String down = split[split.length-2];

        returnKey.set(phone);
        returnValue.setUpFlow(Long.valueOf(up));
        returnValue.setDownFlow(Long.valueOf(down));
        returnValue.setTotalFlow();

        context.write(returnKey,returnValue);
    }
}
