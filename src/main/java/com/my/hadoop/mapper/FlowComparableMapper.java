package com.my.hadoop.mapper;

import com.my.hadoop.writable.FlowBean;
import com.my.hadoop.writable.FlowComparableBean;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 @author 杨宇帆
 @create 2024-01-07
 */
public class FlowComparableMapper extends Mapper<LongWritable, Text, FlowComparableBean,Text> {
    private Text returnValue = new Text();
    private FlowComparableBean returnKey = new FlowComparableBean();

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, FlowComparableBean, Text>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split("\t");

        String phone = split[1];

        String up = split[split.length-3];

        String down = split[split.length-2];

        returnValue.set(phone);
        returnKey.setUpFlow(Long.valueOf(up));
        returnKey.setDownFlow(Long.valueOf(down));
        returnKey.setTotalFlow();

        context.write(returnKey,returnValue);
    }
}
