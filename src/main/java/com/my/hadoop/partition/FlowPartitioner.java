package com.my.hadoop.partition;

import com.my.hadoop.writable.FlowBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 @author 杨宇帆
 @create 2024-01-07
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneStart = text.toString().substring(0, 3);
        if ("136".equals(phoneStart)) {
            return 0;
        } else if ("137".equals(phoneStart)) {
            return 1;
        } else if ("138".equals(phoneStart)) {
            return 2;
        } else if ("139".equals(phoneStart)) {
            return 3;
        } else {
            return 4;
        }

    }
}
