package com.my.hadoop.reducer;

import com.my.hadoop.writable.FlowBean;
import com.my.hadoop.writable.FlowComparableBean;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 @author 杨宇帆
 @create 2024-01-02
 */
public class FlowComparableReducer extends Reducer<FlowComparableBean, Text,Text, FlowComparableBean> {

    @Override
    protected void reduce(FlowComparableBean key, Iterable<Text> values,
                          Reducer<FlowComparableBean, Text, Text, FlowComparableBean>.Context context)
            throws IOException, InterruptedException {
        for(Text value:values){
            context.write(value,key);
        }
    }
}
