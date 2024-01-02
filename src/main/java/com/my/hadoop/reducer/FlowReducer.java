package com.my.hadoop.reducer;

import com.my.hadoop.writable.FlowBean;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 @author 杨宇帆
 @create 2024-01-02
 */
public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
    private FlowBean returnValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {
        long up = 0L;
        long down = 0L;
        for(FlowBean value:values){
            up+=value.getUpFlow();
            down+=value.getDownFlow();
        }

        returnValue.setUpFlow(up);
        returnValue.setDownFlow(down);
        returnValue.setTotalFlow();

        context.write(key,returnValue);
    }
}
