package com.my.hadoop.driver;

import com.my.hadoop.mapper.FlowComparableMapper;
import com.my.hadoop.mapper.FlowMapper;
import com.my.hadoop.partition.FlowPartitioner;
import com.my.hadoop.reducer.FlowComparableReducer;
import com.my.hadoop.reducer.FlowReducer;
import com.my.hadoop.writable.FlowBean;
import com.my.hadoop.writable.FlowComparableBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 @author 杨宇帆
 @create 2024-01-02
 */
@Slf4j
public class FlowComparableDriver {
    /**
     * 格式比较死
     */
    public static void startDriver(String input,String output) throws Exception{
        //1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包路径
        job.setJarByClass(FlowComparableDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(FlowComparableMapper.class);
        job.setReducerClass(FlowComparableReducer.class);
        //4.设置mapper的kv
        job.setMapOutputKeyClass(FlowComparableBean.class);
        job.setMapOutputValueClass(Text.class);
        //5.设置最终输出的key
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowComparableBean.class);
        //6.设置输入和输出路径
        if(StringUtils.isEmpty(input)){
            input = "E:\\code\\hadoop\\src\\main\\resources\\data\\inputflow\\phone_data.txt";
        }
        if(StringUtils.isEmpty(output)){
            output = "E:\\code\\hadoop\\src\\main\\resources\\out"+ RandomUtils.nextLong(10000L,99999L);
        }

        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job, new Path(output));
        //7.提交job
        boolean result = job.waitForCompletion(true);

        log.info("-----------------------result:[{}]-------------------",result);
    }

    public static void main(String[] args) throws Exception{
        startDriver(null,null);
    }
}
