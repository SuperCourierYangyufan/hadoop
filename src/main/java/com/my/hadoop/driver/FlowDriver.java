package com.my.hadoop.driver;

import com.my.hadoop.HadoopApplication;
import com.my.hadoop.mapper.FlowMapper;
import com.my.hadoop.mapper.WordCountMapper;
import com.my.hadoop.partition.FlowPartitioner;
import com.my.hadoop.reducer.FlowReducer;
import com.my.hadoop.reducer.WordCountReducer;
import com.my.hadoop.writable.FlowBean;
import java.io.File;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
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
public class FlowDriver {
    /**
     * 格式比较死
     */
    public static void startDriver(String input,String output) throws Exception{
        //1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包路径
        job.setJarByClass(FlowDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.设置mapper的kv
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终输出的key
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入和输出路径
        if(StringUtils.isEmpty(input)){
            input = "E:\\code\\hadoop\\src\\main\\resources\\data\\inputflow\\phone_data.txt";
        }
        if(StringUtils.isEmpty(output)){
            output = "E:\\code\\hadoop\\src\\main\\resources\\out"+ RandomUtils.nextLong(10000L,99999L);
        }
        /**
         * -----------------如果不设置哦InputFormat,默认采用TextInputFormat----------------------
         */
        job.setInputFormatClass(CombineTextInputFormat.class);
        /**
         * 调整为4M 为一个map
         */
        CombineFileInputFormat.setMaxInputSplitSize(job,4 * 1024 * 1024);

        /**
         * 自定义分区和分区个数
         */
        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(5);


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
