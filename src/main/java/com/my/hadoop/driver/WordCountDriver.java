package com.my.hadoop.driver;

import com.my.hadoop.combiner.WordCountCombiner;
import com.my.hadoop.mapper.WordCountMapper;
import com.my.hadoop.reducer.WordCountReducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 @author 杨宇帆
 @create 2024-01-01
 */
@Slf4j
public class WordCountDriver{

    /**
     * 格式比较死
     * hadoop jar myjar/hadoop-0.0.1-SNAPSHOT.jar com.my.hadoop.driver.WordCountDriver /wcinput /wcoutput
     */
    public static void startDriver(String input,String output) throws Exception{
        //1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.设置mapper的kv
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设置最终输出的key
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * 设置合并
         */
        job.setCombinerClass(WordCountCombiner.class);

        //6.设置输入和输出路径
        if(StringUtils.isEmpty(input)){
            input = "E:\\code\\hadoop\\src\\main\\resources\\data\\inputword\\hello.txt";
        }
        if(StringUtils.isEmpty(output)){
            output = "E:\\code\\hadoop\\src\\main\\resources\\out"+ RandomUtils.nextLong(10000L, 99999L);
        }

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));
        //7.提交job
        boolean result = job.waitForCompletion(true);

        log.info("-----------------------result:[{}]-------------------",result);
    }

    public static void main(String[] args) throws Exception{
        startDriver(null,null);
    }
}
