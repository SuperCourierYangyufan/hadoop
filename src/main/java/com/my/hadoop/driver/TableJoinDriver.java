package com.my.hadoop.driver;

import com.my.hadoop.mapper.TableJoinMapper;
import com.my.hadoop.mapper.WebMapper;
import com.my.hadoop.output.WebOutPutFormat;
import com.my.hadoop.reducer.TableJoinReducer;
import com.my.hadoop.reducer.WebReducer;
import com.my.hadoop.writable.TableJoinBean;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
@Slf4j
public class TableJoinDriver {
    /**
     * 格式比较死
     */
    public static void startDriver(String input,String output) throws Exception{
        //1.获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包路径
        job.setJarByClass(TableJoinDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(TableJoinMapper.class);
        job.setReducerClass(TableJoinReducer.class);
        //4.设置mapper的kv
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableJoinBean.class);
        //5.设置最终输出的key
        job.setOutputKeyClass(TableJoinBean.class);
        job.setOutputValueClass(NullWritable.class);
        //6.设置输入和输出路径
        if(StringUtils.isEmpty(input)){
            input = "E:\\code\\hadoop\\src\\main\\resources\\data\\inputtable\\order.txt";
        }
        if(StringUtils.isEmpty(output)){
            output = "E:\\code\\hadoop\\src\\main\\resources\\out"+ RandomUtils.nextLong(10000L, 99999L);
        }
        /**
         * 加载缓存
         */
        job.addCacheFile(new URI("file:///E:/code/hadoop/src/main/resources/data/inputtable/pd.txt"));
        /**
         * 取消Reduce
         */
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output));
        //7.提交job
        boolean result = job.waitForCompletion(true);

        log.info("-----------------------result:[{}]-------------------",result);
    }

    public static void main(String[] args) throws Exception{
        startDriver(null,null);
    }

}
