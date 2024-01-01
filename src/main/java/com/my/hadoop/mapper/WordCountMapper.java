package com.my.hadoop.mapper;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;  // 注意类型
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 @author 杨宇帆
 @create 2024-01-01
 * LongWritable, Text,Text, IntWritable
 * 偏移量        每行文本 单词  数量
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    /**
     * 提取出来,map()方法会调用无数次,每次都要创建对象会浪费内存
     */
    private Text text = new Text();
    private IntWritable out = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //拿取每行数据,空格分割
        String[] words = value.toString().split("\\s");
        List<String> list = Lists.newArrayList(words).stream()
                .filter(word -> !StringUtils.isEmpty(word))
                .collect(Collectors.toList());
        for(String word:list){
            //key每次修改值
            text.set(word);
            //返回当前行
            context.write(text,out);
        }
    }

}
