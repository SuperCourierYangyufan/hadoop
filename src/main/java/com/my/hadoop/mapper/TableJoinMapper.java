package com.my.hadoop.mapper;

import cn.hutool.core.collection.LineIter;
import cn.hutool.core.io.IoUtil;
import com.google.common.collect.Maps;
import com.my.hadoop.writable.TableJoinBean;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
public class TableJoinMapper extends Mapper<LongWritable, Text, Text, TableJoinBean> {

    private String fileName;

    private TableJoinBean returnValue = new TableJoinBean();

    private Text returnKey = new Text();

    HashMap<String, String> map = Maps.newHashMap();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableJoinBean>.Context context)
            throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //读缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        FSDataInputStream pdInput = fileSystem.open(new Path(cacheFiles[0]));
        //读数据
        LineIter lineIter = IoUtil.lineIter(pdInput, StandardCharsets.UTF_8);
        while (lineIter.hasNext()) {
            String line = lineIter.next();
            String[] split = line.split("\t");
            map.put(split[0], split[1]);
        }
        //关流
        IoUtil.close(lineIter);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableJoinBean>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        returnKey.set(split[1]);
        returnValue.setId(split[0]);
        returnValue.setPid(split[1]);
        returnValue.setPname(map.get(split[1]));
        returnValue.setAmount(Integer.valueOf(split[2]));
        context.write(returnKey,returnValue);
    }

    /**
     * 初始化执行一次
     */
//    @Override
//    protected void setup(Mapper<LongWritable, Text, Text, TableJoinBean>.Context context)
//            throws IOException, InterruptedException {
//        //获取切分信息
//        FileSplit inputSplit = (FileSplit) context.getInputSplit();
//        //获取文件名
//        fileName = inputSplit.getPath().getName();
//    }

//    @Override
//    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableJoinBean>.Context context)
//            throws IOException, InterruptedException {
//        String line = value.toString();
//        String[] split = line.split("\t");
//        if (fileName.contains("order")) {
//            returnKey.set(split[1]);
//            returnValue.setId(split[0]);
//            returnValue.setPid(split[1]);
//            returnValue.setPname("");
//            returnValue.setAmount(Integer.valueOf(split[2]));
//        } else {
//            returnKey.set(split[0]);
//            returnValue.setId("");
//            returnValue.setPid(split[0]);
//            returnValue.setPname(split[1]);
//            returnValue.setAmount(0);
//        }
//        returnValue.setFlag(fileName);
//        context.write(returnKey,returnValue);
//    }
}
