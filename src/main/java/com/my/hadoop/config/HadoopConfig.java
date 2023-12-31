package com.my.hadoop.config;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 @author 杨宇帆
 @create 2023-12-31
 */
@Configuration
public class HadoopConfig {
    @Bean
    public FileSystem fileSystem() throws Exception{
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        //配置副本数
        configuration.set("dfs.replication","2");
        return FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "hadoop");
    }
}
