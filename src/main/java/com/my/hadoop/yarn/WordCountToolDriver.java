package com.my.hadoop.yarn;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 @author 杨宇帆
 @create 2024-01-12
 */
public class WordCountToolDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        WordCountYarn wordCountYarn;
        switch (args[0]) {
            case "wordcount":
                wordCountYarn = new WordCountYarn();
                break;
            default:
                throw new RuntimeException("未找到类");
        }
        int run = ToolRunner.run(configuration, wordCountYarn, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}
