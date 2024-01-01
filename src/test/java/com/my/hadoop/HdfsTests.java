package com.my.hadoop;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Slf4j
class HdfsTests {

    @Autowired
    private FileSystem fs;

    @Test
    void fun1() throws Exception {
        fs.mkdirs(new Path("/xiyou/tianting"));
    }

    /**
     * 上传
     * @throws Exception
     */
    @Test
    void fun2() throws Exception {
        //  1 删除原数据   2 是否覆盖   3 原路径 4 目标路径
        fs.copyFromLocalFile(false, true, new Path("E:\\down\\新建文本文档.txt"), new Path("/xiyou/huaguoshan"));
    }


    /**
     * 调整副本
     * @throws Exception
     */
    @Test
    void fun3() throws Exception {
        fs.setReplication(new Path("/xiyou/tianting/新建文本文档.txt"), new Short("1"));
    }

    /**
     * 文件下载
     * @throws Exception
     */
    @Test
    void fun4() throws Exception {
        //  1 删除原数据  2 原路径 3 目标路径  4 不开启校验
        fs.copyToLocalFile(false, new Path("/xiyou/tianting/新建文本文档.txt"),
                           new Path("C:\\Users\\Administrator\\Desktop\\文本.txt"),true);
    }


    /**
     * 删除
     * @throws Exception
     */
    @Test
    void fun5() throws Exception {
        // 1.删除路径  2.递归删除
        fs.delete(new Path("/xiyou/tianting/"),true);
        /**
         * 其余如
         * fs.rename 改名和改路径 == mv
         *
         */
    }


    /**
     * 获取文件信息
     * @throws Exception
     */
    @Test
    void fun6() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()){
            LocatedFileStatus next = iterator.next();
            log.info("----------------------------------");
            log.info(JSONObject.toJSONString(next.getPermission()));
            log.info(JSONObject.toJSONString(next.getOwner()));
            log.info(JSONObject.toJSONString(next.getGroup()));
            log.info(JSONObject.toJSONString(next.getLen()));
            log.info(JSONObject.toJSONString(next.getModificationTime()));
            log.info(JSONObject.toJSONString(next.getReplication()));
            log.info(JSONObject.toJSONString(next.getBlockSize()));
            log.info(JSONObject.toJSONString(next.getPath().getName()));
            log.info(JSONObject.toJSONString(next.getBlockLocations()));
            log.info("----------------------------------");
        }
    }



    /**
     * 判断一个目录下是文件还是目录
     * @throws Exception
     */
    @Test
    void fun7() throws Exception {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatus:fileStatuses){
            if(fileStatus.isDirectory()){
                log.info(fileStatus.getPath().getName()+":目录");
            }else{
                log.info(fileStatus.getPath().getName()+":文件");
            }

        }

    }


}

