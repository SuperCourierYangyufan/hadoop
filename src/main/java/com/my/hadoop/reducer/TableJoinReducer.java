package com.my.hadoop.reducer;

import cn.hutool.core.bean.BeanUtil;
import com.google.common.collect.Lists;
import com.my.hadoop.writable.TableJoinBean;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
public class TableJoinReducer extends Reducer<Text, TableJoinBean, TableJoinBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableJoinBean> values,
                          Reducer<Text, TableJoinBean, TableJoinBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        ArrayList<TableJoinBean> list = Lists.newArrayList();

        TableJoinBean tableJoinBean = new TableJoinBean();

        for (TableJoinBean value : values) {
            /**
             * hadoop对values 进行了重写,不是简单的集合,而是地址集合,且后面的会把前面覆盖,所以对象必须深拷贝
             *
             */
            if (value.getFlag().contains("order")) {
                TableJoinBean tableBean = BeanUtil.copyProperties(value, TableJoinBean.class);
                list.add(tableBean);
            } else {
                BeanUtil.copyProperties(value, tableJoinBean);
            }
        }

        for (TableJoinBean joinBean : list) {
            joinBean.setPname(tableJoinBean.getPname());
            context.write(joinBean,NullWritable.get());
        }
    }
}
