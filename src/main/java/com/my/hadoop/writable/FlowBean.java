package com.my.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

/**
 @author 杨宇帆
 @create 2024-01-02
 */
@Data
@NoArgsConstructor //空参构造
@AllArgsConstructor
public class FlowBean implements Writable { //实现Writable

    private Long upFlow;

    private Long downFlow;

    private Long totalFlow;

    public void setTotalFlow() {
        totalFlow = upFlow + downFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(totalFlow);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        totalFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", totalFlow=" + totalFlow +
                '}';
    }
}
