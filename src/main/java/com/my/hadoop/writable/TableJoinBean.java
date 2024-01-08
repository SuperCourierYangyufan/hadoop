package com.my.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

/**
 @author 杨宇帆
 @create 2024-01-08
 */
@NoArgsConstructor
@Data
public class TableJoinBean implements Writable {

    private String id;

    private String pid;

    private Integer amount;

    private String pname;

    private String flag;


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }
}
