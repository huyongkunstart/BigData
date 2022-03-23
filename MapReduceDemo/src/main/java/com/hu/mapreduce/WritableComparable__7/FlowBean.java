package com.hu.mapreduce.WritableComparable__7;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shkstart
 * @create 2022-03-17 11:30
 * @description：
 *
 * 需求：
 * 1.实现序列化和反序列化：实现接口 Writable
 * 2.按照总流量进行降序排序：实现接口 WritableComparable
 */
public class FlowBean implements Writable,WritableComparable<FlowBean> {
    private long FlowUp;
    private long FlowDown;
    private long FlowSum;

    public FlowBean() {
    }


    //总流量降序排序
    @Override
    public int compareTo(FlowBean o) {
         return -Long.compare(this.FlowSum,o.FlowSum);
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.FlowUp);
        dataOutput.writeLong(this.FlowDown);
        dataOutput.writeLong(this.FlowSum);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.FlowUp = dataInput.readLong();
        this.FlowDown = dataInput.readLong();
        this.FlowSum = dataInput.readLong();
    }

    @Override
    public String toString() {
        return FlowUp + "\t" + FlowDown + "\t" +  FlowSum;
    }

    //get set 方法

    public long getFlowUp() {
        return FlowUp;
    }

    public void setFlowUp(long flowUp) {
        FlowUp = flowUp;
    }

    public long getFlowDown() {
        return FlowDown;
    }

    public void setFlowDown(long flowDown) {
        FlowDown = flowDown;
    }

    public long getFlowSum() {
        return FlowSum;
    }

    public void setFlowSum(long flowSum) {
        FlowSum = flowSum;
    }
    public void setFlowSum(){
        FlowSum = this.FlowUp+this.FlowDown;
    }


}
