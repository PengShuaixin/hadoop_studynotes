package com.niclas.provinceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//定义FlowBean类，用来对应一个手机号对应的上行流量、下行流量，上下行总流量
public class FlowBean implements WritableComparable<FlowBean>{
	private int upflow;
	private int downFlow;
	private int sumFlow;
	//有参构造函数
//	public FlowBean(int upflow, int downFlow) {
//		super();
//		this.upflow = upflow;
//		this.downFlow = downFlow;
//		this.sumFlow = upflow + downFlow;
//	}
	//模仿系统提供的set方法赋值
	public void set(int upflow,int downFlow) {		
		this.upflow = upflow;
		this.downFlow = downFlow;
		this.sumFlow = upflow + downFlow;
	}
	public FlowBean() {
		
	}
	public int getUpflow() {
		return upflow;
	}
	public void setUpflow(int upflow) {
		this.upflow = upflow;
	}
	public int getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(int downFlow) {
		this.downFlow = downFlow;
	}
	public int getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(int sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	//序列化
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(upflow);
		out.writeInt(downFlow);
		out.writeInt(sumFlow);
	}
	
	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.upflow = in.readInt();
		this.downFlow = in.readInt();
		this.sumFlow = in.readInt();
		
	}
	@Override
	public int compareTo(FlowBean o) {
		// TODO Auto-generated method stub
		return this.sumFlow > o.sumFlow ?-1:1;
	}

	//重写toString方法
	@Override
	public String toString() {
		return  upflow + "\t" + downFlow + "\t" + sumFlow;
	}

	
}
