package com.niclas.flow2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//定义FlowBean类，用来对应一个手机号对应的上行流量，下行流量，上下行总流量
public class JoinBean implements WritableComparable<JoinBean> {
	private int upFlow;
	private int downFlow;
	private int sumFlow;
	public int getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
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
	//序列化、反序列化的类必须要有无参数的构造函数，如若没有反序列化创造对象创造不出来
	public JoinBean() {
		
	}
	//有参数构造函数
//	public FlowBean(int upFlow, int downFlow) {
//		super();
//		this.upFlow = upFlow;
//		this.downFlow = downFlow;
//		this.sumFlow = upFlow + downFlow ;
//	}
	//模仿系统提供的set方法赋值
	public void set(int upFlow, int downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow ;
	}
	//序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upFlow);
		out.writeInt(downFlow);
		out.writeInt(sumFlow);
	}
	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readInt();
		this.downFlow = in.readInt();
		this.sumFlow = in.readInt();
	}
	@Override
	public int compareTo(JoinBean o) {
		
		return this.sumFlow > o.sumFlow ? -1:1;
		//return this.sumFlow - o.sumFlow;
	}
	//重写toString方法
	@Override
	public String toString() {
		return "FlowBean [upFlow=" + upFlow + "\t" + ", downFlow=" + downFlow + "\t" + ", sumFlow=" + sumFlow + "\t" + "]";
	}
	
}
