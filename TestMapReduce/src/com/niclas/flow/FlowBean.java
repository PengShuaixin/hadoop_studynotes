package com.niclas.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//����FlowBean�࣬������Ӧһ���ֻ��Ŷ�Ӧ����������������������������������
public class FlowBean implements Writable {
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
	//���л��������л��������Ҫ���޲����Ĺ��캯��������û�з����л���������첻����
	public FlowBean() {
		
	}
	//�в������캯��
	public FlowBean(int upFlow, int downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow ;
	}
	//���л�
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upFlow);
		out.writeInt(downFlow);
		out.writeInt(sumFlow);
	}
	//�����л�
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readInt();
		this.downFlow = in.readInt();
		this.sumFlow = in.readInt();
	}
	//��дtoString����
	@Override
	public String toString() {
		return "FlowBean [upFlow=" + upFlow + "\t" + ", downFlow=" + downFlow + "\t" + ", sumFlow=" + sumFlow + "\t" + "]";
	}
	

}
