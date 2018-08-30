package com.niclas.provinceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//����FlowBean�࣬������Ӧһ���ֻ��Ŷ�Ӧ����������������������������������
public class FlowBean implements WritableComparable<FlowBean>{
	private int upflow;
	private int downFlow;
	private int sumFlow;
	//�вι��캯��
//	public FlowBean(int upflow, int downFlow) {
//		super();
//		this.upflow = upflow;
//		this.downFlow = downFlow;
//		this.sumFlow = upflow + downFlow;
//	}
	//ģ��ϵͳ�ṩ��set������ֵ
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
	
	//���л�
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(upflow);
		out.writeInt(downFlow);
		out.writeInt(sumFlow);
	}
	
	//�����л�
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

	//��дtoString����
	@Override
	public String toString() {
		return  upflow + "\t" + downFlow + "\t" + sumFlow;
	}

	
}
