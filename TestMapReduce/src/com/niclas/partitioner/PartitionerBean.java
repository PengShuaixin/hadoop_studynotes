package com.niclas.partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PartitionerBean implements Writable {
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
	
	public void set(int upFlow, int downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upFlow);
		out.writeInt(downFlow);
		out.writeInt(sumFlow);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readInt();
		this.downFlow = in.readInt();
		this.sumFlow = in.readInt();
		
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow ;
	}
	
}
