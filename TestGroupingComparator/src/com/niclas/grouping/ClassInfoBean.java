package com.niclas.grouping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class ClassInfoBean implements WritableComparable<ClassInfoBean> {
	private String className;//班级名
	private float score;//分数

	public void set(String className, float score) {
		
		this.className = className;
		this.score = score;
	}
	public ClassInfoBean() {
		super();
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	@Override
	public String toString() {
		return className + "\t" + score;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.className = in.readUTF();
		this.score = in.readFloat();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(className);
		out.writeFloat(score);
	}
	@Override
	public int compareTo(ClassInfoBean o) {
		// TODO Auto-generated method stub
		if (this.className.equals(o.getClassName())) {
			return this.score < o.getScore() ?1 :-1;
		}
		return this.className.hashCode() - o.getClassName().hashCode();

	}


}