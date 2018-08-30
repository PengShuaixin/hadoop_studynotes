package com.niclas.comparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ClassBean implements WritableComparable<ClassBean>{
	private Text class_id;
	private DoubleWritable grade;
	
	public void set(Text class_id, DoubleWritable grade) {
		this.class_id = class_id;
		this.grade = grade;
	}
	
	public ClassBean() {
		
	}


	
	public Text getClass_id() {
		return class_id;
	}

	public void setClass_id(Text class_id) {
		this.class_id = class_id;
	}

	public DoubleWritable getGrade() {
		return grade;
	}

	public void setGrade(DoubleWritable grade) {
		this.grade = grade;
	}

	@Override
	public int compareTo(ClassBean o) {
		int cmp = this.class_id.compareTo(o.getClass_id());
		if (cmp == 0) {
			cmp = -this.grade.compareTo(o.getGrade());
		}
		return cmp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		String readUTF = in.readUTF();
		double readDouble = in.readDouble();
		
		this.class_id = new Text(readUTF);
		this.grade = new DoubleWritable(readDouble);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(class_id.toString());
		out.writeDouble(grade.get());		
	}

	@Override
	public String toString() {
		return class_id + "\t" + grade;
	}
	
}
