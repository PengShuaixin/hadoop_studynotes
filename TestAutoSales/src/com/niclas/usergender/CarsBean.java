package com.niclas.usergender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/*
 * 2.2����Ȩ���������͡�Ʒ�Ʒ�װ�ɶ���ʵ��WritableComparable�ӿ���Ϊkey
 */
public class CarsBean implements WritableComparable<CarsBean>{
	private String ownership;//����Ȩ9
	private String type;//��������8
	private String brand;//Ʒ��7
	
	public void set(String ownership, String type, String brand) {
		this.ownership = ownership;
		this.type = type;
		this.brand = brand;
	}
	
	public CarsBean() {
		super();
	}

	public String getOwnership() {
		return ownership;
	}
	public void setOwnership(String ownership) {
		this.ownership = ownership;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(ownership);
		out.writeUTF(type);
		out.writeUTF(brand);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.ownership = in.readUTF();
		this.type = in.readUTF();
		this.brand = in.readUTF();
	}
	@Override
	public int compareTo(CarsBean o) {
		// TODO Auto-generated method stub
		if(this.ownership.equals(o.getOwnership())) {
			if (this.type.equals(o.getType())) {
				return this.brand.compareTo(o.getBrand());
			}else {
				return this.type.compareTo(o.getType());
			}
			
		}else {
			return this.ownership.compareTo(o.getOwnership());
		}
				
	}
	@Override
	public String toString() {
		return ownership + "\t" + type + "\t" + brand;
	}
	
}
