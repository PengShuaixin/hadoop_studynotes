package com.niclas.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class TypeStatisticsBean implements WritableComparable<TypeStatisticsBean>{
	/*
	 * 封装品牌和月份作为key,实现WritableComparable接口
	 */
	private String brand;//品牌
	private String mouth;//月份
	
	public void set(String brand, String mouth) {
		this.brand = brand;
		this.mouth = mouth;
	}
	
	public TypeStatisticsBean() {
		super();
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getMouth() {
		return mouth;
	}

	public void setMouth(String mouth) {
		this.mouth = mouth;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		this.brand = input.readUTF();
		this.mouth = input.readUTF();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeUTF(brand);
		output.writeUTF(mouth);
	}
	@Override
	public int compareTo(TypeStatisticsBean o) {
		// TODO Auto-generated method stub
		if (this.brand.equals(o.getBrand())) {
			return this.mouth.compareTo(o.getMouth());
		}
		return -this.brand.compareTo(o.getBrand());
	}

	@Override
	public String toString() {
		return  brand + "\t" + mouth ;
	}
	
}
