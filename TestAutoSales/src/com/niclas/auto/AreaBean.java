package com.niclas.auto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AreaBean implements WritableComparable<AreaBean>{
	private String city;
	private String area;
	public void set(String city, String area) {
		this.city = city;
		this.area = area;
	}
	public AreaBean() {
		super();
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(city);
		out.writeUTF(area);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.city = in.readUTF();
		this.area = in.readUTF();
	}
	@Override
	public int compareTo(AreaBean o) {
		// TODO Auto-generated method stub
		if (this.city.equals(o.getCity())) {
			return this.area.compareTo(o.getArea());
		}
		return this.city.compareTo(o.getCity());
	}
	@Override
	public String toString() {
		return city + area ;
	}
	
}
