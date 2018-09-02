package com.niclas.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TypeEngineBean implements WritableComparable<TypeEngineBean>{

	private String brand;//品牌
	private String engine;//发动机
	private String fuel;//燃料
	public void set(String brand, String engine, String fuel) {
		this.brand = brand;
		this.engine = engine;
		this.fuel = fuel;
	}
	public TypeEngineBean() {
		super();
	}
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getEngine() {
		return engine;
	}
	public void setEngine(String engine) {
		this.engine = engine;
	}
	public String getFuel() {
		return fuel;
	}
	public void setFuel(String fuel) {
		this.fuel = fuel;
	}
	@Override
	public String toString() {
		return  brand + "\t" + engine + "\t" + fuel;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.brand = in.readUTF();
		this.engine = in.readUTF();
		this.fuel = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(brand);
		out.writeUTF(engine);
		out.writeUTF(fuel);
	}
	@Override
	public int compareTo(TypeEngineBean o) {
		// TODO Auto-generated method stub
		if(this.brand.equals(o.getBrand())) {
			if (this.engine.equals(o.getEngine())) {
				return this.fuel.compareTo(o.getFuel());
			}else {
				return this.engine.compareTo(o.getEngine());
			}
			
		}else {
			return this.brand.compareTo(o.getBrand());
		}
	}
	
	
}
