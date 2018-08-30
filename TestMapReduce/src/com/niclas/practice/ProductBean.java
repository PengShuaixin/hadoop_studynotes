package com.niclas.practice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ProductBean implements Writable {
	private String name;
	private String category;
	private String price;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	public ProductBean() {
		
	}
	public ProductBean(String name, String category, String price) {
		super();
		this.name = name;
		this.category = category;
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(category);
		out.writeUTF(price);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.category = in.readUTF();
		this.price = in.readUTF();
		
	}
	@Override
	public String toString() {
		return "IdBean [name=" + name + ", category=" + category + ", price=" + price + "]";
	}
	
	

}
