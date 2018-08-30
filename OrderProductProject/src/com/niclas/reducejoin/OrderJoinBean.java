package com.niclas.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OrderJoinBean implements Writable{
	private String order_id;//订单id
	private String date;//订单日期
	private String name;//商品名称
	private String category_id;//种类id
	private String price;//商品价格
	public String getOrder_id() {
		return order_id;
	}
	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCategory_id() {
		return category_id;
	}
	public void setCategory_id(String category_id) {
		this.category_id = category_id;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	
	public OrderJoinBean() {
		super();
	}
	public void set(String order_id, String date, String name, String category_id, String price) {
		this.order_id = order_id;
		this.date = date;
		this.name = name;
		this.category_id = category_id;
		this.price = price;
	}
	@Override
	public String toString() {
		return order_id + "\t"  + date + "\t" + name + "\t" + category_id + "\t" + price + "\t" ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(date);
		out.writeUTF(name);
		out.writeUTF(category_id);
		out.writeUTF(price);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.order_id=in.readUTF();
		this.date=in.readUTF();
		this.name=in.readUTF();
		this.category_id=in.readUTF();
		this.price=in.readUTF();
		
	}
	
}
