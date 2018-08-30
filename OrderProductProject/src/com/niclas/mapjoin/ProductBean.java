package com.niclas.mapjoin;
//用来保存在文件中提取的一行信息
public class ProductBean {
	private String name;
	private String category_id;
	private String price;
	public ProductBean(String[] values) {
		this(values[1], values[2], values[3]);
		
				
	}
	private ProductBean(String name, String category_id, String price) {
		super();
		this.name = name;
		this.category_id = category_id;
		this.price = price;
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
	
}
