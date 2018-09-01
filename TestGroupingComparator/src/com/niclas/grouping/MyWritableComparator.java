package com.niclas.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyWritableComparator extends WritableComparator{
	//下面这个方法必须有，内部需要使用反射机制反射出对应类型的对象，如果不实现，那么类型只能认为是WritableComparator类型
	protected MyWritableComparator() {
		// TODO Auto-generated constructor stub
		super(ClassInfoBean.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		ClassInfoBean classInfoBean1 = (ClassInfoBean) a;
		ClassInfoBean classInfoBean2 = (ClassInfoBean) b;
//		if (classInfoBean1.getClassName().equals(classInfoBean2.getClassName())) {
//			return 0;
//		}
//		return 1;
		return classInfoBean1.getClassName().compareTo(classInfoBean2.getClassName());
	}
	
	
}
