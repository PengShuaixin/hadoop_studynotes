package com.niclas.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyWritableComparator extends WritableComparator{
	//����������������У��ڲ���Ҫʹ�÷�����Ʒ������Ӧ���͵Ķ��������ʵ�֣���ô����ֻ����Ϊ��WritableComparator����
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
