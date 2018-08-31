package com.niclas.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 利用reduce端的GroupingComparator来实现将一组bean看成相同的key

public class Comparator extends WritableComparator {

	//传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
	protected Comparator() {
		super(ClassBean.class, true);
	}
	

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ClassBean abean = (ClassBean) a;
		ClassBean bbean = (ClassBean) b;
		
		//比较两个bean时，指定只比较bean中的
		return abean.getClass_id().compareTo(bbean.getClass_id());		
	}

}
