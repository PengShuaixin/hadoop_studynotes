package com.niclas.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// ����reduce�˵�GroupingComparator��ʵ�ֽ�һ��bean������ͬ��key

public class Comparator extends WritableComparator {

	//������Ϊkey��bean��class���ͣ��Լ��ƶ���Ҫ�ÿ���������ȡʵ������
	protected Comparator() {
		super(ClassBean.class, true);
	}
	

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ClassBean abean = (ClassBean) a;
		ClassBean bbean = (ClassBean) b;
		
		//�Ƚ�����beanʱ��ָ��ֻ�Ƚ�bean�е�
		return abean.getClass_id().compareTo(bbean.getClass_id());		
	}

}
