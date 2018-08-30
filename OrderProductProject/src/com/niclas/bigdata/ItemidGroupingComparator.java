package com.niclas.bigdata;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// ����reduce�˵�GroupingComparator��ʵ�ֽ�һ��bean������ͬ��key

public class ItemidGroupingComparator extends WritableComparator {

	//������Ϊkey��bean��class���ͣ��Լ��ƶ���Ҫ�ÿ���������ȡʵ������
	protected ItemidGroupingComparator() {
		super(OrderBean.class, true);
	}
	

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean abean = (OrderBean) a;
		OrderBean bbean = (OrderBean) b;
		
		//�Ƚ�����beanʱ��ָ��ֻ�Ƚ�bean�е�orderid
		return abean.getItemid().compareTo(bbean.getItemid());
		
	}

}
