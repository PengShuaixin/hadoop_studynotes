package com.niclas.bigdata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean bean, NullWritable value, int numReduceTasks) {
		//��ͬid�Ķ���bean���ᷢ����ͬ��partition
		//���ң������ķ��������ǻ���û����õ�reduce task������һ��
		return (bean.getItemid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		
	}

}
