package com.niclas.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ClassPartitioner extends Partitioner<ClassInfoBean, NullWritable>{

	@Override
	public int getPartition(ClassInfoBean key, NullWritable value, int numReduceTasks) {
		//��ͬid�Ķ���bean���ᷢ����ͬ��partition
		//���ң������ķ��������ǻ���û����õ�reduce task������һ��
		return (key.getClassName().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

}
