package com.niclas.comparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ClassPartitioner extends Partitioner<ClassBean, NullWritable>{

	@Override
	public int getPartition(ClassBean key, NullWritable value, int numReduceTasks) {
		//��ͬid�Ķ���bean���ᷢ����ͬ��partition
		//���ң������ķ��������ǻ���û����õ�reduce task������һ��
		return (key.getClass_id().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

}
