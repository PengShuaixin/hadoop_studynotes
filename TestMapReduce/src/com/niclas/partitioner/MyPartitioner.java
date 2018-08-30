package com.niclas.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
//定义类继承自HashPartitioner，修改返回的分区。
//K,V 来自MapTask的输出类型
public class MyPartitioner extends HashPartitioner<Text,PartitionerBean>{
	@Override
	public int getPartition(Text key, PartitionerBean value, int numReduceTasks) {
		int number = 0;
		//根据不同的手机号返回不同的分区
		String subString = key.toString().substring(0, 3);
		switch (subString) {
		case "134":
				number = 0;
			break;
		case "135":
				number = 1;
		break;
		case "136":
				number = 2;
		break;
		case "137":
			number = 3;
		break;
		default:
			number = 4;
			break;
		}
		return number;
	}

}
