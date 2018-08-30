package com.niclas.provinceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ProvincePartitioner extends Partitioner<FlowBean, Text>{
	
	public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();
	static{
		proviceDict.put("136", 0);
		proviceDict.put("137", 1);
		proviceDict.put("138", 2);
		proviceDict.put("139", 3);
	}	

	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		String prefix = value.toString().substring(0, 3);
		Integer provinceId = proviceDict.get(prefix);		
		return provinceId==null?4:provinceId;
	}

}
