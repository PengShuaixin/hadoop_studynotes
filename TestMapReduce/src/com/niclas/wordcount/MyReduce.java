package com.niclas.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//KEYIN, Text是map方法传过来的
//VALUEIN, IntWritable是map方法传递过来的
//KEYOUT, Text
//VALUEOUT,IntWritable类型
/*
a)接收map阶段输出的单词键值对
b)将相同单词的键值对汇聚成一组
c)对每一组，遍历组中的所有“值”，累加求和，即得到每一个单词的总次数
d)将(单词，总次数)输出到HDFS的文件中
 */
//生命周期：框架每传递进来一个kv 组，reduce方法被调用一次
//conf.set("dfs.replication", "3");


public class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
		//累加value中的内容
		//1、累加器
		int sum = 0;
		Iterator<IntWritable> iterator = value.iterator();
		while (iterator.hasNext()) {
			IntWritable intWritable = (IntWritable) iterator.next();
			sum += intWritable.get();			
		}		
		context.write(key, new IntWritable(sum));
		
	}
	

}
