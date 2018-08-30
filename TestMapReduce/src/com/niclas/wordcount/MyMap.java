package com.niclas.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//int-->IntWritable
//String-->TEXT
//long-->LongWritable
//null-->NullWritable
//KEYIN:Text
//VALUEIN:IntWritable
/*
 *
a)从HDFS的源数据文件中逐行读取数据
b)将每一行数据切分出单词
c)为每一个单词构造一个键值对(单词，1)
d)将键值对发送给reduce
 */
	//map方法的生命周期：  框架每传一行数据就被调用一次
	//key :  这一行的起始点在文件中的偏移量
	//value: 这一行的内容

public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//按照空格进行分割
		String string = value.toString();
		String[] split = string.split(" ");
		for (String string2 : split) {
			context.write(new Text(string2), new IntWritable(1));
		}
	}
	

}
