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
a)��HDFS��Դ�����ļ������ж�ȡ����
b)��ÿһ�������зֳ�����
c)Ϊÿһ�����ʹ���һ����ֵ��(���ʣ�1)
d)����ֵ�Է��͸�reduce
 */
	//map�������������ڣ�  ���ÿ��һ�����ݾͱ�����һ��
	//key :  ��һ�е���ʼ�����ļ��е�ƫ����
	//value: ��һ�е�����

public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//���տո���зָ�
		String string = value.toString();
		String[] split = string.split(" ");
		for (String string2 : split) {
			context.write(new Text(string2), new IntWritable(1));
		}
	}
	

}
