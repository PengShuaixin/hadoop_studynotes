package com.niclas.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//KEYIN, Text��map������������
//VALUEIN, IntWritable��map�������ݹ�����
//KEYOUT, Text
//VALUEOUT,IntWritable����
/*
a)����map�׶�����ĵ��ʼ�ֵ��
b)����ͬ���ʵļ�ֵ�Ի�۳�һ��
c)��ÿһ�飬�������е����С�ֵ�����ۼ���ͣ����õ�ÿһ�����ʵ��ܴ���
d)��(���ʣ��ܴ���)�����HDFS���ļ���
 */
//�������ڣ����ÿ���ݽ���һ��kv �飬reduce����������һ��
//conf.set("dfs.replication", "3");


public class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
		//�ۼ�value�е�����
		//1���ۼ���
		int sum = 0;
		Iterator<IntWritable> iterator = value.iterator();
		while (iterator.hasNext()) {
			IntWritable intWritable = (IntWritable) iterator.next();
			sum += intWritable.get();			
		}		
		context.write(key, new IntWritable(sum));
		
	}
	

}
