package com.niclas.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1����ȡJob����
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2����������job��jar��
		job.setJarByClass(WordCount.class);
		//3������Mapper��Reducer
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		//4����ʽ������Map���������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//5������Reduce�������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6������job���ļ�����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//7������job���ļ����
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//8���ύjobִ��
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag?"ִ�гɹ�":"ִ��ʧ��");
		
	}

}
