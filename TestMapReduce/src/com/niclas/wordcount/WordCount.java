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
		//1、获取Job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2、设置运行job的jar包
		job.setJarByClass(WordCount.class);
		//3、设置Mapper和Reducer
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		//4、显式的设置Map的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//5、设置Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//6、设置job的文件输入
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//7、设置job的文件输出
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//8、提交job执行
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag?"执行成功":"执行失败");
		
	}

}
