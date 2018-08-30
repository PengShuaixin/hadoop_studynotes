package com.niclas.partitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionerTest {
	//1、Mapper类-->Map方法
	//Mapper类的业务逻辑
	static class PartitionerMapper extends Mapper<LongWritable,Text, Text,PartitionerBean>{
		PartitionerBean partitionerBean = new PartitionerBean();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//构造PartitionerBean对象，写入到context中
				String[] values = value.toString().split("\t");
				//排序好的文件
//				String phoneNumber = values[0];
//				int upFlow = Integer.parseInt(values[1]);
//				int downFlow = Integer.parseInt(values[2]);
//				partitionerBean.set(upFlow,downFlow);
				//***.log源文件
				String phoneNumber = values[1];
				String upFlow = values[values.length - 3];
				String downFlow = values[values.length - 2];
				partitionerBean.set(Integer.parseInt(upFlow), Integer.parseInt(downFlow));
				text.set(phoneNumber);
				context.write(text,partitionerBean);
		}
		//map处理之后结果
		//<1345799,<partitionerBean1,partitionerBean2,partitionerBean13>>---》形式满足输出需求但是没有分组到各个文件
		//<1345799,<partitionerBean1,partitionerBean2,partitionerBean13>>
		/*
		 * <1345799,<partitionerBean1>>							a.txt
		 * <14566,<partitionerBean1>>				--->		....
		 * <13458954,<partitionerBean1>>						b.txt
		 * 
		 */
		//HashPartitioner<K, V>可以对map处理后的结进行分区，默认分区为1个，所以重写里面的getPartition方法进行重新分区
	}

	//Reducer类--》没有在reduce里面进行操作，可以省略不写
//	static class PartitionerReducer extends Reducer<PartitionerBean,Text, Text, PartitionerBean>{
//		@Override
//		protected void reduce(PartitionerBean values, Iterable<Text> text,Context context) throws IOException, InterruptedException {
//			for (Text phone : text) {
//				String str = text.toString();
//				if (str.substring(0, 2) == "135") {			
//				}		
//			}
//		}
	
//	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		//设置jar
		job.setJarByClass(PartitionerTest.class);
		//设置Mapper、Reducer
		job.setMapperClass(PartitionerMapper.class);
		//设置分区具体实现类
		job.setPartitionerClass(MyPartitioner.class);
		//设置reducetask的个数
		job.setNumReduceTasks(5);
		//job.setReducerClass(PartitionerReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PartitionerBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PartitionerBean.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean waitForCompletion = job.waitForCompletion(true);
		System.out.println(waitForCompletion?"执行成功":"执行失败");
	}

}
