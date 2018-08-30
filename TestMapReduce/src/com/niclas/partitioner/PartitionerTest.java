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
	//1��Mapper��-->Map����
	//Mapper���ҵ���߼�
	static class PartitionerMapper extends Mapper<LongWritable,Text, Text,PartitionerBean>{
		PartitionerBean partitionerBean = new PartitionerBean();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//����PartitionerBean����д�뵽context��
				String[] values = value.toString().split("\t");
				//����õ��ļ�
//				String phoneNumber = values[0];
//				int upFlow = Integer.parseInt(values[1]);
//				int downFlow = Integer.parseInt(values[2]);
//				partitionerBean.set(upFlow,downFlow);
				//***.logԴ�ļ�
				String phoneNumber = values[1];
				String upFlow = values[values.length - 3];
				String downFlow = values[values.length - 2];
				partitionerBean.set(Integer.parseInt(upFlow), Integer.parseInt(downFlow));
				text.set(phoneNumber);
				context.write(text,partitionerBean);
		}
		//map����֮����
		//<1345799,<partitionerBean1,partitionerBean2,partitionerBean13>>---����ʽ�������������û�з��鵽�����ļ�
		//<1345799,<partitionerBean1,partitionerBean2,partitionerBean13>>
		/*
		 * <1345799,<partitionerBean1>>							a.txt
		 * <14566,<partitionerBean1>>				--->		....
		 * <13458954,<partitionerBean1>>						b.txt
		 * 
		 */
		//HashPartitioner<K, V>���Զ�map�����Ľ���з�����Ĭ�Ϸ���Ϊ1����������д�����getPartition�����������·���
	}

	//Reducer��--��û����reduce������в���������ʡ�Բ�д
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
		//����jar
		job.setJarByClass(PartitionerTest.class);
		//����Mapper��Reducer
		job.setMapperClass(PartitionerMapper.class);
		//���÷�������ʵ����
		job.setPartitionerClass(MyPartitioner.class);
		//����reducetask�ĸ���
		job.setNumReduceTasks(5);
		//job.setReducerClass(PartitionerReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PartitionerBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PartitionerBean.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean waitForCompletion = job.waitForCompletion(true);
		System.out.println(waitForCompletion?"ִ�гɹ�":"ִ��ʧ��");
	}

}
