package com.niclas.usergender;
/*
 * 2.1ͳ����Ů����
 */

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountUserGender {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//����Mapper��Reducer
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReduce.class);
		//����Map���������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//����Reduce�������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//����job�ļ�������
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//����job�ļ������
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//�����Զ����comparator�� 
		//job.setGroupingComparatorClass();
		//ָ���Զ������ݷ�����
		//job.setPartitionerClass();
		//ָ����Ӧ��������������reducetask
		//job.setNumReduceTasks(5);
		//�ύjobִ��
		//job.submit();
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"ִ�гɹ�":"ִ��ʧ��");		
		
	}
	
	static int num = 0;
	public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable intWritable = new IntWritable(1);
		Text text = new Text();		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			
			if (values.length >= 20) {
				if (values[values.length - 1].equals("����") || values[values.length - 1].equals("Ů��")) {
					text.set(values[values.length - 1]);
				} else {
					text.set("δע���Ա�");
				} 
				context.write(text, intWritable);
				num++;
			}
			
		}		
	}
	public static class CountReduce extends Reducer<Text, IntWritable, Text, Text> {
		Text text = new Text();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			double count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			double avg = count/num;
			DecimalFormat decimalFormat = new DecimalFormat("0.00%");
			text.set("ռ�ȣ�" + decimalFormat.format(avg));
			context.write(key, text);
		}		
	}
}
