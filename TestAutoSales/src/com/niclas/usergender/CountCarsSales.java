package com.niclas.usergender;

import java.io.IOException;

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
/*
 * 2.2ͳ�Ƶĳ�������Ȩ���������ͺ�Ʒ�Ƶķֲ�
 */
public class CountCarsSales {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//����Mapper��Reducer
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReduce.class);
		//����Map���������
		job.setMapOutputKeyClass(CarsBean.class);
		job.setMapOutputValueClass(IntWritable.class);
		//����Reduce�������
		job.setOutputKeyClass(CarsBean.class);
		job.setOutputValueClass(IntWritable.class);
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

	public static class CountMapper extends Mapper<LongWritable, Text, CarsBean, IntWritable> {
		CarsBean carsBean = new CarsBean();
		IntWritable intWritable = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			if (values.length >= 20) {
				String ownership = values[9];
				String type = values[8];
				String brand = values[7];
				carsBean.set(ownership, type, brand);
				context.write(carsBean, intWritable);

			}
			
		}
		
	}
	
	public static class CountReduce extends Reducer<CarsBean, IntWritable, CarsBean, IntWritable> {

		@Override
		protected void reduce(CarsBean key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count = 0;
			for (IntWritable intWritable : value) {
				count += intWritable.get();
			}
			context.write(key, new IntWritable(count));
		}
		
	}
}