package com.niclas.auto;

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
 * 1.3�С������������
 */
public class CountAreaSales {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//����Mapper��Reducer
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReduce.class);
		//����Map���������
		job.setMapOutputKeyClass(AreaBean.class);
		job.setMapOutputValueClass(IntWritable.class);
		//����Reduce�������
		job.setOutputKeyClass(AreaBean.class);
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

	public static class CountMapper extends Mapper<LongWritable, Text, AreaBean, IntWritable> {
		AreaBean areaBean = new AreaBean();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			if (values.length >= 10) {
				if (!values[2].isEmpty()&&!values[3].isEmpty()) {
					String city = values[2];
					String area = values[3];
					areaBean.set(city, area);
					context.write(areaBean, new IntWritable(1));
				}
			}
			
		}
		
	}
	
	public static class CountReduce extends Reducer<AreaBean, IntWritable, AreaBean, IntWritable> {

		@Override
		protected void reduce(AreaBean key, Iterable<IntWritable> value,
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
