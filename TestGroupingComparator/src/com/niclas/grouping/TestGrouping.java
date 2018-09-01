package com.niclas.grouping;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TestGrouping {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TestGrouping");
		job.setMapperClass(GroupingMapper.class);
		job.setMapOutputKeyClass(ClassInfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(ClassInfoBean.class);
		job.setOutputValueClass(NullWritable.class);
		job.setGroupingComparatorClass(MyWritableComparator.class);
		job.setReducerClass(GroupingReducer.class);
//		job.setPartitionerClass(ClassPartitioner.class);
//		job.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)?"ִ�гɹ�":"ִ��ʧ��");
	}
	
	static class GroupingMapper extends Mapper<LongWritable, Text, ClassInfoBean, NullWritable> {
		ClassInfoBean classInfobean = new ClassInfoBean();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ClassInfoBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			classInfobean.set(values[0], Float.parseFloat(values[1]));
			context.write(classInfobean, NullWritable.get());
		}
		
	}

	//���뵽Reduce�����е�key����Ψһ�ģ�ÿһ��key����һ��ClassInfoBean����
	//��ClassInfoBean��������ЩClassName��ͬ��ClassInfoBean����һ��key������ͬһ��Reduce����
	static class GroupingReducer extends Reducer<ClassInfoBean, NullWritable, ClassInfoBean, NullWritable> {

		@Override
		protected void reduce(ClassInfoBean key, Iterable<NullWritable> value,
				Reducer<ClassInfoBean, NullWritable, ClassInfoBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
		
	}
}
