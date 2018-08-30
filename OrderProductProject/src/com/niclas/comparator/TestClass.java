package com.niclas.comparator;

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


public class TestClass {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		//1.��ȡjob����
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.��������job��jar��
		job.setJarByClass(TestClass.class);
		//3.����Mapper��Reducer
		job.setMapperClass(ClassMapper.class);
		job.setReducerClass(ClassReducer.class);


		//4.����Map���������
		job.setMapOutputKeyClass(ClassBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		//5.����Reduce�������
		job.setOutputKeyClass(ClassBean.class);
		job.setOutputValueClass(NullWritable.class);
		//6.����job�ļ�������
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//7.����job�ļ������
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//�ڴ������Զ����Groupingcomparator�� 
		job.setGroupingComparatorClass(Comparator.class);
		//ָ���Զ������ݷ�����
		job.setPartitionerClass(ClassPartitioner.class);
		//ָ����Ӧ��������������reducetask
		job.setNumReduceTasks(6);
		//8.�ύjobִ��
//		job.submit();
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"ִ�гɹ�":"ִ��ʧ��");			
	}

	static class ClassMapper extends Mapper<LongWritable, Text, ClassBean, NullWritable> {
		ClassBean bean = new ClassBean();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ClassBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			bean.set(new Text(values[0]), new DoubleWritable(Double.parseDouble(values[1])));
			context.write(bean, NullWritable.get());			
		}		
	}
	static class ClassReducer extends Reducer<ClassBean, NullWritable, ClassBean, NullWritable>{
		//����reduceʱ����ͬid������bean�Ѿ�������һ�飬�ҽ�������Ǹ�һ���ڵ�һλ
		@Override
		protected void reduce(ClassBean key, Iterable<NullWritable> values,
				Reducer<ClassBean, NullWritable, ClassBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
		
	}
	
}
