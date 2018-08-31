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
/*
 * 1.重写map方法，将班级和成绩封装成一个bean，作为map的key输出
 * 2.map作为key时，需要实现WritableComparable接口
 * 3.重写WritableComparable，将班级id相同的合并
 * 4.输出到reduce，重写Partitioner方法，分类输出到文件
 */

public class TestClass {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		//1.获取job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.设置运行job的jar包
		job.setJarByClass(TestClass.class);
		//3.设置Mapper和Reducer
		job.setMapperClass(ClassMapper.class);
		job.setReducerClass(ClassReducer.class);


		//4.设置Map的输出类型
		job.setMapOutputKeyClass(ClassBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		//5.设置Reduce输出类型
		job.setOutputKeyClass(ClassBean.class);
		job.setOutputValueClass(NullWritable.class);
		//6.设置job文件的输入
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//7.设置job文件的输出
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//在此设置自定义的Groupingcomparator类 
		job.setGroupingComparatorClass(Comparator.class);
		//指定自定义数据分区器
//		job.setPartitionerClass(ClassPartitioner.class);
//		//指定相应“分区”数量的reducetask
//		job.setNumReduceTasks(5);
		//8.提交job执行
//		job.submit();
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"执行成功":"执行失败");			
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
		//到达reduce时，相同id的所有bean已经被看成一组，且金额最大的那个一排在第一位
		@Override
		protected void reduce(ClassBean key, Iterable<NullWritable> values,
				Reducer<ClassBean, NullWritable, ClassBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
		
	}
	
}
