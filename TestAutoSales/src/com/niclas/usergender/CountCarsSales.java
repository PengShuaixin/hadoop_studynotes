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
 * 2.2统计的车的所有权、车辆类型和品牌的分布
 */
public class CountCarsSales {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//设置Mapper和Reducer
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		//设置Map的输出类型
		job.setMapOutputKeyClass(CarsBean.class);
		job.setMapOutputValueClass(IntWritable.class);
		//设置Reduce输出类型
		job.setOutputKeyClass(CarsBean.class);
		job.setOutputValueClass(IntWritable.class);
		//设置job文件的输入
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//设置job文件的输出
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//设置自定义的comparator类 
		//job.setGroupingComparatorClass();
		//指定自定义数据分区器
		//job.setPartitionerClass();
		//指定相应“分区”数量的reducetask
		//job.setNumReduceTasks(5);
		//提交job执行
		//job.submit();
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"执行成功":"执行失败");		
		
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
	
	public static class CountReducer extends Reducer<CarsBean, IntWritable, CarsBean, IntWritable> {

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
