package com.niclas.type;

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
 * 3.2通过不同类型（品牌）车销售情况，来统计发动机型号和燃料种类
 */
public class TypeEngine {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//设置Mapper和Reducer
		job.setMapperClass(TypeEngineMapper.class);
		job.setReducerClass(TypeEngineReducer.class);
		//设置Map的输出类型
		job.setMapOutputKeyClass(TypeEngineBean.class);
		job.setMapOutputValueClass(IntWritable.class);
		//设置Reduce输出类型
		job.setOutputKeyClass(TypeEngineBean.class);
		job.setOutputValueClass(IntWritable.class);
		//设置job文件的输入
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//设置job文件的输出
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"执行成功":"执行失败");
	}

	public static class TypeEngineMapper extends Mapper<LongWritable, Text, TypeEngineBean, IntWritable> {
		TypeEngineBean typeEngineBean = new TypeEngineBean();
		IntWritable intWritable = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			if (values.length >= 20) {
				String brand = values[7];
				String engine = values[12];
				String fuel = values[15];
				typeEngineBean.set(brand, engine, fuel);
				context.write(typeEngineBean, intWritable);

			}
			
		}
		
	}
	
	public static class TypeEngineReducer extends Reducer<TypeEngineBean, IntWritable, TypeEngineBean, IntWritable> {

		@Override
		protected void reduce(TypeEngineBean key, Iterable<IntWritable> value,
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
