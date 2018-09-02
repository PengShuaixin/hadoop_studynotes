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
 * 3.1��ͳ�Ʋ�ͬƷ�Ƶĳ���ÿ���µ��������ֲ�
 */
public class TypeStatistics {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//����Mapper��Reducer
		job.setMapperClass(TypeMapper.class);
		job.setReducerClass(TypeReducer.class);
		//����Map���������
		job.setMapOutputKeyClass(TypeStatisticsBean.class);
		job.setMapOutputValueClass(IntWritable.class);
		//����Reduce�������
		job.setOutputKeyClass(TypeStatisticsBean.class);
		job.setOutputValueClass(IntWritable.class);
		//����job�ļ�������
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//����job�ļ������
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"ִ�гɹ�":"ִ��ʧ��");	
	}
	public static class TypeMapper extends Mapper<LongWritable, Text, TypeStatisticsBean, IntWritable> {
		IntWritable intWritable = new IntWritable(1);
		TypeStatisticsBean bean = new TypeStatisticsBean();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TypeStatisticsBean, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			if (values.length >= 20) {
				String brand = values[7];
				String mouth = values[1];
				if (!brand.isEmpty()) {
					bean.set(brand, mouth);
				}else {
					brand = "δע��";
					bean.set(brand, mouth);
				}				
				context.write(bean, intWritable);
			}
		}
		
	}
	public static class TypeReducer extends Reducer<TypeStatisticsBean, IntWritable, TypeStatisticsBean, IntWritable>{
		
		@Override
		protected void reduce(TypeStatisticsBean key, Iterable<IntWritable> value,
				Reducer<TypeStatisticsBean, IntWritable, TypeStatisticsBean, IntWritable>.Context context ) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int num = 0;
			for (IntWritable intWritable : value) {
				num += intWritable.get();
			}
			context.write(key, new IntWritable(num));
		}
		
	}
}
