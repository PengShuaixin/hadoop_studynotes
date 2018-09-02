package com.niclas.practice;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Product {
	static class idMapper extends Mapper<LongWritable, Text, Text, ProductBean>{
		Text text = new Text();
		public static Text transformTextToUTF8(Text text, String encoding) {
			String value = null;
			try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
			} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			}
			return new Text(value);
			}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split(" ");
			String puId = values[0];
			String name = values[1];
			String category = values[2];
			String price = values[3];
			ProductBean idBean = new ProductBean(name, category, price);
			transformTextToUTF8(text, "GBK");
			text.set(puId);
			context.write(text, idBean);		
		}
	}
	//输出全部为字符串的时候为什么要把reduce去掉才会有结果？
//	static class idReducer extends Reducer<Text, IdBean, Text, IdBean>{
//		@Override
//		protected void reduce(Text text, Iterable<IdBean> values, Context context)
//				throws IOException, InterruptedException {
//		}
//	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(Product.class);
		job.setMapperClass(idMapper.class);
		//job.setReducerClass(idReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ProductBean.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean fc = job.waitForCompletion(true);
		System.out.println(fc?"执行成功":"执行失败");
		
				
		

	}

}
