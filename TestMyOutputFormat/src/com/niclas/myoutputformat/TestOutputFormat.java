package com.niclas.myoutputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TestOutputFormat {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//map,job,reduce
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"My");
		job.setJarByClass(TestOutputFormat.class);
		
		job.setMapperClass(OutputMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(MyOutPutFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)?"执行成功":"执行失败");
	}
	static class OutputMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		IntWritable id = new IntWritable();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			id.set(Integer.parseInt(values[0]));
			String str = "";
			for (int i = 0; i < values.length; i++) {
				str += values[i];
			}
			text.set(str);
			context.write(id, text);
		}
		
	}
}
