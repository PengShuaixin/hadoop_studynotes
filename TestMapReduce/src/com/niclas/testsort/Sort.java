package com.niclas.testsort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {
	
	//Mapper
	/*KEYIN:LongWritable:偏移量
	 *VALUEIN：Text：一行文本的内容
	 *KEYOUT:FlowBean
	 *VALUEOUT:Text手机号
	 * <0,"155666 180 180 56">
	 */
	static class SortMapper extends Mapper<LongWritable, Text, SortBean, Text>{
		SortBean flowBean = new SortBean();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\\s+");
			String phoneNumber = values[0];
			String  upFlow = values[1];
			String downFlow = values[2];	
			flowBean.set(Integer.parseInt(upFlow),Integer.parseInt(downFlow));
			text.set(phoneNumber);
			context.write(flowBean,text);
						
		}
	}
	//Reducer
	static class SortReducer extends Reducer<SortBean, Text, Text, SortBean>{
		@Override
		protected void reduce(SortBean key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
				Iterator<Text> iterator = value.iterator();
				context.write(iterator.next(), key);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//设置输出文件中Key和Value的分割符：configuration.set("mapred.textoutputformat.separator", ";");
		Configuration configuration = new Configuration();	
		Job job = Job.getInstance(configuration);
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeyClass(SortBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SortBean.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean fc = job.waitForCompletion(true);
		System.out.println(fc?"执行成功":"执行失败");
		

	}

}
