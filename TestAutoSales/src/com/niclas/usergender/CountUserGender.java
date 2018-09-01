package com.niclas.usergender;
/*
 * 2.1统计男女比例
 */

import java.io.IOException;
import java.text.DecimalFormat;

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

public class CountUserGender {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//设置Mapper和Reducer
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReduce.class);
		//设置Map的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//设置Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
	
	static int num = 0;
	public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable intWritable = new IntWritable(1);
		Text text = new Text();		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] values = value.toString().split("\t");
			
			if (values.length >= 20) {
				if (values[values.length - 1].equals("男性") || values[values.length - 1].equals("女性")) {
					text.set(values[values.length - 1]);
				} else {
					text.set("未注明性别");
				} 
				context.write(text, intWritable);
				num++;
			}
			
		}		
	}
	public static class CountReduce extends Reducer<Text, IntWritable, Text, Text> {
		Text text = new Text();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			double count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			double avg = count/num;
			DecimalFormat decimalFormat = new DecimalFormat("0.00%");
			text.set("占比：" + decimalFormat.format(avg));
			context.write(key, text);
		}		
	}
}
