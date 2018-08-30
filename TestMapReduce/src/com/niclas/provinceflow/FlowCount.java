package com.niclas.provinceflow;

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


public class FlowCount {
	
	//Mapper
	
	//KEYIN:LongWritable:偏移量
	//VALUEIN:
	//KEYOUT:
	//VALUEOUT:
	static class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
		FlowBean flowBean = new FlowBean();
		Text text = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split(" ");
			String phoneNumber = values[0];
			String upFlow = values[1];
			String downFlow = values[2];
			flowBean.set(Integer.parseInt(upFlow),Integer.parseInt(downFlow));
			text.set(phoneNumber);
			context.write(flowBean,text);
		}

		
	}
	//Reducer
	static class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{

		@Override
		protected void reduce(FlowBean key, Iterable<Text> value, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iterator = value.iterator();
			context.write(iterator.next(), key);
		}
		
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		//1.获取job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//2.设置运行job的jar包
		job.setJarByClass(FlowCount.class);
		//3.设置Mapper和Reducer
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		//指定自定义数据分区器
		job.setPartitionerClass(ProvincePartitioner.class);
		//指定相应“分区”数量的reducetask
		job.setNumReduceTasks(5);
		//4.设置Map的输出类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		//5.设置Reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		//6.设置job文件的输入
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//7.设置job文件的输出
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//8.提交job执行
//		job.submit();
		boolean falag = job.waitForCompletion(true);
		System.out.println(falag?"执行成功":"执行失败");
	
	}
	
}
