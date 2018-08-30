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



/*
 *执行多个job
 */
public class FlowCount2 {
	static FlowBean flowBean = new FlowBean();
	static Text text = new Text();
	//第一次处理文件
	static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			//逻辑处理
			String[] values = value.toString().split("\t");
			String phoneNumber = values[1];
			String upFlowStr = values[values.length - 3];
			String downFlowStr = values[values.length - 2];
			flowBean.set(Integer.parseInt(upFlowStr), Integer.parseInt(downFlowStr));
			//写到文件中
			context.write(new Text(phoneNumber), flowBean);	
		}
		
	}
	//<135180123345,<flowbean1,flowbean2,flowbean3>>
	//<135180123345,<flowbean1,flowbean2,flowbean3>>
	//<135180123345,<总Flowbean>>
	static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

		@Override
		protected void reduce(Text text, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			int sumUPFlow = 0;
			int sumDownFlow = 0;
			for (FlowBean flowBean : values) {
				sumUPFlow += flowBean.getUpflow();
				sumDownFlow += flowBean.getDownFlow();
			}
		flowBean.set(sumUPFlow,sumDownFlow);
		context.write(text, flowBean);
		}
		
	}
	
	//第二次处理
	static class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			String phoneNumber = values[0];
			String upFlow = values[1];
			String downFlow = values[2];
			flowBean.set(Integer.parseInt(upFlow),Integer.parseInt(downFlow));
			text.set(phoneNumber);
			context.write(flowBean,text);
		}

		
	}
	static class SortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{

		@Override
		protected void reduce(FlowBean key, Iterable<Text> value, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iterator = value.iterator();
			context.write(iterator.next(), key);
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path path = new Path("D:/tmp");
		Job job1 = Job.getInstance(conf, "flow");
		//设置运行job的jar包
		job1.setJarByClass(FlowCount2.class);
		//设置Mapper和Reducer
		job1.setMapperClass(FlowMapper.class);
		job1.setReducerClass(FlowReducer.class);
		//显式的设置Map的输出类型
		job1.setMapOutputKeyClass(Text.class);
		job1.setOutputValueClass(FlowBean.class);
		//设置Reduce输出类型
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FlowBean.class);
		//设置job文件的输入
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		//设置job文件的输出
		FileOutputFormat.setOutputPath(job1, path);
	
		
		
		Job job2 = Job.getInstance(conf);
		//设置运行job的jar包
		job2.setJarByClass(FlowCount2.class);
		//设置Mapper和Reducer
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);
		//指定自定义数据分区器
		job2.setPartitionerClass(ProvincePartitioner.class);
		//指定相应“分区”数量的reducetask
		job2.setNumReduceTasks(5);
		//设置Map的输出类型
		job2.setMapOutputKeyClass(FlowBean.class);
		job2.setMapOutputValueClass(Text.class);
		//设置Reduce输出类型
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FlowBean.class);
		//设置job文件的输入
		FileInputFormat.addInputPath(job2, path);
		//设置job文件的输出
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
						
		//提交job执行
		//job.submit();
		if (job1.waitForCompletion(true)) {
			boolean falag = job2.waitForCompletion(true);
			System.out.println(falag?"执行成功":"执行失败");
		}else {
			System.out.println("执行失败-1");
		}
		

		
	}
}
